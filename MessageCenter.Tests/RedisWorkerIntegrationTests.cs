using MessageCenter.Api.HttpClients.Dtos;
using MessageCenter.Api.Options;
using MessageCenter.Api.Services;
using MessageCenter.Api.Data;
using MessageCenter.Api.Outbox;
using System.Reflection;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using StackExchange.Redis;
using Xunit;

namespace MessageCenter.Tests;

public sealed class RedisWorkerIntegrationTests : IAsyncLifetime
{
    private readonly ConnectionMultiplexer _redis;
    private readonly RedisOptions _options;
    private readonly ServiceProvider _provider;

    public RedisWorkerIntegrationTests()
    {
        var connectionString =
            Environment.GetEnvironmentVariable("MESSAGE_CENTER_TEST_REDIS");
        if (string.IsNullOrWhiteSpace(connectionString))
        {
            throw new InvalidOperationException(
                "MESSAGE_CENTER_TEST_REDIS must point to a disposable Redis database.");
        }

        _options = new RedisOptions
        {
            ConnectionString = connectionString,
            Database = 7,
            KeyPrefix = $"message-center-test-{Guid.NewGuid():N}"
        };
        _redis = ConnectionMultiplexer.Connect(connectionString);
        var services = new ServiceCollection();
        var databaseName = "redis-projection-" + Guid.NewGuid();
        services.AddDbContext<MessageCenterDbContext>(o => o.UseInMemoryDatabase(databaseName)
            .ConfigureWarnings(w => w.Ignore(InMemoryEventId.TransactionIgnoredWarning)));
        services.AddScoped<MessageProjectionAuthority>();
        _provider = services.BuildServiceProvider();
    }

    [Fact]
    public async Task Rebuild_keeps_live_projection_until_atomic_switch()
    {
        var store = CreateStore();
        const string subscriberId = "worker-test-user";

        await store.BeginRebuildAsync(subscriberId, "old");
        await store.UpsertRebuildAsync(
            subscriberId,
            "old",
            new[] { Message("old-1", read: false) });
        await store.CompleteRebuildAsync(subscriberId, "old");

        await store.BeginRebuildAsync(subscriberId, "new");
        await store.UpsertRebuildAsync(
            subscriberId,
            "new",
            new[]
            {
                Message("new-1", read: false),
                Message("new-2", read: false)
            });

        var duringRebuild = await store.GetSummaryAsync(subscriberId);
        Assert.NotNull(duringRebuild);
        Assert.Equal(1, duringRebuild!.TotalCount);

        await store.CompleteRebuildAsync(subscriberId, "new");
        var afterSwitch = await store.GetSummaryAsync(subscriberId);
        Assert.NotNull(afterSwitch);
        Assert.Equal(2, afterSwitch!.TotalCount);
        Assert.Equal(2, afterSwitch.UnreadCount);
    }

    [Fact]
    public async Task Read_state_change_is_applied_to_live_and_staging_projection()
    {
        var store = CreateStore();
        const string subscriberId = "worker-state-test-user";

        await store.BeginRebuildAsync(subscriberId, "old");
        await store.UpsertRebuildAsync(
            subscriberId,
            "old",
            new[] { Message("message-1", read: false) });
        await store.CompleteRebuildAsync(subscriberId, "old");

        await store.BeginRebuildAsync(subscriberId, "new");
        await store.UpsertRebuildAsync(
            subscriberId,
            "new",
            new[]
            {
                Message("message-1", read: false),
                Message("message-2", read: false)
            });

        await store.SetReadStateAsync(subscriberId, "message-1", read: true);
        await store.CompleteRebuildAsync(subscriberId, "new");

        var summary = await store.GetSummaryAsync(subscriberId);
        Assert.NotNull(summary);
        Assert.Equal(2, summary!.TotalCount);
        Assert.Equal(1, summary.UnreadCount);
        var page = await store.GetPageAsync(subscriberId, "all", 1, 10);
        Assert.True(page!.Items.Single(x => x.MessageId == "message-1").Read);
        await store.SetReadStateAsync(subscriberId, "absent", read: false);
        Assert.Equal(2, (await store.GetSummaryAsync(subscriberId))!.TotalCount);
    }

    [Fact]
    public async Task Legacy_ready_value_requires_rebuild_before_any_projection_is_served()
    {
        const string person = "legacy-person";
        await _redis.GetDatabase(_options.Database).StringSetAsync($"{_options.KeyPrefix}:{{{person}}}:ready", "ready");
        var store = CreateStore();
        Assert.False(await store.IsReadyAsync(person));
        Assert.Null(await store.GetPageAsync(person, "all", 1, 10));
        Assert.Null(await store.GetSummaryAsync(person));
        await store.BeginRebuildAsync(person, "p9");
        await store.CompleteRebuildAsync(person, "p9");
        Assert.Equal(0, (await store.GetSummaryAsync(person))!.TotalCount);
    }

    [Theory]
    [InlineData("CONSUME")]
    [InlineData("REVOKE")]
    [InlineData("EXPIRE")]
    public async Task Terminal_invalidates_counts_and_rebuild_never_reimports_original_message(string action)
    {
        const string person = "terminal-person";
        await SeedAsync(person);
        var original = Message("original", false);
        original.TransactionId = "send";
        original.Payload = new() { SendId = "send", SourceSystem = "micip" };
        var unbound = Message("unbound", false);
        unbound.Payload = new() { SourceSystem = "micip" };
        var legacy = Message("legacy", false);
        var store = CreateStore();
        await store.BeginRebuildAsync(person, "before");
        await store.UpsertRebuildAsync(person, "before", new[] { original, unbound, legacy });
        await store.CompleteRebuildAsync(person, "before");
        Assert.Equal(2, (await store.GetSummaryAsync(person))!.UnreadCount);
        await store.SetReadStateAsync(person, "original", true);
        Assert.Equal(1, (await store.GetSummaryAsync(person))!.UnreadCount);
        await TerminalAsync(person, action);
        Assert.Null(await store.GetPageAsync(person, "all", 1, 10));
        Assert.Null(await store.GetSummaryAsync(person));
        Assert.Empty(await store.GetUnreadIdsAsync(person, 10));
        await store.BeginRebuildAsync(person, "after");
        await store.UpsertRebuildAsync(person, "after", new[] { original, unbound, legacy });
        await store.CompleteRebuildAsync(person, "after");
        await store.UpsertAsync(person, new[] { original, unbound });
        var page = await store.GetPageAsync(person, "all", 1, 10);
        Assert.Equal("legacy", Assert.Single(page!.Items).MessageId);
        Assert.Equal(1, page.TotalCount);
        Assert.Equal(1, page.UnreadCount);
    }

    [Fact]
    public async Task Terminal_during_rebuild_prevents_publication_of_old_revision()
    {
        const string person = "revision-person";
        await SeedAsync(person);
        var store = CreateStore();
        await store.BeginRebuildAsync(person, "old");
        await store.UpsertRebuildAsync(person, "old", new[] { Message("legacy", false) });
        await TerminalAsync(person, "REVOKE");
        await Assert.ThrowsAsync<InvalidOperationException>(() => store.CompleteRebuildAsync(person, "old"));
        Assert.Null(await store.GetSummaryAsync(person));
    }

    [Fact]
    public async Task Old_incremental_writer_cannot_write_after_new_generation_is_published()
    {
        const string person = "generation-person";
        var store = CreateStore();
        await store.BeginRebuildAsync(person, "old");
        await store.CompleteRebuildAsync(person, "old");
        await store.BeginRebuildAsync(person, "new");
        await store.UpsertRebuildAsync(person, "new", new[] { Message("current", false) });
        // 在实际Lua执行前切换代际，保留真实Redis和真实Lua裁决。
        var stale = CreateStoreBeforeBatch(() => store.CompleteRebuildAsync(person, "new").GetAwaiter().GetResult());
        await Assert.ThrowsAsync<InvalidOperationException>(() => stale.UpsertAsync(person, new[] { Message("stale", false) }));
        var page = await store.GetPageAsync(person, "all", 1, 10);
        Assert.Equal("current", Assert.Single(page!.Items).MessageId);
        Assert.Equal(1, page.UnreadCount);
    }

    [Fact]
    public async Task Old_staging_writer_cannot_restore_abandoned_generation_keys()
    {
        const string person = "staging-person";
        var store = CreateStore();
        await store.BeginRebuildAsync(person, "old");
        var stale = CreateStoreBeforeBatch(() => {
            store.BeginRebuildAsync(person, "new").GetAwaiter().GetResult();
            store.AbandonRebuildAsync(person, "old").GetAwaiter().GetResult();
        });
        await Assert.ThrowsAsync<InvalidOperationException>(() => stale.UpsertRebuildAsync(person, "old", new[] { Message("stale", false) }));
        Assert.False(await _redis.GetDatabase(_options.Database).KeyExistsAsync($"{_options.KeyPrefix}:{{{person}}}:rebuild:old:messages"));
        await store.CompleteRebuildAsync(person, "new");
        Assert.Equal(0, (await store.GetSummaryAsync(person))!.UnreadCount);
    }

    private async Task SeedAsync(string person)
    {
        using var scope = _provider.CreateScope();
        var db = scope.ServiceProvider.GetRequiredService<MessageCenterDbContext>();
        db.MessageOutbox.Add(new() {
            SendId = "send", RequestId = "request", SubscriberId = person, SourceSystem = "micip",
            WorkflowId = "fixture", RequestHash = "hash", Payload = "{}", Status = MessageOutboxState.Visible,
            BusinessState = MessageBusinessState.Active, NovuTransactionId = "send"
        });
        await db.SaveChangesAsync();
    }

    private async Task TerminalAsync(string person, string action)
    {
        using var scope = _provider.CreateScope();
        await new MessageLifecycleStore(scope.ServiceProvider.GetRequiredService<MessageCenterDbContext>())
            .ApplyAsync(new("micip", "request", person, "operation", action, 1, null, null), default);
    }

    private MessageProjectionStore CreateStoreBeforeBatch(Action beforeBatch)
    {
        var connection = ForwardProxy.Wrap<IConnectionMultiplexer>(_redis, (method, result) =>
            method == "GetDatabase" ? ForwardProxy.Wrap<IDatabase>(result!, (name, value) =>
                name == "CreateBatch" ? ForwardProxy.Wrap<IBatch>(value!, before: beforeBatch) : value) : result);
        return new(connection, Options.Create(_options), _provider.GetRequiredService<IServiceScopeFactory>());
    }

    public class ForwardProxy : DispatchProxy
    {
        private object _target = null!;
        private Func<string, object?, object?>? _after;
        private Action? _before;
        public static T Wrap<T>(object target, Func<string, object?, object?>? after = null, Action? before = null) where T : class
        {
            var instance = Create<T, ForwardProxy>();
            var proxy = (ForwardProxy)(object)instance;
            proxy._target = target; proxy._after = after; proxy._before = before;
            return instance;
        }
        protected override object? Invoke(MethodInfo? method, object?[]? args)
        {
            if (method!.Name == "Execute") _before?.Invoke();
            var result = method.Invoke(_target, args);
            return _after is null ? result : _after(method.Name, result);
        }
    }

    [Fact]
    public async Task Pending_stream_job_is_reclaimed_by_another_consumer()
    {
        var queue = CreateQueue();
        await queue.SetHeartbeatAsync("consumer-one");
        var heartbeat = await queue.GetHeartbeatAsync();
        Assert.NotNull(heartbeat);
        Assert.Equal("consumer-one", heartbeat!.Worker);

        await queue.EnqueueRebuildAsync("worker-recovery-test-user");
        await queue.EnsureConsumerGroupAsync();

        var first = await queue.ClaimAsync("consumer-one", TimeSpan.FromMinutes(5));
        Assert.NotNull(first);

        var recovered = await queue.ClaimAsync("consumer-two", TimeSpan.Zero);
        Assert.NotNull(recovered);
        Assert.Equal(first!.Item.Id, recovered!.Item.Id);
        Assert.Equal(first.StreamId, recovered.StreamId);

        await queue.CompleteAsync(recovered);
    }

    public Task InitializeAsync() => Task.CompletedTask;

    public async Task DisposeAsync()
    {
        var database = _redis.GetDatabase(_options.Database);
        foreach (var endpoint in _redis.GetEndPoints())
        {
            var server = _redis.GetServer(endpoint);
            var keys = server.Keys(
                    _options.Database,
                    $"{_options.KeyPrefix}:*")
                .ToArray();
            if (keys.Length > 0)
            {
                await database.KeyDeleteAsync(keys);
            }
        }
        await _redis.CloseAsync();
        _redis.Dispose();
        await _provider.DisposeAsync();
    }

    private MessageProjectionStore CreateStore()
        => new(_redis, Options.Create(_options), _provider.GetRequiredService<IServiceScopeFactory>());

    private MessageWorkQueue CreateQueue()
        => new(_redis, Options.Create(_options));

    private static NovuMessageItem Message(string id, bool read)
        => new()
        {
            Id = id,
            Subject = id,
            Content = id,
            Read = read,
            CreatedAt = DateTime.UtcNow
        };
}
