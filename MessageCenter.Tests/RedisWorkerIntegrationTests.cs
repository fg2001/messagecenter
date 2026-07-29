using MessageCenter.Api.HttpClients.Dtos;
using MessageCenter.Api.Options;
using MessageCenter.Api.Services;
using Microsoft.Extensions.Options;
using StackExchange.Redis;
using Xunit;

namespace MessageCenter.Tests;

public sealed class RedisWorkerIntegrationTests : IAsyncLifetime
{
    private readonly ConnectionMultiplexer _redis;
    private readonly RedisOptions _options;

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
    }

    private MessageProjectionStore CreateStore()
        => new(_redis, Options.Create(_options));

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
