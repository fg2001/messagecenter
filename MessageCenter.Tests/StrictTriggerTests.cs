using System.Net;
using System.Text;
using System.Text.Json;
using MessageCenter.Api.Data;
using MessageCenter.Api.HttpClients;
using MessageCenter.Api.Options;
using MessageCenter.Api.Outbox;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Logging.Abstractions;
using System.Reflection;
using Xunit;

namespace MessageCenter.Tests;

/// <summary>PR4：严格触发来源的不确定结果控制契约。
/// 客户端分类（超时/5xx=不确定、4xx=可安全重试）+ uncertain 冻结/认领排除/人工对账收敛。</summary>
public sealed class StrictTriggerTests
{
    // ---- 客户端分类 ----

    [Fact]
    public async Task Timeout_classifies_as_uncertain()
    {
        var handler = new UncertainHandler(() => throw new TaskCanceledException("timeout"));
        var error = await Assert.ThrowsAsync<NovuTriggerUncertainException>(() =>
            Client(handler).TriggerAsync("wf", "sub", new { }, "send-1"));
        Assert.Contains("uncertain", error.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task Server_5xx_classifies_as_uncertain()
    {
        var handler = new UncertainHandler(() => new HttpResponseMessage(HttpStatusCode.BadGateway));
        await Assert.ThrowsAsync<NovuTriggerUncertainException>(() =>
            Client(handler).TriggerAsync("wf", "sub", new { }, "send-1"));
    }

    [Fact]
    public async Task Client_4xx_stays_plain_HttpRequestException_for_safe_retry()
    {
        var handler = new UncertainHandler(() => new HttpResponseMessage(HttpStatusCode.BadRequest));
        await Assert.ThrowsAsync<HttpRequestException>(() =>
            Client(handler).TriggerAsync("wf", "sub", new { }, "send-1"));
    }

    [Fact]
    public async Task Receipt_mismatch_is_typed_but_still_InvalidOperation_for_PR3_contract()
    {
        var handler = new UncertainHandler(() => Json(new { data = new
            { acknowledged = true, status = "processed", transactionId = "other" } }));
        var error = await Assert.ThrowsAnyAsync<InvalidOperationException>(() =>
            Client(handler).TriggerAsync("wf", "sub", new { }, "send-1"));
        Assert.IsType<NovuTriggerReceiptMismatchException>(error);
        Assert.Equal("Novu trigger transaction mismatch.", error.Message);
    }

    // ---- 选项 ----

    [Fact]
    public void Strict_sources_default_off_and_exact_match()
    {
        var options = new NovuOptions();
        Assert.False(options.IsStrictTriggerSource("micip"));
        options.StrictTriggerSources.Add("micip");
        Assert.True(options.IsStrictTriggerSource("micip"));
        Assert.False(options.IsStrictTriggerSource("Micip"));
        Assert.False(options.IsStrictTriggerSource("micip-svc"));
    }

    // ---- uncertain 冻结与认领排除（内存数据库）----

    [Fact]
    public async Task MarkTriggerUncertain_freezes_and_is_never_claimed_again()
    {
        await using var fx0 = await SeedClaimedRow("micip"); var store = fx0.NewStore();
        await store.MarkTriggerUncertainAsync("send-1", "w1", "simulated timeout", default);

        var row = await fx0.NewDb().MessageOutbox.AsNoTracking().SingleAsync();
        Assert.Equal(MessageOutboxState.Uncertain, row.Status);
        Assert.Null(row.NextRetryAt);
        Assert.Null(row.LockedBy);
        Assert.StartsWith("TRIGGER_UNCERTAIN:", row.LastError);
        Assert.NotNull(row.UncertainAt);
        // 认领谓词：仅 pending/accepted 且到期可被认领——uncertain 永不进入下一轮 POST。
        Assert.DoesNotContain(row.Status, new[] { MessageOutboxState.Pending, MessageOutboxState.Accepted });
    }

    [Fact]
    public async Task MarkTriggerAttempt_increments_attempt_counter()
    {
        await using var fx1 = await SeedClaimedRow("cooper"); var store = fx1.NewStore();
        await store.MarkTriggerAttemptAsync("send-1", "w1", default);
        await store.MarkTriggerAttemptAsync("send-1", "w1", default);
        var row = await fx1.NewDb().MessageOutbox.AsNoTracking().SingleAsync();
        Assert.Equal(2, row.TriggerAttempts);
        Assert.NotNull(row.LastTriggerAt);
    }

    [Fact]
    public async Task Strict_attempt_persists_uncertain_before_http_and_survives_new_scope()
    {
        await using var fx = await SeedClaimedRow("micip");
        Assert.True(await fx.NewStore().MarkTriggerAttemptAsync("send-1", "w1", default));
        var recovered = await fx.NewDb().MessageOutbox.AsNoTracking().SingleAsync();
        Assert.Equal(MessageOutboxState.Uncertain, recovered.Status);
        Assert.NotNull(recovered.UncertainAt);
        Assert.Null(recovered.NextRetryAt);
        Assert.Equal("w1", recovered.LockedBy);
        Assert.Equal(1, recovered.TriggerAttempts);
    }

    [Fact]
    public async Task Strict_attempt_cannot_issue_a_second_post_permit()
    {
        await using var fx = await SeedClaimedRow("micip");
        var store = fx.NewStore();
        await store.MarkTriggerAttemptAsync("send-1", "w1", default);
        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            store.MarkTriggerAttemptAsync("send-1", "w1", default));
    }

    [Fact]
    public async Task Expired_uncertain_lease_can_be_reconciled_but_late_worker_cannot_complete()
    {
        await using var fx = await SeedClaimedRow("micip");
        var db = fx.NewDb();
        var row = await db.MessageOutbox.SingleAsync();
        row.Status = MessageOutboxState.Uncertain;
        row.LockedUntil = DateTime.UtcNow.AddMinutes(-1);
        await db.SaveChangesAsync();
        var resolved = await fx.NewStore().ResolveTriggerUncertainAsync(
            "micip", "req-1", "send-1", "CONFIRM_NOT_SENT", default);
        Assert.Equal(MessageOutboxState.Pending, resolved.Status);
        Assert.Null(resolved.LockedBy);
        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            fx.NewStore().MarkTriggerAttemptAsync("send-1", "w1", default));
    }

    [Fact]
    public async Task Worker_invalid_acknowledgement_does_not_restore_automatic_retry()
    {
        var handler = new UncertainHandler(() => Json(new { data = new {
            acknowledged = false, status = "rejected", transactionId = "send-1" } }));
        await using var fx = await SeedClaimedRow("micip", handler);
        var policy = new NovuOptions(); policy.StrictTriggerSources.Add("micip");
        using var worker = new MessageOutboxWorker(fx.Provider.GetRequiredService<IServiceScopeFactory>(),
            Options.Create(new MessageWorkerOptions()), Options.Create(policy), NullLogger<MessageOutboxWorker>.Instance);
        var workerId = (string)typeof(MessageOutboxWorker).GetField("_worker", BindingFlags.NonPublic | BindingFlags.Instance)!.GetValue(worker)!;
        var db = fx.NewDb(); var row = await db.MessageOutbox.SingleAsync(); row.LockedBy = workerId;
        await db.SaveChangesAsync();
        var claimed = new ClaimedMessageOutboxItem("send-1", "179070", "micip-test-inapp", "{}",
            MessageOutboxState.Pending, MessageBusinessState.Active, 0, null, "micip");
        var task = (Task)typeof(MessageOutboxWorker).GetMethod("SendAsync", BindingFlags.NonPublic | BindingFlags.Instance)!
            .Invoke(worker, new object[] { claimed, fx.Provider, fx.NewStore(), CancellationToken.None })!;
        await task;
        var after = await fx.NewDb().MessageOutbox.AsNoTracking().SingleAsync();
        Assert.Equal(MessageOutboxState.Uncertain, after.Status);
        Assert.Null(after.NextRetryAt);
        Assert.Equal(0, after.RetryCount);
    }

    // ---- 人工对账收敛 ----

    [Fact]
    public async Task Resolve_CONFIRM_SENT_moves_to_accepted_with_manual_status()
    {
        await using var fx0 = await SeedClaimedRow("micip"); var store = fx0.NewStore();
        await store.MarkTriggerUncertainAsync("send-1", "w1", "timeout", default);
        var resolved = await store.ResolveTriggerUncertainAsync(
            "micip", "req-1", "send-1", "CONFIRM_SENT", default);

        Assert.Equal(MessageOutboxState.Accepted, resolved.Status);
        Assert.Equal("send-1", resolved.NovuTransactionId);
        Assert.Equal("manual-confirm-sent", resolved.NovuStatus);
        Assert.Equal("CONFIRM_SENT", resolved.UncertainResolution);
        Assert.NotNull(resolved.AcceptedAt);
    }

    [Fact]
    public async Task Resolve_CONFIRM_NOT_SENT_returns_to_pending_once_with_same_key()
    {
        await using var fx0 = await SeedClaimedRow("micip"); var store = fx0.NewStore();
        await store.MarkTriggerUncertainAsync("send-1", "w1", "timeout", default);
        var before = await fx0.NewDb().MessageOutbox.AsNoTracking().SingleAsync();
        var resolved = await store.ResolveTriggerUncertainAsync(
            "micip", "req-1", "send-1", "CONFIRM_NOT_SENT", default);

        Assert.Equal(MessageOutboxState.Pending, resolved.Status);
        Assert.Equal(before.RetryCount + 1, resolved.RetryCount);
        Assert.Equal("CONFIRM_NOT_SENT", resolved.UncertainResolution);
        // 重发沿用原键 sendId 作为 transactionId（Worker 触发时 item.SendId 不变），Novu 幂等兜底去重。
    }

    [Fact]
    public async Task Resolve_rejects_non_uncertain_or_cross_request_rows()
    {
        await using var fx = await SeedClaimedRow("micip"); var store = fx.NewStore();
        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            store.ResolveTriggerUncertainAsync("micip", "req-1", "send-1", "CONFIRM_SENT", default));
        await Assert.ThrowsAsync<MessageOutboxConflictException>(() =>
            store.ResolveTriggerUncertainAsync("micip", "req-other", "send-1", "CONFIRM_SENT", default));
        await Assert.ThrowsAsync<MessageOutboxConflictException>(() =>
            store.ResolveTriggerUncertainAsync("cooper-value", "req-1", "send-1", "CONFIRM_SENT", default));
    }

    [Fact]
    public async Task Resolve_rejects_invalid_resolution_value()
    {
        await using var fx = await SeedClaimedRow("micip"); var store = fx.NewStore();
        await store.MarkTriggerUncertainAsync("send-1", "w1", "timeout", default);
        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            store.ResolveTriggerUncertainAsync("micip", "req-1", "send-1", "RESEND", default));
    }

    [Fact]
    public async Task Double_resolve_is_rejected_after_first_convergence()
    {
        await using var fx = await SeedClaimedRow("micip"); var store = fx.NewStore();
        await store.MarkTriggerUncertainAsync("send-1", "w1", "timeout", default);
        await store.ResolveTriggerUncertainAsync("micip", "req-1", "send-1", "CONFIRM_SENT", default);
        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            store.ResolveTriggerUncertainAsync("micip", "req-1", "send-1", "CONFIRM_SENT", default));
    }

    // ---- 夹具 ----

    private static NovuClient Client(HttpMessageHandler handler) => new(new HttpClient(handler)
    {
        BaseAddress = new Uri("http://novu.test")
    });

    private static HttpResponseMessage Json(object body) => new(HttpStatusCode.Created)
    {
        Content = new StringContent(JsonSerializer.Serialize(body), Encoding.UTF8, "application/json")
    };

    private sealed class StoreFixture : IAsyncDisposable
    {
        public readonly ServiceProvider Provider;
        public StoreFixture(HttpMessageHandler? handler = null)
        {
            // 库名在 lambda 外求值一次：同一 fixture 的所有 scope 共享同一 InMemory 库，
            // 模拟"Worker 操作 scope / 状态查询 scope"跨作用积可见（与生产 DM8 行为一致）。
            var databaseName = "strict-" + Guid.NewGuid();
            var services = new ServiceCollection();
            services.Configure<NovuOptions>(o => o.StrictTriggerSources.Add("micip"));
            if (handler is not null) services.AddSingleton(Client(handler));
            services.AddDbContext<MessageCenterDbContext>(o =>
                o.UseInMemoryDatabase(databaseName));
            services.AddTransient<MessageOutboxStore>();
            Provider = services.BuildServiceProvider();
        }

        public MessageOutboxStore NewStore()
        {
            var scope = Provider.CreateScope();
            Scopes.Add(scope);
            return scope.ServiceProvider.GetRequiredService<MessageOutboxStore>();
        }

        public MessageCenterDbContext NewDb()
        {
            var scope = Provider.CreateScope();
            Scopes.Add(scope);
            return scope.ServiceProvider.GetRequiredService<MessageCenterDbContext>();
        }

        private readonly List<IServiceScope> Scopes = new();

        public async ValueTask DisposeAsync()
        {
            foreach (var scope in Scopes) scope.Dispose();
            await Provider.DisposeAsync();
        }
    }

    private static async Task<StoreFixture> SeedClaimedRow(string sourceSystem, HttpMessageHandler? handler = null)
    {
        var fixture = new StoreFixture(handler);
        var db = fixture.NewDb();
        db.MessageOutbox.Add(new MessageOutboxEntity
        {
            SendId = "send-1",
            RequestId = "req-1",
            RequestHash = "hash",
            SourceSystem = sourceSystem,
            SubscriberId = "179070",
            WorkflowId = "micip-test-inapp",
            Payload = "{}",
            Status = MessageOutboxState.Pending,
            LockedBy = "w1",
            LockedUntil = DateTime.UtcNow.AddMinutes(5)
        });
        await db.SaveChangesAsync();
        return fixture;
    }

    private sealed class UncertainHandler : HttpMessageHandler
    {
        private readonly Func<object> _respond;
        public UncertainHandler(Func<object> respond) => _respond = respond;

        protected override Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request, CancellationToken cancellationToken)
        {
            var result = _respond();
            return Task.FromResult(result switch
            {
                HttpResponseMessage message => message,
                _ => (HttpResponseMessage)result
            });
        }
    }
}
