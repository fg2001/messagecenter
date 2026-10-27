using System.Net;
using System.Text;
using System.Text.Json;
using MessageCenter.Api.Data;
using MessageCenter.Api.HttpClients;
using MessageCenter.Api.Models;
using MessageCenter.Api.Outbox;
using MessageCenter.Api.Services;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace MessageCenter.Tests;

/// <summary>PR5：消息生命周期契约（T-INT-01 候选稿 §6）。
/// operationId 幂等（同载荷 202 原结果/异载荷 409）、businessVersion 防倒退（≤ 当前 422）、
/// REVOKE/EXPIRE 原键终止标记在 send 到达前暂存、迟到 send 受理即套用终态且抑制发送。</summary>
public sealed class MessageLifecycleTests
{
    // ---- 幂等与防倒退 ----

    [Fact]
    public async Task Consume_visible_row_sets_terminal_business_state_only()
    {
        await using var fx = await SeedRow("micip", status: MessageOutboxState.Visible);
        var store = fx.NewLifecycleStore();

        var result = await store.ApplyAsync(Input(action: "CONSUME", version: 1), default);

        Assert.Equal("applied", result.ApplyStatus);
        Assert.Equal(MessageBusinessState.Consumed, result.ResultBusinessState);
        Assert.Equal("send-1", result.SendId);

        var row = await fx.NewDb().MessageOutbox.AsNoTracking().SingleAsync();
        Assert.Equal(MessageBusinessState.Consumed, row.BusinessState);
        Assert.Equal(1, row.BusinessVersion);
        Assert.NotNull(row.TerminalAt);
        // 已可见消息的回执历史不被改写：收敛由 Novu 删除/标记已读与投影重建负责。
        Assert.Equal(MessageOutboxState.Visible, row.Status);
    }

    [Fact]
    public async Task Replay_same_operation_returns_original_result_without_double_apply()
    {
        await using var fx = await SeedRow("micip", status: MessageOutboxState.Visible);
        var store = fx.NewLifecycleStore();
        var first = await store.ApplyAsync(Input(action: "CONSUME", version: 1), default);
        var second = await store.ApplyAsync(Input(action: "CONSUME", version: 1), default);

        Assert.False(first.Replayed);
        Assert.True(second.Replayed);
        Assert.Equal(first.RecordedAt, second.RecordedAt);
        Assert.Equal(first.ResultBusinessState, second.ResultBusinessState);

        var db = fx.NewDb();
        var row = await db.MessageOutbox.AsNoTracking().SingleAsync();
        Assert.Equal(1, row.BusinessVersion);
        Assert.Single(await db.MessageLifecycleOperations.AsNoTracking().ToListAsync());
    }

    [Fact]
    public async Task Replay_with_different_payload_conflicts()
    {
        await using var fx = await SeedRow("micip", status: MessageOutboxState.Visible);
        var store = fx.NewLifecycleStore();
        await store.ApplyAsync(Input(action: "CONSUME", version: 1), default);

        await Assert.ThrowsAsync<MessageLifecycleConflictException>(() =>
            store.ApplyAsync(Input(action: "REVOKE", version: 1), default));
        await Assert.ThrowsAsync<MessageLifecycleConflictException>(() =>
            store.ApplyAsync(Input(action: "CONSUME", version: 2), default));
    }

    [Fact]
    public async Task Version_not_greater_than_current_is_rejected()
    {
        await using var fx = await SeedRow("micip", status: MessageOutboxState.Visible);
        var store = fx.NewLifecycleStore();
        await store.ApplyAsync(Input(operationId: "op-consume", action: "CONSUME", version: 2), default);

        // 等值（v2）与更低（v1）一律拒绝：防倒退，也挡并发旧写。
        await Assert.ThrowsAsync<MessageLifecycleVersionRejectException>(() =>
            store.ApplyAsync(Input(operationId: "op-same", action: "REVOKE", version: 2), default));
        await Assert.ThrowsAsync<MessageLifecycleVersionRejectException>(() =>
            store.ApplyAsync(Input(operationId: "op-lower", action: "REVOKE", version: 1), default));

        // 首个操作 v0（≤ 初始 0）同样拒绝。
        await using var fx2 = await SeedRow("micip", requestId: "req-2", status: MessageOutboxState.Visible);
        await Assert.ThrowsAsync<MessageLifecycleVersionRejectException>(() =>
            fx2.NewLifecycleStore().ApplyAsync(
                Input(requestId: "req-2", action: "REVOKE", version: 0), default));
    }

    [Fact]
    public async Task Higher_version_moves_terminal_state_forward()
    {
        await using var fx = await SeedRow("micip", status: MessageOutboxState.Visible);
        var store = fx.NewLifecycleStore();
        await store.ApplyAsync(Input(operationId: "op-consume", action: "CONSUME", version: 1), default);
        var moved = await store.ApplyAsync(Input(operationId: "op-expire", action: "EXPIRE", version: 3), default);

        Assert.Equal(MessageBusinessState.Expired, moved.ResultBusinessState);
        var row = await fx.NewDb().MessageOutbox.AsNoTracking().SingleAsync();
        Assert.Equal(MessageBusinessState.Expired, row.BusinessState);
        Assert.Equal(3, row.BusinessVersion);
    }

    // ---- 行不存在：CONSUME 404，REVOKE/EXPIRE 暂存 ----

    [Fact]
    public async Task Consume_missing_row_is_not_found()
    {
        await using var fx = new StoreFixture();
        await Assert.ThrowsAsync<MessageLifecycleNotFoundException>(() =>
            fx.NewLifecycleStore().ApplyAsync(Input(action: "CONSUME", version: 1), default));
    }

    [Fact]
    public async Task Revoke_before_send_persists_deferred_marker_by_original_key()
    {
        await using var fx = new StoreFixture();
        var result = await fx.NewLifecycleStore().ApplyAsync(
            Input(action: "REVOKE", version: 1), default);

        Assert.Equal("deferred", result.ApplyStatus);
        Assert.Equal(MessageBusinessState.Revoked, result.ResultBusinessState);
        Assert.Null(result.SendId);

        var op = await fx.NewDb().MessageLifecycleOperations.AsNoTracking().SingleAsync();
        Assert.Equal(MessageLifecycleApplyStatus.Deferred, op.ApplyStatus);
        Assert.Equal("req-1", op.RequestId);
        Assert.Equal("179070", op.SubscriberId);
    }

    [Theory]
    [InlineData(2)]
    [InlineData(1)]
    public async Task Deferred_marker_rejects_equal_or_lower_version_without_message_row(long version)
    {
        await using var fx = new StoreFixture();
        await fx.NewLifecycleStore().ApplyAsync(Input(operationId: "op-high", action: "REVOKE", version: 2), default);
        await Assert.ThrowsAsync<MessageLifecycleVersionRejectException>(() =>
            fx.NewLifecycleStore().ApplyAsync(Input(operationId: "op-late", action: "EXPIRE", version: version), default));
        Assert.Equal(1, await fx.NewDb().MessageLifecycleOperations.CountAsync());
    }

    [Fact]
    public async Task Consume_missing_row_remains_not_found_even_with_existing_marker()
    {
        await using var fx = new StoreFixture();
        await fx.NewLifecycleStore().ApplyAsync(Input(operationId: "op-high", action: "REVOKE", version: 2), default);
        await Assert.ThrowsAsync<MessageLifecycleNotFoundException>(() =>
            fx.NewLifecycleStore().ApplyAsync(Input(operationId: "op-consume", action: "CONSUME", version: 1), default));
    }

    [Fact]
    public async Task Deferred_marker_suppresses_late_send_and_is_never_claimed()
    {
        await using var fx = new StoreFixture();
        await fx.NewLifecycleStore().ApplyAsync(Input(action: "REVOKE", version: 1), default);

        var response = await fx.NewOutboxStore().EnqueueAsync(
            "micip",
            SendRequest(),
            new[] { Trigger("179070") },
            Array.Empty<string>(),
            default);

        Assert.Equal(MessageOutboxState.Suppressed, response.State);
        var db = fx.NewDb();
        var row = await db.MessageOutbox.AsNoTracking().SingleAsync();
        Assert.Equal(MessageOutboxState.Suppressed, row.Status);
        Assert.Equal(MessageBusinessState.Revoked, row.BusinessState);
        Assert.Equal(1, row.BusinessVersion);
        Assert.Null(row.NextRetryAt);

        var op = await db.MessageLifecycleOperations.AsNoTracking().SingleAsync();
        Assert.Equal(MessageLifecycleApplyStatus.Applied, op.ApplyStatus);
        Assert.Equal(row.SendId, op.SendId);

        // 认领谓词仅认领 pending/accepted：suppressed 行即使到期也不会被 Worker 再触发。
        var claimed = await fx.NewOutboxStore().ClaimDueAsync("w-late", 10, TimeSpan.FromMinutes(5), default);
        Assert.DoesNotContain(claimed, x => x.SendId == row.SendId);
    }

    [Fact]
    public async Task Highest_version_deferred_marker_wins_and_lower_is_superseded()
    {
        await using var fx = new StoreFixture();
        var lifecycle = fx.NewLifecycleStore();
        await lifecycle.ApplyAsync(Input(operationId: "op-v1", action: "REVOKE", version: 1), default);
        await lifecycle.ApplyAsync(Input(operationId: "op-v2", action: "EXPIRE", version: 2), default);

        await fx.NewOutboxStore().EnqueueAsync(
            "micip", SendRequest(), new[] { Trigger("179070") }, Array.Empty<string>(), default);

        var row = await fx.NewDb().MessageOutbox.AsNoTracking().SingleAsync();
        Assert.Equal(MessageBusinessState.Expired, row.BusinessState);
        Assert.Equal(2, row.BusinessVersion);

        var ops = await fx.NewDb().MessageLifecycleOperations.AsNoTracking()
            .OrderBy(x => x.BusinessVersion).ToListAsync();
        Assert.Equal(MessageLifecycleApplyStatus.Applied, ops[1].ApplyStatus);
        Assert.Equal(MessageLifecycleApplyStatus.Superseded, ops[0].ApplyStatus);
    }

    // ---- 已存在行上的终态与抑制 ----

    [Fact]
    public async Task Terminal_on_pending_row_suppresses_send_and_releases_lease()
    {
        await using var fx = await SeedRow("micip", status: MessageOutboxState.Pending, leased: true);
        var result = await fx.NewLifecycleStore().ApplyAsync(
            Input(action: "EXPIRE", version: 1), default);

        Assert.Equal("applied", result.ApplyStatus);
        var row = await fx.NewDb().MessageOutbox.AsNoTracking().SingleAsync();
        Assert.Equal(MessageOutboxState.Suppressed, row.Status);
        Assert.Equal(MessageBusinessState.Expired, row.BusinessState);
        Assert.Null(row.NextRetryAt);
        Assert.Null(row.LockedBy);
        Assert.Null(row.LockedUntil);
    }

    [Fact]
    public async Task Terminal_on_uncertain_row_keeps_uncertain_for_audit()
    {
        await using var fx = await SeedRow("micip", status: MessageOutboxState.Uncertain);
        await fx.NewLifecycleStore().ApplyAsync(Input(action: "REVOKE", version: 1), default);

        var row = await fx.NewDb().MessageOutbox.AsNoTracking().SingleAsync();
        // uncertain 冻结语义不变（供人工对账）；业务终态独立生效。
        Assert.Equal(MessageOutboxState.Uncertain, row.Status);
        Assert.Equal(MessageBusinessState.Revoked, row.BusinessState);
    }

    [Fact]
    public async Task Resolve_CONFIRM_NOT_SENT_after_terminal_stays_suppressed()
    {
        await using var fx = await SeedRow("micip", status: MessageOutboxState.Uncertain);
        await fx.NewLifecycleStore().ApplyAsync(Input(action: "REVOKE", version: 1), default);
        var resolved = await fx.NewOutboxStore().ResolveTriggerUncertainAsync(
            "micip", "req-1", "send-1", "CONFIRM_NOT_SENT", default);

        // 已撤回的消息不得以原键补投复活。
        Assert.Equal(MessageOutboxState.Suppressed, resolved.Status);
        Assert.Equal(MessageBusinessState.Revoked, resolved.BusinessState);
    }

    [Fact]
    public async Task TrySuppressTerminal_intercepts_visibility_pass_for_terminal_row()
    {
        // 可见性补投前终态到达（accepted→visible 窗口）：行被抑制，消息不得投进收件箱。
        await using var fx = await SeedRow(
            "micip", status: MessageOutboxState.Accepted, leased: true,
            businessState: MessageBusinessState.Revoked);
        var suppressed = await fx.NewOutboxStore().TrySuppressTerminalAsync("send-1", "w1", default);

        Assert.True(suppressed);
        var row = await fx.NewDb().MessageOutbox.AsNoTracking().SingleAsync();
        Assert.Equal(MessageOutboxState.Suppressed, row.Status);
        Assert.Equal(MessageBusinessState.Revoked, row.BusinessState);
        Assert.Null(row.LockedBy);
    }

    [Fact]
    public async Task TrySuppressTerminal_leaves_active_row_to_normal_visibility()
    {
        await using var fx = await SeedRow(
            "micip", status: MessageOutboxState.Accepted, leased: true);
        var suppressed = await fx.NewOutboxStore().TrySuppressTerminalAsync("send-1", "w1", default);

        Assert.False(suppressed);
        var row = await fx.NewDb().MessageOutbox.AsNoTracking().SingleAsync();
        Assert.Equal(MessageOutboxState.Accepted, row.Status);
        Assert.Equal("w1", row.LockedBy);
    }

    [Fact]
    public async Task MarkTriggerAttempt_refuses_and_suppresses_terminal_claimed_row()
    {
        await using var fx = await SeedRow(
            "micip", status: MessageOutboxState.Pending, leased: true,
            businessState: MessageBusinessState.Consumed);
        var allowed = await fx.NewOutboxStore().MarkTriggerAttemptAsync("send-1", "w1", default);

        Assert.False(allowed);
        var row = await fx.NewDb().MessageOutbox.AsNoTracking().SingleAsync();
        Assert.Equal(MessageOutboxState.Suppressed, row.Status);
        Assert.Equal(0, row.TriggerAttempts);
    }

    // ---- 回读与状态查询 ----

    [Fact]
    public async Task GetOperation_reads_back_only_within_same_request_scope()
    {
        await using var fx = await SeedRow("micip", status: MessageOutboxState.Visible);
        await fx.NewLifecycleStore().ApplyAsync(
            Input(operationId: "op-9", action: "CONSUME", version: 1), default);

        var found = await fx.NewLifecycleStore().GetOperationAsync("micip", "req-1", "op-9", default);
        Assert.NotNull(found);
        Assert.Equal("CONSUME", found!.Action);

        Assert.Null(await fx.NewLifecycleStore().GetOperationAsync("micip", "req-other", "op-9", default));
        Assert.Null(await fx.NewLifecycleStore().GetOperationAsync("cooper-value", "req-1", "op-9", default));
    }

    [Fact]
    public async Task Status_response_exposes_business_state_only_with_include_terminal()
    {
        await using var fx = await SeedRow("micip", status: MessageOutboxState.Visible);
        await fx.NewLifecycleStore().ApplyAsync(Input(action: "REVOKE", version: 1), default);

        var plain = await fx.NewOutboxStore().GetRequestAsync("micip", "req-1", default);
        var terminal = await fx.NewOutboxStore().GetRequestAsync(
            "micip", "req-1", default, includeTerminal: true);

        Assert.NotNull(plain);
        Assert.NotNull(terminal);
        Assert.Null(plain!.Recipients.Single().BusinessState);
        var item = terminal!.Recipients.Single();
        Assert.Equal(MessageBusinessState.Revoked, item.BusinessState);
        Assert.Equal(1, item.BusinessVersion);
        Assert.NotNull(item.TerminalAt);
    }

    // ---- Novu 删除（撤回/过期收敛客户端）----

    [Fact]
    public async Task DeleteMessage_treats_404_as_already_converged()
    {
        var handler = new StubHandler(() => new HttpResponseMessage(HttpStatusCode.NotFound));
        await Client(handler).DeleteMessageAsync("msg-1");

        var ok = new StubHandler(() => new HttpResponseMessage(HttpStatusCode.OK));
        await Client(ok).DeleteMessageAsync("msg-1");

        await Assert.ThrowsAsync<HttpRequestException>(() =>
            Client(new StubHandler(() => new HttpResponseMessage(HttpStatusCode.InternalServerError)))
                .DeleteMessageAsync("msg-1"));
    }

    // ---- 夹具 ----

    private static LifecycleApplyInput Input(
        string sourceSystem = "micip",
        string requestId = "req-1",
        string subscriberId = "179070",
        string operationId = "op-1",
        string action = "CONSUME",
        long version = 1,
        string? reason = null) =>
        new(sourceSystem, requestId, subscriberId, operationId, action, version, reason, OccurredAt: null);

    private static NovuClient Client(HttpMessageHandler handler) => new(new HttpClient(handler)
    {
        BaseAddress = new Uri("http://novu.test")
    });

    private static SendMessageRequest SendRequest(string requestId = "req-1") => new()
    {
        RequestId = requestId,
        BusinessType = "issue-assigned",
        BusinessId = "300001",
        Title = "t",
        Receivers = { new Receiver { Type = "user", Id = "179070" } }
    };

    private static NovuTriggerPayload Trigger(string subscriberId) => new()
    {
        WorkflowId = "micip-test-inapp",
        SubscriberId = subscriberId,
        Payload = new NovuPayloadFields
        {
            SourceSystem = "micip",
            BusinessType = "issue-assigned",
            BusinessId = "300001",
            Title = "t"
        }
    };

    private sealed class StoreFixture : IAsyncDisposable
    {
        public readonly ServiceProvider Provider;

        public StoreFixture()
        {
            // 库名在 lambda 外求值一次：同一 fixture 的所有 scope 共享同一 InMemory 库，
            // 模拟"生命周期/受理/状态查询"跨 scope 可见（与生产 DM8 行为一致）。
            var databaseName = "lifecycle-" + Guid.NewGuid();
            var services = new ServiceCollection();
            services.AddDbContext<MessageCenterDbContext>(o =>
                o.UseInMemoryDatabase(databaseName)
                    // EnqueueAsync 在受理事务内套用终止标记；InMemory 不支持事务，按警告忽略（生产行为见 DM8）。
                    .ConfigureWarnings(w => w.Ignore(InMemoryEventId.TransactionIgnoredWarning)));
            services.AddTransient<MessageOutboxStore>();
            services.AddTransient<MessageLifecycleStore>();
            Provider = services.BuildServiceProvider();
        }

        public MessageOutboxStore NewOutboxStore() => Resolve<MessageOutboxStore>();
        public MessageLifecycleStore NewLifecycleStore() => Resolve<MessageLifecycleStore>();
        public MessageCenterDbContext NewDb() => Resolve<MessageCenterDbContext>();

        private T Resolve<T>()
        {
            var scope = Provider.CreateScope();
            Scopes.Add(scope);
            return scope.ServiceProvider.GetRequiredService<T>();
        }

        private readonly List<IServiceScope> Scopes = new();

        public async ValueTask DisposeAsync()
        {
            foreach (var scope in Scopes) scope.Dispose();
            await Provider.DisposeAsync();
        }
    }

    private static async Task<StoreFixture> SeedRow(
        string sourceSystem,
        string requestId = "req-1",
        string status = MessageOutboxState.Pending,
        bool leased = false,
        string businessState = MessageBusinessState.Active)
    {
        var fixture = new StoreFixture();
        var db = fixture.NewDb();
        db.MessageOutbox.Add(new MessageOutboxEntity
        {
            SendId = "send-1",
            RequestId = requestId,
            RequestHash = "hash",
            SourceSystem = sourceSystem,
            SubscriberId = "179070",
            WorkflowId = "micip-test-inapp",
            Payload = "{}",
            Status = status,
            BusinessState = businessState,
            LockedBy = leased ? "w1" : null,
            LockedUntil = leased ? DateTime.UtcNow.AddMinutes(5) : null
        });
        await db.SaveChangesAsync();
        return fixture;
    }

    private sealed class StubHandler : HttpMessageHandler
    {
        private readonly Func<object> _respond;
        public StubHandler(Func<object> respond) => _respond = respond;

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
