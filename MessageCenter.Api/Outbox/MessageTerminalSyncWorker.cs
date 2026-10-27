using MessageCenter.Api.HttpClients;
using System.Text.Json;
using MessageCenter.Api.Options;
using MessageCenter.Api.Services;
using Microsoft.Extensions.Options;

namespace MessageCenter.Api.Outbox;

public sealed class MessageTerminalSyncWorker : BackgroundService
{
    private readonly IServiceScopeFactory _scopes;
    private readonly MessageWorkerOptions _options;
    private readonly ILogger<MessageTerminalSyncWorker> _logger;
    private readonly string _owner = $"terminal-{Environment.ProcessId}-{Guid.NewGuid():N}";

    public MessageTerminalSyncWorker(IServiceScopeFactory scopes, IOptions<MessageWorkerOptions> options,
        ILogger<MessageTerminalSyncWorker> logger)
    {
        _scopes = scopes; _options = options.Value; _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            try
            {
                await using var scope = _scopes.CreateAsyncScope();
                var items = await scope.ServiceProvider.GetRequiredService<MessageTerminalSyncStore>()
                    .ClaimAsync(_owner, _options.OutboxBatchSize,
                        TimeSpan.FromSeconds(_options.OutboxLeaseSeconds), ct);
                await Task.WhenAll(items.Select(x => ProcessAsync(x, ct)));
                if (items.Count == 0)
                    await Task.Delay(_options.OutboxPollingIntervalMilliseconds, ct);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested) { return; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Terminal convergence iteration failed.");
                await Task.Delay(TimeSpan.FromSeconds(2), ct);
            }
        }
    }

    private async Task ProcessAsync(MessageTerminalSyncEntity item, CancellationToken ct)
    {
        await using var scope = _scopes.CreateAsyncScope();
        var services = scope.ServiceProvider;
        var store = services.GetRequiredService<MessageTerminalSyncStore>();
        try
        {
            var row = await store.RecipientAsync(item.SendId, ct);
            if (row is null) throw new InvalidOperationException("TERMINAL_RECIPIENT_MISSING");
            if (row.BusinessVersion != item.BusinessVersion || !MessageBusinessState.IsTerminal(row.BusinessState))
            {
                await store.FinishAsync(item, "superseded", null, null, false, ct);
                return;
            }
            // 仅受理时即抑制的新行可证明未触发；不能用历史默认零计数断言从未发送。
            if (item.NeverTriggered && row.Status == MessageOutboxState.Suppressed && row.TriggerAttempts == 0
                && row.NovuTransactionId is null && row.AcceptedAt is null && row.VisibleAt is null)
            {
                await store.FinishAsync(item, "not-required", null, null, true, ct);
                return;
            }
            var novu = services.GetRequiredService<NovuClient>();
            var result = await FindTargetsAsync(novu, row, _options, ct, item.NextPage);
            var ids = (item.MessageIds is null ? Array.Empty<string>()
                : JsonSerializer.Deserialize<string[]>(item.MessageIds) ?? throw new InvalidOperationException("TERMINAL_TARGETS_INVALID"))
                .Concat(result.MessageIds).Distinct(StringComparer.Ordinal).ToList();
            if (!await store.RecordTargetsAsync(item, JsonSerializer.Serialize(ids), result.NextPage, ct)) return;
            var projection = services.GetRequiredService<MessageProjectionStore>();
            foreach (var id in ids)
            {
                await ApplyTargetAsync(novu, row, id, ct);
                await projection.RemoveAsync(row.SubscriberId, id);
            }
            var confirmed = ids.Count > 0 && result.Exhausted;
            await store.FinishAsync(item, confirmed ? "watching" : "pending",
                DateTime.UtcNow.AddSeconds(confirmed ? _options.ActiveSyncIntervalSeconds : _options.OutboxVisibilityRetrySeconds),
                confirmed ? null : result.Exhausted ? "TERMINAL_MESSAGE_NOT_VISIBLE" : "TERMINAL_FEED_SCAN_INCOMPLETE",
                confirmed, ct);
        }
        catch (Exception ex) when (!ct.IsCancellationRequested)
        {
            // 同步失败不撤销业务终态；安全错误码持久保存，原键重试不会再次trigger。
            await store.FinishAsync(item, "pending",
                DateTime.UtcNow.AddSeconds(Math.Min(300, Math.Pow(2, Math.Min(item.Attempts, 8)))),
                "TERMINAL_SYNC_" + ex.GetType().Name, false, ct);
            _logger.LogWarning(ex, "Terminal convergence pending. SendId={SendId} Version={Version}",
                item.SendId, item.BusinessVersion);
        }
    }

    /// <summary>固定收件人的消息只按非空事务键或sendId匹配；查无结果不能证明永远不会迟到。</summary>
    public static async Task<(IReadOnlyList<string> MessageIds, bool Exhausted, int NextPage)> FindTargetsAsync(
        NovuClient novu, MessageOutboxEntity row, MessageWorkerOptions options, CancellationToken ct, int firstPage = 0)
    {
        var ids = new HashSet<string>(StringComparer.Ordinal);
        var exhausted = false;
        var nextPage = firstPage;
        // 先完成扫描再删除，避免删除导致分页偏移漏项；每次有界巡检，后续重试仍使用原键。
        for (var count = 0; count < options.OutboxVisibilityPages; count++)
        {
            var feed = await novu.GetFeedAsync(nextPage++, 100, row.SubscriberId, ct);
            foreach (var message in feed.Data)
                if (!string.IsNullOrWhiteSpace(message.Id) && (
                    !string.IsNullOrWhiteSpace(row.NovuTransactionId) && message.TransactionId == row.NovuTransactionId
                    || message.TransactionId == row.SendId || message.Payload?.SendId == row.SendId))
                    ids.Add(message.Id);
            if (!feed.HasMore || feed.Data.Count == 0) { exhausted = !feed.HasMore; break; }
        }
        return (ids.ToList(), exhausted, exhausted ? 0 : nextPage);
    }

    public static Task ApplyTargetAsync(NovuClient novu, MessageOutboxEntity row, string id, CancellationToken ct)
        => row.BusinessState == MessageBusinessState.Consumed
            ? novu.MarkAsAsync(row.SubscriberId, id, true, ct)
            : novu.DeleteMessageAsync(id, ct);
}
