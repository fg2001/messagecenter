using System.Text.Json;
using MessageCenter.Api.HttpClients;
using MessageCenter.Api.Options;
using MessageCenter.Api.Services;
using Microsoft.Extensions.Options;

namespace MessageCenter.Api.Outbox;

public sealed class MessageOutboxWorker : BackgroundService
{
    private const int FeedPageSize = 100;
    private static readonly JsonSerializerOptions JsonOptions =
        new(JsonSerializerDefaults.Web);

    private readonly IServiceScopeFactory _scopeFactory;
    private readonly MessageWorkerOptions _options;
    private readonly NovuOptions _novuOptions;
    private readonly ILogger<MessageOutboxWorker> _logger;
    private readonly string _worker =
        $"{Environment.MachineName}-{Environment.ProcessId}-{Guid.NewGuid():N}";

    public MessageOutboxWorker(
        IServiceScopeFactory scopeFactory,
        IOptions<MessageWorkerOptions> options,
        IOptions<NovuOptions> novuOptions,
        ILogger<MessageOutboxWorker> logger)
    {
        _scopeFactory = scopeFactory;
        _options = options.Value;
        _novuOptions = novuOptions.Value;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Message outbox worker started. Worker={Worker}", _worker);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var items = await ClaimAsync(stoppingToken);
                if (items.Count == 0)
                {
                    await Task.Delay(
                        TimeSpan.FromMilliseconds(_options.OutboxPollingIntervalMilliseconds),
                        stoppingToken);
                    continue;
                }

                await Task.WhenAll(items.Select(item =>
                    ProcessAsync(item, stoppingToken)));
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Message outbox worker iteration failed.");
                await Task.Delay(TimeSpan.FromSeconds(2), stoppingToken);
            }
        }
    }

    private async Task<IReadOnlyList<ClaimedMessageOutboxItem>> ClaimAsync(
        CancellationToken ct)
    {
        await using var scope = _scopeFactory.CreateAsyncScope();
        return await scope.ServiceProvider
            .GetRequiredService<MessageOutboxStore>()
            .ClaimDueAsync(
                _worker,
                _options.OutboxBatchSize,
                TimeSpan.FromSeconds(_options.OutboxLeaseSeconds),
                ct);
    }

    private async Task ProcessAsync(
        ClaimedMessageOutboxItem item,
        CancellationToken ct)
    {
        await using var scope = _scopeFactory.CreateAsyncScope();
        var services = scope.ServiceProvider;
        var store = services.GetRequiredService<MessageOutboxStore>();

        if (item.Status == MessageOutboxState.Pending)
        {
            await SendAsync(item, services, store, ct);
            return;
        }

        await CompleteProjectionAsync(item, services, store, ct);
    }

    private async Task SendAsync(
        ClaimedMessageOutboxItem item,
        IServiceProvider services,
        MessageOutboxStore store,
        CancellationToken ct)
    {
        try
        {
            var payload = DeserializePayload(item);
            var novu = services.GetRequiredService<NovuClient>();
            if (!await store.MarkTriggerAttemptAsync(item.SendId, _worker, ct))
            {
                // 受理与认领之间套用了业务终态（撤回/过期/消费先行到达）：抑制本次触发，保留受理审计。
                _logger.LogInformation(
                    "Message suppressed by business terminal state. SendId={SendId} BusinessState={BusinessState}",
                    item.SendId,
                    item.BusinessState);
                return;
            }
            var result = await novu.TriggerAsync(
                item.WorkflowId,
                item.SubscriberId,
                payload.Payload,
                item.SendId,
                ct);

            if (!result.Acknowledged ||
                !string.Equals(result.Status, "processed", StringComparison.OrdinalIgnoreCase) ||
                string.IsNullOrWhiteSpace(result.TransactionId))
            {
                throw new InvalidOperationException(
                    $"Novu did not acknowledge send '{item.SendId}'. Status={result.Status ?? "<null>"}.");
            }

            await store.MarkAcceptedAsync(item.SendId, _worker, result, ct);
            _logger.LogInformation(
                "Message accepted by Novu. SendId={SendId} SubscriberId={SubscriberId} TransactionId={TransactionId}",
                item.SendId,
                item.SubscriberId,
                result.TransactionId);
        }
        // 严格来源：触发结果不确定（请求可能已被 Novu 受理）→ 冻结 uncertain，不自动重发同键请求，
        // 等待原操作查询或人工对账（POST /send/{requestId}/resolve-trigger）收敛。
        catch (NovuTriggerUncertainException ex) when (
            !ct.IsCancellationRequested && _novuOptions.IsStrictTriggerSource(item.SourceSystem))
        {
            await store.MarkTriggerUncertainAsync(item.SendId, _worker, ex.Message, ct);
            _logger.LogError(
                ex,
                "Trigger outcome uncertain. SendId={SendId} frozen as uncertain; manual reconciliation required.",
                item.SendId);
        }
        catch (NovuTriggerReceiptMismatchException ex) when (
            !ct.IsCancellationRequested && _novuOptions.IsStrictTriggerSource(item.SourceSystem))
        {
            await store.MarkTriggerUncertainAsync(item.SendId, _worker, ex.Message, ct);
            _logger.LogError(
                ex,
                "Trigger receipt mismatch on strict source. SendId={SendId} frozen as uncertain.",
                item.SendId);
        }
        catch (Exception ex) when (
            !ct.IsCancellationRequested && _novuOptions.IsStrictTriggerSource(item.SourceSystem))
        {
            // 包括成功响应解析/入账失败：可能已到达Novu，不能退回pending自动重发。
            await store.MarkTriggerUncertainAsync(item.SendId, _worker, ex.Message, ct);
            _logger.LogError(ex, "Strict trigger remains uncertain. SendId={SendId}", item.SendId);
        }
        catch (Exception ex) when (!ct.IsCancellationRequested)
        {
            var retry = item.RetryCount + 1;
            var delay = TimeSpan.FromSeconds(Math.Min(300, Math.Pow(2, Math.Min(retry, 8))));
            await store.MarkSendFailureAsync(
                item.SendId,
                _worker,
                _options.OutboxMaxRetryCount,
                delay,
                ex.Message,
                ct);
            _logger.LogError(
                ex,
                "Message send failed. SendId={SendId} Retry={Retry}/{MaxRetry}",
                item.SendId,
                retry,
                _options.OutboxMaxRetryCount);
        }
    }

    private async Task CompleteProjectionAsync(
        ClaimedMessageOutboxItem item,
        IServiceProvider services,
        MessageOutboxStore store,
        CancellationToken ct)
    {
        try
        {
            var novu = services.GetRequiredService<NovuClient>();
            var message = await FindMessageAsync(novu, item, ct);
            if (message is null)
            {
                await store.ScheduleAcceptedCheckAsync(
                    item.SendId,
                    _worker,
                    DateTime.UtcNow.AddSeconds(_options.OutboxVisibilityRetrySeconds),
                    "Novu accepted the trigger, but the in-app message is not visible yet.",
                    ct);
                return;
            }

            // 可见性补投前的终态拦截（T-INT-01 §6）：终态在本行 accepted 与可见之间到达时，
            // 不得把消息投进收件箱——按未送达处理：删除 Novu 侧消息并抑制本行。
            if (await store.TrySuppressTerminalAsync(item.SendId, _worker, ct))
            {
                if (!string.IsNullOrEmpty(message.Id))
                {
                    try
                    {
                        await novu.DeleteMessageAsync(message.Id, ct);
                        var projectionNow = services.GetRequiredService<MessageProjectionStore>();
                        await projectionNow.RemoveAsync(item.SubscriberId, message.Id);
                    }
                    catch (Exception ex) when (!ct.IsCancellationRequested)
                    {
                        _logger.LogWarning(
                            ex,
                            "Terminal message delete failed during visibility pass (row already suppressed). " +
                            "SendId={SendId} MessageId={MessageId}",
                            item.SendId,
                            message.Id);
                    }
                }

                _logger.LogInformation(
                    "Message suppressed by business terminal state before visibility. " +
                    "SendId={SendId} BusinessState={BusinessState}",
                    item.SendId,
                    item.BusinessState);
                return;
            }

            var projection = services.GetRequiredService<MessageProjectionStore>();
            if (!await projection.IsReadyAsync(item.SubscriberId))
            {
                await services.GetRequiredService<MessageWorkQueue>()
                    .EnqueueRebuildAsync(item.SubscriberId);
                await store.ScheduleAcceptedCheckAsync(
                    item.SendId,
                    _worker,
                    DateTime.UtcNow.AddSeconds(_options.OutboxVisibilityRetrySeconds),
                    "The message exists in Novu, but its Redis projection is still building.",
                    ct);
                return;
            }

            await projection.UpsertAsync(item.SubscriberId, new[] { message });
            await projection.RegisterActiveAsync(item.SubscriberId);
            await store.MarkVisibleAsync(item.SendId, _worker, ct);
            _logger.LogInformation(
                "Message is visible. SendId={SendId} SubscriberId={SubscriberId} MessageId={MessageId}",
                item.SendId,
                item.SubscriberId,
                message.Id);
        }
        catch (Exception ex) when (!ct.IsCancellationRequested)
        {
            await store.ScheduleAcceptedCheckAsync(
                item.SendId,
                _worker,
                DateTime.UtcNow.AddSeconds(_options.OutboxVisibilityRetrySeconds),
                ex.Message,
                ct);
            _logger.LogWarning(
                ex,
                "Accepted message visibility check failed. SendId={SendId}",
                item.SendId);
        }
    }

    private async Task<HttpClients.Dtos.NovuMessageItem?> FindMessageAsync(
        NovuClient novu,
        ClaimedMessageOutboxItem item,
        CancellationToken ct)
    {
        for (var page = 0; page < _options.OutboxVisibilityPages; page++)
        {
            var feed = await novu.GetFeedAsync(page, FeedPageSize, item.SubscriberId, ct);
            var message = feed.Data.FirstOrDefault(x =>
                string.Equals(x.TransactionId, item.NovuTransactionId, StringComparison.Ordinal) ||
                string.Equals(x.TransactionId, item.SendId, StringComparison.Ordinal) ||
                string.Equals(x.Payload?.SendId, item.SendId, StringComparison.Ordinal));
            if (message is not null) return message;
            if (!feed.HasMore || feed.Data.Count == 0) break;
        }

        return null;
    }

    private static NovuTriggerPayload DeserializePayload(ClaimedMessageOutboxItem item)
        => JsonSerializer.Deserialize<NovuTriggerPayload>(item.Payload, JsonOptions)
           ?? throw new InvalidOperationException(
               $"Outbox payload for send '{item.SendId}' is invalid.");
}
