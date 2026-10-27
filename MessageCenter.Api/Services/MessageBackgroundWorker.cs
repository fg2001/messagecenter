using System.Net;
using MessageCenter.Api.HttpClients;
using MessageCenter.Api.HttpClients.Dtos;
using MessageCenter.Api.Models;
using MessageCenter.Api.Options;
using Microsoft.Extensions.Options;

namespace MessageCenter.Api.Services;

public sealed class MessageBackgroundWorker : BackgroundService
{
    private const int FeedPageSize = 100;
    private const int ReadBatchSize = 100;
    private const int ReadConcurrency = 8;

    private readonly IServiceProvider _services;
    private readonly MessageWorkerOptions _options;
    private readonly ILogger<MessageBackgroundWorker> _logger;
    private readonly string _consumerName =
        $"{Environment.MachineName}-{Environment.ProcessId}-{Guid.NewGuid():N}";

    public MessageBackgroundWorker(
        IServiceProvider services,
        IOptions<MessageWorkerOptions> options,
        ILogger<MessageBackgroundWorker> logger)
    {
        _services = services;
        _options = options.Value;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using (var scope = _services.CreateScope())
        {
            await scope.ServiceProvider
                .GetRequiredService<MessageWorkQueue>()
                .EnsureConsumerGroupAsync();
        }

        _logger.LogInformation(
            "Message worker started. ConsumerName={ConsumerName}",
            _consumerName);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                using var scope = _services.CreateScope();
                var queue = scope.ServiceProvider.GetRequiredService<MessageWorkQueue>();
                await queue.SetHeartbeatAsync(_consumerName);

                var store = scope.ServiceProvider.GetRequiredService<MessageProjectionStore>();
                var activeSubscriber = await store.TakeDueActiveSubscriberAsync(
                    TimeSpan.FromSeconds(_options.ActiveSyncIntervalSeconds));
                if (activeSubscriber is not null)
                {
                    await queue.EnqueueSyncAsync(activeSubscriber);
                }

                var claimed = await queue.ClaimAsync(
                    _consumerName,
                    TimeSpan.FromSeconds(_options.PendingIdleSeconds));
                if (claimed is null)
                {
                    await Task.Delay(250, stoppingToken);
                    continue;
                }

                var delayMilliseconds =
                    claimed.Item.NotBeforeUnixMilliseconds -
                    DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                if (delayMilliseconds > 0)
                {
                    await queue.DeferAsync(
                        claimed,
                        TimeSpan.FromMilliseconds(delayMilliseconds));
                    await Task.Delay(
                        TimeSpan.FromMilliseconds(Math.Min(delayMilliseconds, 1000)),
                        stoppingToken);
                    continue;
                }

                await ProcessClaimedAsync(
                    scope.ServiceProvider,
                    queue,
                    claimed,
                    stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception) when (stoppingToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Message worker iteration failed.");
                await Task.Delay(TimeSpan.FromSeconds(1), stoppingToken);
            }
        }
    }

    private async Task ProcessClaimedAsync(
        IServiceProvider services,
        MessageWorkQueue queue,
        ClaimedMessageWorkItem claimed,
        CancellationToken stoppingToken)
    {
        var item = claimed.Item;
        if (!await queue.IsCurrentAsync(item))
        {
            await queue.AcknowledgeAsync(claimed);
            return;
        }

        var lockExpiry = TimeSpan.FromSeconds(_options.LockSeconds);
        if (!await queue.AcquireSubscriberLockAsync(
                item.SubscriberId,
                item.Id,
                lockExpiry))
        {
            await Task.Delay(TimeSpan.FromSeconds(1), stoppingToken);
            return;
        }

        using var processingCts =
            CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
        var renewal = RenewLeaseAsync(
            queue,
            claimed,
            item,
            lockExpiry,
            processingCts);

        try
        {
            await ProcessAsync(services, queue, item, processingCts.Token);
            await queue.CompleteAsync(claimed);
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogError(
                ex,
                "Message job failed. JobId={JobId} Type={Type} SubscriberId={SubscriberId} Attempt={Attempt}",
                item.Id,
                item.Type,
                item.SubscriberId,
                item.Attempt + 1);

            if (item.Attempt + 1 < _options.JobRetryCount)
            {
                await queue.RetryAsync(
                    claimed,
                    RetryDelay(item.Attempt),
                    ex.Message);
            }
            else
            {
                if (item.Type == MessageWorkQueue.ReadAll)
                {
                    var current = await queue.GetReadAllStatusAsync(item.Id);
                    await queue.UpdateReadAllStatusAsync(new ReadAllJobResponse(
                        item.Id,
                        "failed",
                        current?.Total ?? 0,
                        current?.Processed ?? 0,
                        ex.Message));
                }

                await queue.CompleteAsync(claimed);
            }
        }
        finally
        {
            processingCts.Cancel();
            try
            {
                await renewal;
            }
            catch (OperationCanceledException)
            {
                // Normal completion stops lease renewal.
            }
            await queue.ReleaseSubscriberLockAsync(item.SubscriberId, item.Id);
        }
    }

    private async Task ProcessAsync(
        IServiceProvider services,
        MessageWorkQueue queue,
        MessageWorkItem item,
        CancellationToken ct)
    {
        switch (item.Type)
        {
            case MessageWorkQueue.Rebuild:
                await RebuildAsync(services, item, ct);
                break;
            case MessageWorkQueue.Sync:
                await SyncRecentAsync(services, queue, item.SubscriberId, ct);
                break;
            case MessageWorkQueue.ReadAll:
                await ReadAllAsync(services, queue, item, ct);
                break;
            default:
                throw new InvalidOperationException($"Unknown message job type '{item.Type}'.");
        }
    }

    private async Task RebuildAsync(
        IServiceProvider services,
        MessageWorkItem item,
        CancellationToken ct)
    {
        var novu = services.GetRequiredService<NovuClient>();
        var store = services.GetRequiredService<MessageProjectionStore>();
        // 每次尝试使用新代际，旧租约持有者不能向同一重试代际补写过时内容。
        var generation = Guid.NewGuid().ToString("N");
        await store.BeginRebuildAsync(item.SubscriberId, generation);
        try
        {
            for (var page = 0; ; page++)
            {
                var feed = await GetFeedWithRetryAsync(novu, page, item.SubscriberId, ct);
                await store.UpsertRebuildAsync(item.SubscriberId, generation, feed.Data);
                if (!feed.HasMore || feed.Data.Count == 0) break;
            }
            await store.CompleteRebuildAsync(item.SubscriberId, generation);
        }
        catch
        {
            await store.AbandonRebuildAsync(item.SubscriberId, generation);
            throw;
        }
    }

    private async Task SyncRecentAsync(
        IServiceProvider services,
        MessageWorkQueue queue,
        string subscriberId,
        CancellationToken ct)
    {
        var novu = services.GetRequiredService<NovuClient>();
        var store = services.GetRequiredService<MessageProjectionStore>();

        if (!await store.IsReadyAsync(subscriberId))
        {
            await queue.EnqueueRebuildAsync(subscriberId);
            return;
        }

        for (var page = 0; ; page++)
        {
            var feed = await GetFeedWithRetryAsync(novu, page, subscriberId, ct);
            var messageIds = feed.Data
                .Select(message => message.Id)
                .Where(id => !string.IsNullOrWhiteSpace(id))
                .Cast<string>()
                .ToList();
            var reachedKnownMessage = await store.ContainsAnyAsync(
                subscriberId,
                messageIds);

            await store.UpsertAsync(subscriberId, feed.Data);
            if (reachedKnownMessage || !feed.HasMore || feed.Data.Count == 0) break;
        }
    }

    private async Task<FeedResult> GetFeedWithRetryAsync(
        NovuClient novu,
        int page,
        string subscriberId,
        CancellationToken ct)
    {
        for (var attempt = 0; ; attempt++)
        {
            try
            {
                return await novu.GetFeedAsync(page, FeedPageSize, subscriberId, ct);
            }
            catch (Exception ex) when (
                IsTransient(ex, ct) &&
                attempt + 1 < _options.PageRetryCount)
            {
                var delay = RetryDelay(attempt);
                _logger.LogWarning(
                    ex,
                    "Novu page request will retry. SubscriberId={SubscriberId} Page={Page} Attempt={Attempt} Delay={Delay}",
                    subscriberId,
                    page,
                    attempt + 1,
                    delay);
                await Task.Delay(delay, ct);
            }
        }
    }

    private static bool IsTransient(Exception ex, CancellationToken ct)
        => ex switch
        {
            TaskCanceledException when !ct.IsCancellationRequested => true,
            HttpRequestException http when
                !http.StatusCode.HasValue ||
                http.StatusCode == HttpStatusCode.RequestTimeout ||
                http.StatusCode == HttpStatusCode.TooManyRequests ||
                (int)http.StatusCode.Value >= 500 => true,
            _ => false
        };

    private async Task ReadAllAsync(
        IServiceProvider services,
        MessageWorkQueue queue,
        MessageWorkItem item,
        CancellationToken ct)
    {
        var novu = services.GetRequiredService<NovuClient>();
        var store = services.GetRequiredService<MessageProjectionStore>();
        var current = await queue.GetReadAllStatusAsync(item.Id);
        var total = current?.Total > 0
            ? current.Total
            : (await store.GetSummaryAsync(item.SubscriberId))?.UnreadCount ?? 0;
        var processed = current?.Processed ?? 0;

        await queue.UpdateReadAllStatusAsync(
            new ReadAllJobResponse(item.Id, "running", total, processed, null));

        while (true)
        {
            var ids = await store.GetUnreadIdsAsync(item.SubscriberId, ReadBatchSize);
            if (ids.Count == 0) break;

            using var gate = new SemaphoreSlim(ReadConcurrency);
            var completed = new List<string>();
            var completedLock = new object();
            await Task.WhenAll(ids.Select(async messageId =>
            {
                await gate.WaitAsync(ct);
                try
                {
                    try
                    {
                        await novu.MarkAsAsync(item.SubscriberId, messageId, read: true, ct);
                        lock (completedLock) completed.Add(messageId);
                    }
                    catch when (!ct.IsCancellationRequested)
                    {
                        // Failed IDs remain unread and the job retry will process them again.
                    }
                }
                finally
                {
                    gate.Release();
                }
            }));

            if (completed.Count == 0)
            {
                throw new InvalidOperationException("Read-all job made no progress.");
            }

            await Task.WhenAll(completed.Select(messageId =>
                store.SetReadStateAsync(item.SubscriberId, messageId, read: true)));

            processed += completed.Count;
            await queue.UpdateReadAllStatusAsync(
                new ReadAllJobResponse(item.Id, "running", total, processed, null));
        }

        await queue.UpdateReadAllStatusAsync(
            new ReadAllJobResponse(item.Id, "completed", total, processed, null));
    }

    private async Task RenewLeaseAsync(
        MessageWorkQueue queue,
        ClaimedMessageWorkItem claimed,
        MessageWorkItem item,
        TimeSpan lockExpiry,
        CancellationTokenSource processingCts)
    {
        var interval = TimeSpan.FromMilliseconds(
            Math.Max(1000, lockExpiry.TotalMilliseconds / 3));
        try
        {
            while (true)
            {
                await Task.Delay(interval, processingCts.Token);
                if (!await queue.RenewSubscriberLockAsync(
                        item.SubscriberId,
                        item.Id,
                        lockExpiry))
                {
                    _logger.LogError(
                        "Subscriber lock was lost. JobId={JobId} SubscriberId={SubscriberId}",
                        item.Id,
                        item.SubscriberId);
                    processingCts.Cancel();
                    return;
                }
                await queue.RenewClaimAsync(claimed, _consumerName);
                await queue.SetHeartbeatAsync(_consumerName);
            }
        }
        catch (OperationCanceledException) when (processingCts.IsCancellationRequested)
        {
            // Normal completion or lock loss stops renewal.
        }
        catch (Exception ex)
        {
            _logger.LogError(
                ex,
                "Subscriber lock renewal failed. JobId={JobId} SubscriberId={SubscriberId}",
                item.Id,
                item.SubscriberId);
            processingCts.Cancel();
        }
    }

    private TimeSpan RetryDelay(int attempt)
    {
        var seconds = Math.Min(
            30,
            _options.RetryBaseDelaySeconds * Math.Pow(2, attempt));
        return TimeSpan.FromMilliseconds(
            seconds * 1000 + Random.Shared.Next(100, 500));
    }
}
