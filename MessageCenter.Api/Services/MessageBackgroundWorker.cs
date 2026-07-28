using MessageCenter.Api.HttpClients;
using MessageCenter.Api.Models;

namespace MessageCenter.Api.Services;

public sealed class MessageBackgroundWorker : BackgroundService
{
    private const int FeedPageSize = 100;
    private const int ReadBatchSize = 100;
    private const int ReadConcurrency = 8;

    private readonly IServiceProvider _services;
    private readonly ILogger<MessageBackgroundWorker> _logger;

    public MessageBackgroundWorker(
        IServiceProvider services,
        ILogger<MessageBackgroundWorker> logger)
    {
        _services = services;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                using var scope = _services.CreateScope();
                var queue = scope.ServiceProvider.GetRequiredService<MessageWorkQueue>();
                var item = await queue.DequeueAsync();

                if (item is not null)
                {
                    await ProcessAsync(scope.ServiceProvider, queue, item, stoppingToken);
                    continue;
                }

                var store = scope.ServiceProvider.GetRequiredService<MessageProjectionStore>();
                var activeSubscriber = await store.TakeDueActiveSubscriberAsync(
                    TimeSpan.FromSeconds(2));
                if (activeSubscriber is not null)
                {
                    await SyncRecentAsync(scope.ServiceProvider, activeSubscriber, stoppingToken);
                    continue;
                }

                await Task.Delay(250, stoppingToken);
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
                _logger.LogError(ex, "Message background worker iteration failed.");
                await Task.Delay(TimeSpan.FromSeconds(1), stoppingToken);
            }
        }
    }

    private async Task ProcessAsync(
        IServiceProvider services,
        MessageWorkQueue queue,
        MessageWorkItem item,
        CancellationToken ct)
    {
        try
        {
            switch (item.Type)
            {
                case MessageWorkQueue.Rebuild:
                    await RebuildAsync(services, item.SubscriberId, ct);
                    break;
                case MessageWorkQueue.Sync:
                    await SyncRecentAsync(services, item.SubscriberId, ct);
                    break;
                case MessageWorkQueue.ReadAll:
                    await ReadAllAsync(services, queue, item, ct);
                    break;
                default:
                    throw new InvalidOperationException($"Unknown message job type '{item.Type}'.");
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(
                ex,
                "Message job failed. JobId={JobId} Type={Type} SubscriberId={SubscriberId}",
                item.Id,
                item.Type,
                item.SubscriberId);

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
        }
        finally
        {
            await queue.CompleteAsync(item);
        }
    }

    private static async Task RebuildAsync(
        IServiceProvider services,
        string subscriberId,
        CancellationToken ct)
    {
        var novu = services.GetRequiredService<NovuClient>();
        var store = services.GetRequiredService<MessageProjectionStore>();
        await store.BeginRebuildAsync(subscriberId);

        for (var page = 0; ; page++)
        {
            var feed = await novu.GetFeedAsync(page, FeedPageSize, subscriberId, ct);
            await store.UpsertAsync(subscriberId, feed.Data);
            if (!feed.HasMore || feed.Data.Count == 0) break;
        }

        await store.CompleteRebuildAsync(subscriberId);
    }

    private static async Task SyncRecentAsync(
        IServiceProvider services,
        string subscriberId,
        CancellationToken ct)
    {
        var novu = services.GetRequiredService<NovuClient>();
        var store = services.GetRequiredService<MessageProjectionStore>();
        var queue = services.GetRequiredService<MessageWorkQueue>();

        if (!await store.IsReadyAsync(subscriberId))
        {
            await queue.EnqueueRebuildAsync(subscriberId);
            return;
        }

        for (var page = 0; ; page++)
        {
            var feed = await novu.GetFeedAsync(page, FeedPageSize, subscriberId, ct);
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

    private static async Task ReadAllAsync(
        IServiceProvider services,
        MessageWorkQueue queue,
        MessageWorkItem item,
        CancellationToken ct)
    {
        var novu = services.GetRequiredService<NovuClient>();
        var store = services.GetRequiredService<MessageProjectionStore>();
        var total = (await store.GetSummaryAsync(item.SubscriberId))?.UnreadCount ?? 0;
        long processed = 0;

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
                        // Failed IDs stay unread and are retried by the next batch.
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
}
