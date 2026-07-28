using System.Text.Json;
using MessageCenter.Api.Models;
using MessageCenter.Api.Options;
using Microsoft.Extensions.Options;
using StackExchange.Redis;

namespace MessageCenter.Api.Services;

public sealed record MessageWorkItem(
    string Id,
    string Type,
    string SubscriberId);

public sealed class MessageWorkQueue
{
    public const string Rebuild = "rebuild";
    public const string Sync = "sync";
    public const string ReadAll = "read-all";

    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);

    private readonly IDatabase _redis;
    private readonly string _prefix;

    public MessageWorkQueue(IConnectionMultiplexer redis, IOptions<RedisOptions> options)
    {
        var value = options.Value;
        _redis = redis.GetDatabase(value.Database);
        _prefix = value.KeyPrefix;
    }

    public Task EnqueueRebuildAsync(string subscriberId)
        => EnqueueDeduplicatedAsync(Rebuild, subscriberId);

    public Task EnqueueSyncAsync(string subscriberId)
        => EnqueueDeduplicatedAsync(Sync, subscriberId);

    public async Task<ReadAllAcceptedResponse> EnqueueReadAllAsync(string subscriberId)
    {
        var activeKey = ActiveReadAllKey(subscriberId);
        var existing = await _redis.StringGetAsync(activeKey);
        if (existing.HasValue)
        {
            var existingStatus = await GetReadAllStatusAsync(existing!);
            if (existingStatus is not null &&
                existingStatus.State is "queued" or "running")
            {
                return new ReadAllAcceptedResponse(existing!, existingStatus.State);
            }
        }

        var job = new MessageWorkItem(Guid.NewGuid().ToString("N"), ReadAll, subscriberId);
        var status = new ReadAllJobResponse(job.Id, "queued", 0, 0, null);
        await _redis.StringSetAsync(activeKey, job.Id, TimeSpan.FromDays(1));
        await _redis.StringSetAsync(
            StatusKey(job.Id),
            JsonSerializer.Serialize(status, JsonOptions),
            TimeSpan.FromDays(1));
        await _redis.StringSetAsync(
            OwnerKey(job.Id),
            subscriberId,
            TimeSpan.FromDays(1));
        await PushAsync(job);
        return new ReadAllAcceptedResponse(job.Id, "queued");
    }

    public async Task<MessageWorkItem?> DequeueAsync()
    {
        var value = await _redis.ListRightPopAsync(QueueKey);
        return value.HasValue
            ? JsonSerializer.Deserialize<MessageWorkItem>(value!, JsonOptions)
            : null;
    }

    public Task UpdateReadAllStatusAsync(ReadAllJobResponse status)
        => _redis.StringSetAsync(
            StatusKey(status.JobId),
            JsonSerializer.Serialize(status, JsonOptions),
            TimeSpan.FromDays(1));

    public async Task<ReadAllJobResponse?> GetReadAllStatusAsync(string jobId)
    {
        var value = await _redis.StringGetAsync(StatusKey(jobId));
        return value.HasValue
            ? JsonSerializer.Deserialize<ReadAllJobResponse>(value!, JsonOptions)
            : null;
    }

    public async Task<ReadAllJobResponse?> GetReadAllStatusAsync(
        string jobId,
        string subscriberId)
    {
        var owner = await _redis.StringGetAsync(OwnerKey(jobId));
        return owner == subscriberId
            ? await GetReadAllStatusAsync(jobId)
            : null;
    }

    public async Task CompleteAsync(MessageWorkItem item)
    {
        if (item.Type is Rebuild or Sync)
        {
            await _redis.KeyDeleteAsync(DeduplicationKey(item.Type, item.SubscriberId));
        }
        else if (item.Type == ReadAll)
        {
            await _redis.KeyDeleteAsync(ActiveReadAllKey(item.SubscriberId));
        }
    }

    private async Task EnqueueDeduplicatedAsync(string type, string subscriberId)
    {
        var acquired = await _redis.StringSetAsync(
            DeduplicationKey(type, subscriberId),
            "1",
            TimeSpan.FromMinutes(30),
            When.NotExists);
        if (!acquired) return;

        await PushAsync(new MessageWorkItem(
            Guid.NewGuid().ToString("N"),
            type,
            subscriberId));
    }

    private Task PushAsync(MessageWorkItem item)
        => _redis.ListLeftPushAsync(
            QueueKey,
            JsonSerializer.Serialize(item, JsonOptions));

    private RedisKey QueueKey => $"{_prefix}:jobs";
    private RedisKey StatusKey(string jobId) => $"{_prefix}:job:{jobId}";
    private RedisKey OwnerKey(string jobId) => $"{_prefix}:job:{jobId}:owner";
    private RedisKey ActiveReadAllKey(string subscriberId)
        => $"{_prefix}:{{{subscriberId}}}:read-all-active";
    private RedisKey DeduplicationKey(string type, string subscriberId)
        => $"{_prefix}:{{{subscriberId}}}:dedupe:{type}";
}
