using System.Text.Json;
using MessageCenter.Api.Models;
using MessageCenter.Api.Options;
using Microsoft.Extensions.Options;
using StackExchange.Redis;

namespace MessageCenter.Api.Services;

public sealed record MessageWorkItem(
    string Id,
    string Type,
    string SubscriberId,
    int Attempt = 0,
    long NotBeforeUnixMilliseconds = 0);

public sealed record ClaimedMessageWorkItem(
    RedisValue StreamId,
    MessageWorkItem Item);

public sealed record MessageWorkerHeartbeat(
    string Worker,
    DateTimeOffset At);

public sealed class MessageWorkQueue
{
    public const string Rebuild = "rebuild";
    public const string Sync = "sync";
    public const string ReadAll = "read-all";

    private const string GroupName = "message-center-workers";
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
                existingStatus.State is "queued" or "running" or "retrying")
            {
                return new ReadAllAcceptedResponse(existing!, existingStatus.State);
            }
            await DeleteIfValueMatchesAsync(activeKey, existing!);
        }

        var job = new MessageWorkItem(Guid.NewGuid().ToString("N"), ReadAll, subscriberId);
        var status = new ReadAllJobResponse(job.Id, "queued", 0, 0, null);
        const string script = @"
            if redis.call('SET', KEYS[1], ARGV[1], 'EX', ARGV[5], 'NX') == false then
                return 0
            end
            redis.call('SET', KEYS[2], ARGV[2], 'EX', ARGV[5])
            redis.call('SET', KEYS[3], ARGV[3], 'EX', ARGV[5])
            redis.call('XADD', KEYS[4], '*', 'payload', ARGV[4])
            return 1";
        var created = await _redis.ScriptEvaluateAsync(
            script,
            new RedisKey[]
            {
                activeKey,
                StatusKey(job.Id),
                OwnerKey(job.Id),
                StreamKey
            },
            new RedisValue[]
            {
                job.Id,
                JsonSerializer.Serialize(status, JsonOptions),
                subscriberId,
                JsonSerializer.Serialize(job, JsonOptions),
                (long)TimeSpan.FromDays(1).TotalSeconds
            });
        if ((long)created == 0)
        {
            existing = await _redis.StringGetAsync(activeKey);
            var existingStatus = existing.HasValue
                ? await GetReadAllStatusAsync(existing!)
                : null;
            if (existingStatus is not null)
            {
                return new ReadAllAcceptedResponse(existing!, existingStatus.State);
            }
            throw new InvalidOperationException(
                $"Read-all job for subscriber '{subscriberId}' could not be created.");
        }
        return new ReadAllAcceptedResponse(job.Id, "queued");
    }

    public async Task EnsureConsumerGroupAsync()
    {
        try
        {
            await _redis.StreamCreateConsumerGroupAsync(
                StreamKey,
                GroupName,
                "0-0",
                createStream: true);
        }
        catch (RedisServerException ex) when (ex.Message.Contains("BUSYGROUP", StringComparison.Ordinal))
        {
            // The group is shared by every worker instance.
        }
    }

    public async Task<ClaimedMessageWorkItem?> ClaimAsync(
        string consumerName,
        TimeSpan pendingIdle)
    {
        var reclaimed = await _redis.StreamAutoClaimAsync(
            StreamKey,
            GroupName,
            consumerName,
            (long)pendingIdle.TotalMilliseconds,
            "0-0",
            count: 1);
        if (reclaimed.ClaimedEntries.Length > 0)
        {
            return Parse(reclaimed.ClaimedEntries[0]);
        }

        var entries = await _redis.StreamReadGroupAsync(
            StreamKey,
            GroupName,
            consumerName,
            ">",
            count: 1);
        return entries.Length == 0 ? null : Parse(entries[0]);
    }

    public async Task AcknowledgeAsync(ClaimedMessageWorkItem claimed)
    {
        const string script = @"
            redis.call('XACK', KEYS[1], ARGV[1], ARGV[2])
            return redis.call('XDEL', KEYS[1], ARGV[2])";
        await _redis.ScriptEvaluateAsync(
            script,
            new RedisKey[] { StreamKey },
            new RedisValue[] { GroupName, claimed.StreamId });
    }

    public async Task<bool> IsCurrentAsync(MessageWorkItem item)
    {
        var key = item.Type == ReadAll
            ? ActiveReadAllKey(item.SubscriberId)
            : DeduplicationKey(item.Type, item.SubscriberId);
        return await _redis.StringGetAsync(key) == item.Id;
    }

    public async Task RenewClaimAsync(
        ClaimedMessageWorkItem claimed,
        string consumerName)
    {
        await _redis.StreamClaimIdsOnlyAsync(
            StreamKey,
            GroupName,
            consumerName,
            minIdleTimeInMs: 0,
            new[] { claimed.StreamId });
    }

    public async Task RetryAsync(
        ClaimedMessageWorkItem claimed,
        TimeSpan delay,
        string error)
    {
        var item = claimed.Item with
        {
            Attempt = claimed.Item.Attempt + 1,
            NotBeforeUnixMilliseconds =
                DateTimeOffset.UtcNow.Add(delay).ToUnixTimeMilliseconds()
        };
        await PushAsync(item);
        await AcknowledgeAsync(claimed);

        if (item.Type == ReadAll)
        {
            var current = await GetReadAllStatusAsync(item.Id);
            await UpdateReadAllStatusAsync(new ReadAllJobResponse(
                item.Id,
                "retrying",
                current?.Total ?? 0,
                current?.Processed ?? 0,
                error));
        }
    }

    public async Task DeferAsync(ClaimedMessageWorkItem claimed, TimeSpan delay)
    {
        var item = claimed.Item with
        {
            NotBeforeUnixMilliseconds =
                DateTimeOffset.UtcNow.Add(delay).ToUnixTimeMilliseconds()
        };
        await PushAsync(item);
        await AcknowledgeAsync(claimed);
    }

    public async Task CompleteAsync(ClaimedMessageWorkItem claimed)
    {
        var item = claimed.Item;

        if (item.Type is Rebuild or Sync)
        {
            await DeleteIfValueMatchesAsync(
                DeduplicationKey(item.Type, item.SubscriberId),
                item.Id);
        }
        else if (item.Type == ReadAll)
        {
            await DeleteIfValueMatchesAsync(ActiveReadAllKey(item.SubscriberId), item.Id);
        }

        await AcknowledgeAsync(claimed);
    }

    public async Task<bool> AcquireSubscriberLockAsync(
        string subscriberId,
        string jobId,
        TimeSpan expiry)
        => await _redis.StringSetAsync(
            SubscriberLockKey(subscriberId),
            jobId,
            expiry,
            When.NotExists);

    public async Task<bool> RenewSubscriberLockAsync(
        string subscriberId,
        string jobId,
        TimeSpan expiry)
    {
        const string script = @"
            if redis.call('GET', KEYS[1]) == ARGV[1] then
                return redis.call('PEXPIRE', KEYS[1], ARGV[2])
            end
            return 0";
        var result = await _redis.ScriptEvaluateAsync(
            script,
            new RedisKey[] { SubscriberLockKey(subscriberId) },
            new RedisValue[] { jobId, (long)expiry.TotalMilliseconds });
        return (long)result != 0;
    }

    public Task ReleaseSubscriberLockAsync(string subscriberId, string jobId)
        => DeleteIfValueMatchesAsync(SubscriberLockKey(subscriberId), jobId);

    public Task SetHeartbeatAsync(string workerName)
        => _redis.StringSetAsync(
            WorkerHeartbeatKey,
            JsonSerializer.Serialize(
                new MessageWorkerHeartbeat(workerName, DateTimeOffset.UtcNow),
                JsonOptions),
            TimeSpan.FromSeconds(30));

    public async Task<MessageWorkerHeartbeat?> GetHeartbeatAsync()
    {
        var value = await _redis.StringGetAsync(WorkerHeartbeatKey);
        return value.HasValue
            ? JsonSerializer.Deserialize<MessageWorkerHeartbeat>(value!, JsonOptions)
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

    private async Task EnqueueDeduplicatedAsync(string type, string subscriberId)
    {
        var job = new MessageWorkItem(Guid.NewGuid().ToString("N"), type, subscriberId);
        var key = DeduplicationKey(type, subscriberId);
        const string script = @"
            if redis.call('SET', KEYS[1], ARGV[1], 'EX', ARGV[3], 'NX') == false then
                return 0
            end
            redis.call('XADD', KEYS[2], '*', 'payload', ARGV[2])
            return 1";
        await _redis.ScriptEvaluateAsync(
            script,
            new RedisKey[] { key, StreamKey },
            new RedisValue[]
            {
                job.Id,
                JsonSerializer.Serialize(job, JsonOptions),
                (long)TimeSpan.FromDays(1).TotalSeconds
            });
    }

    private Task<RedisValue> PushAsync(MessageWorkItem item)
        => _redis.StreamAddAsync(
            StreamKey,
            new[]
            {
                new NameValueEntry("payload", JsonSerializer.Serialize(item, JsonOptions))
            });

    private static ClaimedMessageWorkItem Parse(StreamEntry entry)
    {
        var payload = entry["payload"];
        var item = JsonSerializer.Deserialize<MessageWorkItem>(payload!, JsonOptions)
            ?? throw new InvalidOperationException($"Invalid message job payload at {entry.Id}.");
        return new ClaimedMessageWorkItem(entry.Id, item);
    }

    private async Task DeleteIfValueMatchesAsync(RedisKey key, RedisValue value)
    {
        const string script = @"
            if redis.call('GET', KEYS[1]) == ARGV[1] then
                return redis.call('DEL', KEYS[1])
            end
            return 0";
        await _redis.ScriptEvaluateAsync(
            script,
            new RedisKey[] { key },
            new RedisValue[] { value });
    }

    private RedisKey StreamKey => $"{_prefix}:jobs-stream";
    private RedisKey StatusKey(string jobId) => $"{_prefix}:job:{jobId}";
    private RedisKey OwnerKey(string jobId) => $"{_prefix}:job:{jobId}:owner";
    private RedisKey ActiveReadAllKey(string subscriberId)
        => $"{_prefix}:{{{subscriberId}}}:read-all-active";
    private RedisKey SubscriberLockKey(string subscriberId)
        => $"{_prefix}:{{{subscriberId}}}:worker-lock";
    private RedisKey DeduplicationKey(string type, string subscriberId)
        => $"{_prefix}:{{{subscriberId}}}:dedupe:{type}";
    private RedisKey WorkerHeartbeatKey => $"{_prefix}:worker:heartbeat";
}
