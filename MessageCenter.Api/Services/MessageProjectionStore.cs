using System.Text.Json;
using MessageCenter.Api.HttpClients.Dtos;
using MessageCenter.Api.Models;
using MessageCenter.Api.Options;
using Microsoft.Extensions.Options;
using StackExchange.Redis;

namespace MessageCenter.Api.Services;

public sealed class MessageProjectionStore
{
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);

    private readonly IDatabase _redis;
    private readonly string _prefix;
    private readonly IServiceScopeFactory _scopes;

    public MessageProjectionStore(IConnectionMultiplexer redis, IOptions<RedisOptions> options, IServiceScopeFactory scopes)
    {
        var value = options.Value;
        _redis = redis.GetDatabase(value.Database);
        _prefix = value.KeyPrefix;
        _scopes = scopes;
    }

    public Task RegisterActiveAsync(string subscriberId)
        => _redis.SortedSetAddAsync(ActiveSubscribersKey, subscriberId, UnixNow());

    public async Task<string?> TakeDueActiveSubscriberAsync(TimeSpan interval)
    {
        var maximum = UnixNow() - interval.TotalMilliseconds;
        var due = await _redis.SortedSetRangeByScoreAsync(
            ActiveSubscribersKey,
            stop: maximum,
            take: 1);
        if (due.Length == 0) return null;

        var subscriberId = due[0].ToString();
        await _redis.SortedSetAddAsync(ActiveSubscribersKey, subscriberId, UnixNow());
        return subscriberId;
    }

    public async Task<bool> IsReadyAsync(string subscriberId)
        => await ReadyStampAsync(subscriberId) is not null;

    private async Task<long> RevisionAsync(string subscriberId)
    {
        await using var scope = _scopes.CreateAsyncScope();
        return await scope.ServiceProvider.GetRequiredService<MessageProjectionAuthority>().RevisionAsync(subscriberId);
    }

    private async Task<string?> ReadyStampAsync(string subscriberId)
    {
        var revision = await RevisionAsync(subscriberId);
        var stamp = await _redis.StringGetAsync(ReadyKey(subscriberId));
        return stamp.HasValue && stamp.ToString().StartsWith("p9:" + revision + ":", StringComparison.Ordinal)
            ? stamp.ToString() : null;
    }

    private async Task<IReadOnlyList<ProjectionMessage>> ActiveAsync(string subscriberId, IReadOnlyList<ProjectionMessage> messages)
    {
        await using var scope = _scopes.CreateAsyncScope();
        return await scope.ServiceProvider.GetRequiredService<MessageProjectionAuthority>().ActiveAsync(subscriberId, messages);
    }

    public async Task BeginRebuildAsync(string subscriberId, string generation)
    {
        var revision = await RevisionAsync(subscriberId);
        await _redis.KeyDeleteAsync(new RedisKey[]
        {
            RebuildMessagesKey(subscriberId, generation),
            RebuildAllKey(subscriberId, generation),
            RebuildUnreadKey(subscriberId, generation),
            RebuildReadKey(subscriberId, generation)
        });
        await _redis.StringSetAsync(
            RebuildRevisionKey(subscriberId, generation), revision, TimeSpan.FromDays(1));
        await _redis.StringSetAsync(
            ActiveRebuildKey(subscriberId),
            generation,
            TimeSpan.FromDays(1));
    }

    public async Task CompleteRebuildAsync(string subscriberId, string generation)
    {
        var revision = await _redis.StringGetAsync(RebuildRevisionKey(subscriberId, generation));
        if (!revision.HasValue || (long)revision != await RevisionAsync(subscriberId))
            throw new InvalidOperationException("Terminal state changed during projection rebuild; retry with a new generation.");
        const string script = @"
            if redis.call('GET', KEYS[1]) ~= ARGV[1] then
                return 0
            end
            for i = 2, 5 do
                redis.call('DEL', KEYS[i])
                if redis.call('EXISTS', KEYS[i + 4]) == 1 then
                    redis.call('RENAME', KEYS[i + 4], KEYS[i])
                end
            end
            redis.call('SET', KEYS[10], ARGV[2])
            redis.call('DEL', KEYS[1])
            redis.call('DEL', KEYS[11])
            return 1";
        var result = await _redis.ScriptEvaluateAsync(
            script,
            new RedisKey[]
            {
                ActiveRebuildKey(subscriberId),
                MessagesKey(subscriberId),
                AllKey(subscriberId),
                UnreadKey(subscriberId),
                ReadKey(subscriberId),
                RebuildMessagesKey(subscriberId, generation),
                RebuildAllKey(subscriberId, generation),
                RebuildUnreadKey(subscriberId, generation),
                RebuildReadKey(subscriberId, generation),
                ReadyKey(subscriberId),
                RebuildRevisionKey(subscriberId, generation)
            },
            new RedisValue[] { generation, "p9:" + revision + ":" + generation });
        if ((long)result == 0)
        {
            throw new InvalidOperationException(
                $"Rebuild generation '{generation}' is no longer active.");
        }
    }

    public async Task AbandonRebuildAsync(string subscriberId, string generation)
    {
        await _redis.KeyDeleteAsync(new RedisKey[]
        {
            RebuildMessagesKey(subscriberId, generation),
            RebuildAllKey(subscriberId, generation),
            RebuildUnreadKey(subscriberId, generation),
            RebuildReadKey(subscriberId, generation),
            RebuildRevisionKey(subscriberId, generation)
        });

        const string script = @"
            if redis.call('GET', KEYS[1]) == ARGV[1] then
                return redis.call('DEL', KEYS[1])
            end
            return 0";
        await _redis.ScriptEvaluateAsync(
            script,
            new RedisKey[] { ActiveRebuildKey(subscriberId) },
            new RedisValue[] { generation });
    }

    public async Task<bool> ContainsAnyAsync(
        string subscriberId,
        IReadOnlyList<string> messageIds)
    {
        if (messageIds.Count == 0) return false;

        var values = await _redis.HashGetAsync(
            MessagesKey(subscriberId),
            messageIds.Select(id => (RedisValue)id).ToArray());
        return values.Any(value => value.HasValue);
    }

    public async Task UpsertAsync(string subscriberId, IEnumerable<NovuMessageItem> messages)
    {
        var stamp = await ReadyStampAsync(subscriberId)
            ?? throw new InvalidOperationException("Projection is rebuilding or its terminal version is stale.");
        await UpsertIntoAsync(subscriberId, MessagesKey(subscriberId), AllKey(subscriberId),
            UnreadKey(subscriberId), ReadKey(subscriberId), ReadyKey(subscriberId), stamp, messages);
    }

    public async Task UpsertRebuildAsync(string subscriberId, string generation, IEnumerable<NovuMessageItem> messages)
        => await UpsertIntoAsync(subscriberId, RebuildMessagesKey(subscriberId, generation),
            RebuildAllKey(subscriberId, generation), RebuildUnreadKey(subscriberId, generation),
            RebuildReadKey(subscriberId, generation), ActiveRebuildKey(subscriberId), generation, messages);

    private async Task UpsertIntoAsync(string subscriberId, RedisKey messagesKey, RedisKey allKey,
        RedisKey unreadKey, RedisKey readKey, RedisKey fenceKey, string stamp, IEnumerable<NovuMessageItem> messages)
    {
        var candidates = messages.Where(x => !string.IsNullOrWhiteSpace(x.Id))
            .Select(x => new ProjectionMessage(Map(x), x.Payload?.SendId, x.TransactionId)).ToList();
        var active = await ActiveAsync(subscriberId, candidates);
        // 与代际检查一起写入：旧同步请求不能在新代际切换后重新导入旧消息。
        const string script = @"
            if redis.call('GET', KEYS[5]) ~= ARGV[1] then return 0 end
            redis.call('HSET', KEYS[1], ARGV[2], ARGV[3])
            redis.call('ZADD', KEYS[2], ARGV[4], ARGV[2])
            if ARGV[5] == '1' then
                redis.call('ZADD', KEYS[4], ARGV[4], ARGV[2])
                redis.call('ZREM', KEYS[3], ARGV[2])
            else
                redis.call('ZADD', KEYS[3], ARGV[4], ARGV[2])
                redis.call('ZREM', KEYS[4], ARGV[2])
            end
            return 1";
        var batch = _redis.CreateBatch();
        var tasks = new List<Task<RedisResult>>();
        foreach (var message in active)
        {
            var item = message.Item;
            tasks.Add(batch.ScriptEvaluateAsync(script,
                new RedisKey[] { messagesKey, allKey, unreadKey, readKey, fenceKey },
                new RedisValue[] { stamp, item.MessageId, JsonSerializer.Serialize(message, JsonOptions),
                    new DateTimeOffset(item.CreatedAt ?? DateTime.UnixEpoch).ToUnixTimeMilliseconds(), item.Read ? "1" : "0" }));
        }
        batch.Execute();
        if ((await Task.WhenAll(tasks)).Any(x => (long)x == 0))
            throw new InvalidOperationException("Projection generation changed before import; retry.");
    }

    public async Task<MessagePageResponse?> GetPageAsync(
        string subscriberId,
        string view,
        int page,
        int limit)
    {
        var stamp = await ReadyStampAsync(subscriberId);
        if (stamp is null) return null;

        var indexKey = ViewKey(subscriberId, view);
        var start = (page - 1L) * limit;
        var stop = start + limit - 1;
        var ids = await _redis.SortedSetRangeByRankAsync(
            indexKey,
            start,
            stop,
            Order.Descending);
        var values = ids.Length == 0
            ? Array.Empty<RedisValue>()
            : await _redis.HashGetAsync(MessagesKey(subscriberId), ids);
        var items = values
            .Where(value => value.HasValue)
            .Select(value => JsonSerializer.Deserialize<ProjectionMessage>(value!, JsonOptions))
            .Where(item => item is not null)
            .Cast<ProjectionMessage>()
            .ToList();

        var totalTask = _redis.SortedSetLengthAsync(indexKey);
        var unreadTask = _redis.SortedSetLengthAsync(UnreadKey(subscriberId));
        await Task.WhenAll(totalTask, unreadTask);

        var active = await ActiveAsync(subscriberId, items);
        if (active.Count != items.Count || stamp != await ReadyStampAsync(subscriberId)) return null;
        return new MessagePageResponse(
            "ready",
            active.Select(x => x.Item).ToList(),
            page,
            limit,
            totalTask.Result,
            unreadTask.Result);
    }

    public async Task<MessageSummaryResponse?> GetSummaryAsync(string subscriberId)
    {
        var stamp = await ReadyStampAsync(subscriberId);
        if (stamp is null) return null;

        var totalTask = _redis.SortedSetLengthAsync(AllKey(subscriberId));
        var unreadTask = _redis.SortedSetLengthAsync(UnreadKey(subscriberId));
        await Task.WhenAll(totalTask, unreadTask);
        return stamp != await ReadyStampAsync(subscriberId) ? null
            : new MessageSummaryResponse("ready", totalTask.Result, unreadTask.Result);
    }

    public async Task SetReadStateAsync(string subscriberId, string messageId, bool read)
    {
        var stamp = await ReadyStampAsync(subscriberId);
        if (stamp is not null)
            await SetReadStateInKeysAsync(MessagesKey(subscriberId), AllKey(subscriberId), UnreadKey(subscriberId),
                ReadKey(subscriberId), ReadyKey(subscriberId), stamp, messageId, read);
        var generation = await _redis.StringGetAsync(ActiveRebuildKey(subscriberId));
        if (!generation.HasValue) return;
        await SetReadStateInKeysAsync(RebuildMessagesKey(subscriberId, generation!), RebuildAllKey(subscriberId, generation!),
            RebuildUnreadKey(subscriberId, generation!), RebuildReadKey(subscriberId, generation!),
            ActiveRebuildKey(subscriberId), generation!, messageId, read);
    }

    private async Task SetReadStateInKeysAsync(RedisKey messagesKey, RedisKey allKey, RedisKey unreadKey,
        RedisKey readKey, RedisKey fenceKey, string stamp, string messageId, bool read)
    {
        // 原地修改缓存信封，不丢原发送关联键；代际切换后旧读写不得重新创建条目或计数。
        const string script = @"
            if redis.call('GET', KEYS[5]) ~= ARGV[1] then return 0 end
            local raw = redis.call('HGET', KEYS[1], ARGV[2])
            local score = redis.call('ZSCORE', KEYS[2], ARGV[2])
            if not raw or not score then return 0 end
            local message = cjson.decode(raw)
            message.item.read = ARGV[3] == '1'
            redis.call('HSET', KEYS[1], ARGV[2], cjson.encode(message))
            if ARGV[3] == '1' then
                redis.call('ZADD', KEYS[4], score, ARGV[2])
                redis.call('ZREM', KEYS[3], ARGV[2])
            else
                redis.call('ZADD', KEYS[3], score, ARGV[2])
                redis.call('ZREM', KEYS[4], ARGV[2])
            end
            return 1";
        await _redis.ScriptEvaluateAsync(script, new RedisKey[] { messagesKey, allKey, unreadKey, readKey, fenceKey },
            new RedisValue[] { stamp, messageId, read ? "1" : "0" });
    }

    /// <summary>从收件箱投影移除一条消息（撤回/过期收敛）：主键集合与三个视图同步删除，
    /// 进行中的 rebuild 代际键同样处理，避免重建把终态消息带回。</summary>
    public async Task RemoveAsync(string subscriberId, string messageId)
    {
        await RemoveFromKeysAsync(
            MessagesKey(subscriberId),
            AllKey(subscriberId),
            UnreadKey(subscriberId),
            ReadKey(subscriberId),
            messageId);

        var generation = await _redis.StringGetAsync(ActiveRebuildKey(subscriberId));
        if (!generation.HasValue) return;

        await RemoveFromKeysAsync(
            RebuildMessagesKey(subscriberId, generation!),
            RebuildAllKey(subscriberId, generation!),
            RebuildUnreadKey(subscriberId, generation!),
            RebuildReadKey(subscriberId, generation!),
            messageId);
    }

    private async Task RemoveFromKeysAsync(
        RedisKey messagesKey,
        RedisKey allKey,
        RedisKey unreadKey,
        RedisKey readKey,
        string messageId)
    {
        var batch = _redis.CreateBatch();
        var tasks = new List<Task>
        {
            batch.HashDeleteAsync(messagesKey, messageId),
            batch.SortedSetRemoveAsync(allKey, messageId),
            batch.SortedSetRemoveAsync(unreadKey, messageId),
            batch.SortedSetRemoveAsync(readKey, messageId)
        };
        batch.Execute();
        await Task.WhenAll(tasks);
    }

    public async Task<IReadOnlyList<string>> GetUnreadIdsAsync(
        string subscriberId,
        int limit)
    {
        var stamp = await ReadyStampAsync(subscriberId);
        if (stamp is null) return Array.Empty<string>();
        var ids = await _redis.SortedSetRangeByRankAsync(
            UnreadKey(subscriberId),
            0,
            limit - 1,
            Order.Descending);
        return stamp != await ReadyStampAsync(subscriberId) ? Array.Empty<string>()
            : ids.Select(value => value.ToString()).ToList();
    }

    private RedisKey RebuildRevisionKey(string subscriberId, string generation)
        => $"{_prefix}:{{{subscriberId}}}:rebuild:{generation}:revision";

    private RedisKey ViewKey(string subscriberId, string view)
        => view switch
        {
            "all" => AllKey(subscriberId),
            "unread" => UnreadKey(subscriberId),
            "read" => ReadKey(subscriberId),
            _ => throw new ArgumentOutOfRangeException(nameof(view))
        };

    private static MessageItemResponse Map(NovuMessageItem message)
        => new()
        {
            MessageId = message.Id!,
            SourceSystem = message.Payload?.SourceSystem,
            BusinessType = message.Payload?.BusinessType,
            BusinessId = message.Payload?.BusinessId,
            Title = message.Subject,
            Content = message.Content,
            Url = message.Cta?.Data?.Url,
            Read = message.Read,
            Seen = message.Seen,
            CreatedAt = message.CreatedAt
        };

    private double UnixNow() => DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
    private RedisKey ActiveSubscribersKey => $"{_prefix}:active-subscribers";
    private RedisKey ReadyKey(string subscriberId) => $"{_prefix}:{{{subscriberId}}}:ready";
    private RedisKey MessagesKey(string subscriberId) => $"{_prefix}:{{{subscriberId}}}:messages";
    private RedisKey AllKey(string subscriberId) => $"{_prefix}:{{{subscriberId}}}:all";
    private RedisKey UnreadKey(string subscriberId) => $"{_prefix}:{{{subscriberId}}}:unread";
    private RedisKey ReadKey(string subscriberId) => $"{_prefix}:{{{subscriberId}}}:read";
    private RedisKey ActiveRebuildKey(string subscriberId)
        => $"{_prefix}:{{{subscriberId}}}:rebuild-active";
    private RedisKey RebuildMessagesKey(string subscriberId, string generation)
        => $"{_prefix}:{{{subscriberId}}}:rebuild:{generation}:messages";
    private RedisKey RebuildAllKey(string subscriberId, string generation)
        => $"{_prefix}:{{{subscriberId}}}:rebuild:{generation}:all";
    private RedisKey RebuildUnreadKey(string subscriberId, string generation)
        => $"{_prefix}:{{{subscriberId}}}:rebuild:{generation}:unread";
    private RedisKey RebuildReadKey(string subscriberId, string generation)
        => $"{_prefix}:{{{subscriberId}}}:rebuild:{generation}:read";
}
