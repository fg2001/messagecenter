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

    public MessageProjectionStore(IConnectionMultiplexer redis, IOptions<RedisOptions> options)
    {
        var value = options.Value;
        _redis = redis.GetDatabase(value.Database);
        _prefix = value.KeyPrefix;
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

    public Task<bool> IsReadyAsync(string subscriberId)
        => _redis.KeyExistsAsync(ReadyKey(subscriberId));

    public async Task BeginRebuildAsync(string subscriberId, string generation)
    {
        await _redis.KeyDeleteAsync(new RedisKey[]
        {
            RebuildMessagesKey(subscriberId, generation),
            RebuildAllKey(subscriberId, generation),
            RebuildUnreadKey(subscriberId, generation),
            RebuildReadKey(subscriberId, generation)
        });
        await _redis.StringSetAsync(
            ActiveRebuildKey(subscriberId),
            generation,
            TimeSpan.FromDays(1));
    }

    public async Task CompleteRebuildAsync(string subscriberId, string generation)
    {
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
            redis.call('SET', KEYS[10], 'ready')
            redis.call('DEL', KEYS[1])
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
                ReadyKey(subscriberId)
            },
            new RedisValue[] { generation });
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
            RebuildReadKey(subscriberId, generation)
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
        => await UpsertIntoAsync(
            MessagesKey(subscriberId),
            AllKey(subscriberId),
            UnreadKey(subscriberId),
            ReadKey(subscriberId),
            messages);

    public async Task UpsertRebuildAsync(
        string subscriberId,
        string generation,
        IEnumerable<NovuMessageItem> messages)
        => await UpsertIntoAsync(
            RebuildMessagesKey(subscriberId, generation),
            RebuildAllKey(subscriberId, generation),
            RebuildUnreadKey(subscriberId, generation),
            RebuildReadKey(subscriberId, generation),
            messages);

    private async Task UpsertIntoAsync(
        RedisKey messagesKey,
        RedisKey allKey,
        RedisKey unreadKey,
        RedisKey readKey,
        IEnumerable<NovuMessageItem> messages)
    {
        var batch = _redis.CreateBatch();
        var tasks = new List<Task>();

        foreach (var message in messages)
        {
            if (string.IsNullOrWhiteSpace(message.Id)) continue;

            var item = Map(message);
            var score = new DateTimeOffset(
                message.CreatedAt ?? DateTime.UnixEpoch).ToUnixTimeMilliseconds();
            var id = message.Id;

            tasks.Add(batch.HashSetAsync(
                messagesKey,
                id,
                JsonSerializer.Serialize(item, JsonOptions)));
            tasks.Add(batch.SortedSetAddAsync(allKey, id, score));

            if (message.Read)
            {
                tasks.Add(batch.SortedSetAddAsync(readKey, id, score));
                tasks.Add(batch.SortedSetRemoveAsync(unreadKey, id));
            }
            else
            {
                tasks.Add(batch.SortedSetAddAsync(unreadKey, id, score));
                tasks.Add(batch.SortedSetRemoveAsync(readKey, id));
            }
        }

        batch.Execute();
        await Task.WhenAll(tasks);
    }

    public async Task<MessagePageResponse?> GetPageAsync(
        string subscriberId,
        string view,
        int page,
        int limit)
    {
        if (!await IsReadyAsync(subscriberId)) return null;

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
            .Select(value => JsonSerializer.Deserialize<MessageItemResponse>(value!, JsonOptions))
            .Where(item => item is not null)
            .Cast<MessageItemResponse>()
            .ToList();

        var totalTask = _redis.SortedSetLengthAsync(indexKey);
        var unreadTask = _redis.SortedSetLengthAsync(UnreadKey(subscriberId));
        await Task.WhenAll(totalTask, unreadTask);

        return new MessagePageResponse(
            "ready",
            items,
            page,
            limit,
            totalTask.Result,
            unreadTask.Result);
    }

    public async Task<MessageSummaryResponse?> GetSummaryAsync(string subscriberId)
    {
        if (!await IsReadyAsync(subscriberId)) return null;

        var totalTask = _redis.SortedSetLengthAsync(AllKey(subscriberId));
        var unreadTask = _redis.SortedSetLengthAsync(UnreadKey(subscriberId));
        await Task.WhenAll(totalTask, unreadTask);
        return new MessageSummaryResponse("ready", totalTask.Result, unreadTask.Result);
    }

    public async Task SetReadStateAsync(string subscriberId, string messageId, bool read)
    {
        await SetReadStateInKeysAsync(
            MessagesKey(subscriberId),
            AllKey(subscriberId),
            UnreadKey(subscriberId),
            ReadKey(subscriberId),
            messageId,
            read);

        var generation = await _redis.StringGetAsync(ActiveRebuildKey(subscriberId));
        if (!generation.HasValue) return;

        await SetReadStateInKeysAsync(
            RebuildMessagesKey(subscriberId, generation!),
            RebuildAllKey(subscriberId, generation!),
            RebuildUnreadKey(subscriberId, generation!),
            RebuildReadKey(subscriberId, generation!),
            messageId,
            read);
    }

    private async Task SetReadStateInKeysAsync(
        RedisKey messagesKey,
        RedisKey allKey,
        RedisKey unreadKey,
        RedisKey readKey,
        string messageId,
        bool read)
    {
        var score = await _redis.SortedSetScoreAsync(allKey, messageId);
        if (!score.HasValue) return;

        var addKey = read ? readKey : unreadKey;
        var removeKey = read ? unreadKey : readKey;
        var transaction = _redis.CreateTransaction();
        _ = transaction.SortedSetAddAsync(addKey, messageId, score.Value);
        _ = transaction.SortedSetRemoveAsync(removeKey, messageId);
        await transaction.ExecuteAsync();

        var value = await _redis.HashGetAsync(messagesKey, messageId);
        if (!value.HasValue) return;

        var item = JsonSerializer.Deserialize<MessageItemResponse>(value!, JsonOptions);
        if (item is null) return;

        await _redis.HashSetAsync(
            messagesKey,
            messageId,
            JsonSerializer.Serialize(new MessageItemResponse
            {
                MessageId = item.MessageId,
                SourceSystem = item.SourceSystem,
                BusinessType = item.BusinessType,
                BusinessId = item.BusinessId,
                Title = item.Title,
                Content = item.Content,
                Url = item.Url,
                Read = read,
                Seen = item.Seen,
                CreatedAt = item.CreatedAt
            }, JsonOptions));
    }

    public async Task<IReadOnlyList<string>> GetUnreadIdsAsync(
        string subscriberId,
        int limit)
    {
        var ids = await _redis.SortedSetRangeByRankAsync(
            UnreadKey(subscriberId),
            0,
            limit - 1,
            Order.Descending);
        return ids.Select(value => value.ToString()).ToList();
    }

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
