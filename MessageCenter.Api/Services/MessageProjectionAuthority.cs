using System.Data.Common;
using MessageCenter.Api.Data;
using MessageCenter.Api.Outbox;
using Microsoft.EntityFrameworkCore;

namespace MessageCenter.Api.Services;

public sealed class MessageSubscriberVersionEntity
{
    public string SubscriberId { get; set; } = string.Empty;
    public long Revision { get; set; }
}

/// <summary>每收件人的持久终态版本使旧Redis代际失效；所有判定使用MC原发送记录，不凭业务标题猜配。</summary>
public sealed class MessageProjectionAuthority
{
    private readonly MessageCenterDbContext _db;
    public MessageProjectionAuthority(MessageCenterDbContext db) => _db = db;

    public async Task<long> RevisionAsync(string subscriberId)
        => await _db.MessageSubscriberVersions.AsNoTracking().Where(x => x.SubscriberId == subscriberId)
            .Select(x => (long?)x.Revision).FirstOrDefaultAsync() ?? 0;

    /// <summary>在原请求事务前独立建立版本行；并发首次建行只由唯一键裁决。</summary>
    public async Task EnsureAsync(string subscriberId, CancellationToken ct)
    {
        if (await _db.MessageSubscriberVersions.AsNoTracking().AnyAsync(x => x.SubscriberId == subscriberId, ct)) return;
        if (!_db.Database.IsRelational())
        {
            _db.MessageSubscriberVersions.Add(new() { SubscriberId = subscriberId });
            await _db.SaveChangesAsync(ct);
            return;
        }
        try
        {
            await _db.Database.ExecuteSqlInterpolatedAsync($@"
INSERT INTO ""message_subscriber_version"" (""subscriber_id"", ""revision"") VALUES ({subscriberId}, 0)", ct);
        }
        catch (DbException)
        {
            if (!await _db.MessageSubscriberVersions.AsNoTracking().AnyAsync(x => x.SubscriberId == subscriberId, ct)) throw;
        }
    }

    /// <summary>与终态及补偿同事务单调递增，回滚时不得提前使投影失效。</summary>
    public async Task BumpAsync(string subscriberId, CancellationToken ct)
    {
        if (!_db.Database.IsRelational())
        {
            (await _db.MessageSubscriberVersions.SingleAsync(x => x.SubscriberId == subscriberId, ct)).Revision++;
            return;
        }
        if (await _db.Database.ExecuteSqlInterpolatedAsync($@"
UPDATE ""message_subscriber_version"" SET ""revision"" = ""revision"" + 1 WHERE ""subscriber_id"" = {subscriberId}", ct) != 1)
            throw new InvalidOperationException("Subscriber projection version is missing.");
    }

    public async Task<IReadOnlyList<ProjectionMessage>> ActiveAsync(string subscriberId, IReadOnlyList<ProjectionMessage> messages)
    {
        var sendIds = messages.Select(x => x.SendId).Where(x => !string.IsNullOrWhiteSpace(x)).Cast<string>().Distinct().ToList();
        var transactions = messages.Select(x => x.TransactionId).Where(x => !string.IsNullOrWhiteSpace(x)).Cast<string>().Distinct().ToList();
        var rows = sendIds.Count == 0 && transactions.Count == 0 ? new List<MessageOutboxEntity>()
            : await _db.MessageOutbox.AsNoTracking().Where(x => x.SubscriberId == subscriberId
                && (sendIds.Contains(x.SendId) || transactions.Contains(x.SendId)
                    || x.NovuTransactionId != null && transactions.Contains(x.NovuTransactionId))).ToListAsync();
        return messages.Where(message => {
            var matches = rows.Where(row => row.SendId == message.SendId
                || row.SendId == message.TransactionId
                || !string.IsNullOrWhiteSpace(row.NovuTransactionId) && row.NovuTransactionId == message.TransactionId).ToList();
            if (matches.Count == 0) return message.Item.SourceSystem != "micip";
            if (matches.Count != 1) return false;
            var match = matches[0];
            return (string.IsNullOrWhiteSpace(message.Item.SourceSystem) || message.Item.SourceSystem == match.SourceSystem)
                && !MessageBusinessState.IsTerminal(match.BusinessState);
        }).ToList();
    }
}

// 只在Redis内部保留关联键；公开MessageItemResponse结构保持兼容。
public sealed record ProjectionMessage(Models.MessageItemResponse Item, string? SendId, string? TransactionId);
