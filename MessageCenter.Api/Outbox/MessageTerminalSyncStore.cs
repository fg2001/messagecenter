using MessageCenter.Api.Data;
using Microsoft.EntityFrameworkCore;

namespace MessageCenter.Api.Outbox;

/// <summary>与业务终态同事务入账；外部同步和后续迟到消息巡检不依赖Redis队列。</summary>
public sealed class MessageTerminalSyncEntity
{
    public string SendId { get; set; } = string.Empty;
    public long BusinessVersion { get; set; }
    public string State { get; set; } = "pending";
    public int Attempts { get; set; }
    public DateTime? NextAttemptAt { get; set; }
    public string? LockedBy { get; set; }
    public DateTime? LockedUntil { get; set; }
    public string? LastError { get; set; }
    public DateTime? LastConfirmedAt { get; set; }
    public string? MessageIds { get; set; }
    public int NextPage { get; set; }
    /// <summary>仅send受理事务内已套用先行终止标记的新行可证明从未触发；历史零计数不作证明。</summary>
    public bool NeverTriggered { get; set; }
}

public sealed class MessageTerminalSyncStore
{
    private readonly MessageCenterDbContext _db;
    public MessageTerminalSyncStore(MessageCenterDbContext db) => _db = db;

    public static void Schedule(MessageCenterDbContext db, MessageOutboxEntity row, DateTime now, bool neverTriggered = false)
    {
        db.MessageTerminalSync.Add(new MessageTerminalSyncEntity {
            SendId = row.SendId, BusinessVersion = row.BusinessVersion, NextAttemptAt = now, NeverTriggered = neverTriggered
        });
    }

    public async Task<IReadOnlyList<MessageTerminalSyncEntity>> ClaimAsync(
        string owner, int limit, TimeSpan lease, CancellationToken ct)
    {
        var now = DateTime.UtcNow;
        var candidates = await _db.MessageTerminalSync.AsNoTracking()
            .Where(x => (x.State == "pending" || x.State == "watching")
                && x.NextAttemptAt <= now && (x.LockedUntil == null || x.LockedUntil < now))
            .OrderBy(x => x.NextAttemptAt).Take(limit).ToListAsync(ct);
        var result = new List<MessageTerminalSyncEntity>();
        foreach (var item in candidates)
        {
            var until = now.Add(lease);
            var changed = await _db.Database.ExecuteSqlInterpolatedAsync($@"
UPDATE ""message_terminal_sync""
SET ""locked_by"" = {owner}, ""locked_until"" = {until}, ""attempts"" = ""attempts"" + 1
WHERE ""send_id"" = {item.SendId} AND ""business_version"" = {item.BusinessVersion}
  AND ""state"" IN ('pending', 'watching') AND ""next_attempt_at"" <= {now}
  AND (""locked_until"" IS NULL OR ""locked_until"" < {now})", ct);
            if (changed == 1)
            {
                item.LockedBy = owner; item.LockedUntil = until; item.Attempts++;
                result.Add(item);
            }
        }
        return result;
    }

    public Task<MessageOutboxEntity?> RecipientAsync(string sendId, CancellationToken ct)
        => _db.MessageOutbox.AsNoTracking().FirstOrDefaultAsync(x => x.SendId == sendId, ct);

    public Task<MessageTerminalSyncEntity?> GetAsync(string sendId, long version, CancellationToken ct)
        => _db.MessageTerminalSync.AsNoTracking().FirstOrDefaultAsync(
            x => x.SendId == sendId && x.BusinessVersion == version, ct);

    /// <summary>删除前保存目标ID；Novu删除成功但Redis失败时，重启后仍可清除原投影。</summary>
    public async Task<bool> RecordTargetsAsync(MessageTerminalSyncEntity item, string ids, int nextPage, CancellationToken ct)
    {
        var now = DateTime.UtcNow;
        return await _db.Database.ExecuteSqlInterpolatedAsync($@"
UPDATE ""message_terminal_sync"" SET ""message_ids"" = {ids}, ""next_page"" = {nextPage}
WHERE ""send_id"" = {item.SendId} AND ""business_version"" = {item.BusinessVersion}
  AND ""locked_by"" = {item.LockedBy} AND ""locked_until"" = {item.LockedUntil}
  AND ""locked_until"" > {now}", ct) == 1;
    }

    /// <summary>完成一次检查而非证明未来没有迟到投递；watching仍按原键周期巡检。过期owner不能回写。</summary>
    public async Task<bool> FinishAsync(MessageTerminalSyncEntity item, string state,
        DateTime? next, string? error, bool confirmed, CancellationToken ct)
    {
        var now = DateTime.UtcNow;
        DateTime? confirmedAt = confirmed ? now : item.LastConfirmedAt;
        return await _db.Database.ExecuteSqlInterpolatedAsync($@"
UPDATE ""message_terminal_sync""
SET ""state"" = {state}, ""next_attempt_at"" = {next}, ""last_error"" = {error},
    ""last_confirmed_at"" = {confirmedAt}, ""locked_by"" = NULL, ""locked_until"" = NULL
WHERE ""send_id"" = {item.SendId} AND ""business_version"" = {item.BusinessVersion}
  AND ""locked_by"" = {item.LockedBy} AND ""locked_until"" = {item.LockedUntil}
  AND ""locked_until"" > {now}", ct) == 1;
    }
}
