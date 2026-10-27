using System.Data;
using System.Data.Common;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using MessageCenter.Api.Data;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;

namespace MessageCenter.Api.Outbox;

public sealed class MessageRequestGuardEntity
{
    public string RequestKey { get; set; } = string.Empty;
    public string SourceSystem { get; set; } = string.Empty;
    public string RequestId { get; set; } = string.Empty;
}

/// <summary>发送与生命周期操作共用持久原键锁，覆盖消息行尚不存在的窗口；锁记录不随请求结束删除。</summary>
internal static class MessageRequestGuard
{
    public static async Task<IDbContextTransaction> LockAsync(
        MessageCenterDbContext db, string sourceSystem, string requestId, CancellationToken ct)
    {
        // 内存测试不支持关系事务/行锁；并发保障必须由真实DM8专项证明。
        if (!db.Database.IsRelational()) return await db.Database.BeginTransactionAsync(ct);
        var key = Convert.ToHexString(SHA256.HashData(
            Encoding.UTF8.GetBytes(JsonSerializer.Serialize(new[] { sourceSystem, requestId })))).ToLowerInvariant();
        if (!await db.MessageRequestGuards.AsNoTracking().AnyAsync(x => x.RequestKey == key, ct))
        {
            try
            {
                // 独立提交只创建锁行，不涉及发送/终态；并发首次创建由唯一键裁决。
                await db.Database.ExecuteSqlInterpolatedAsync($@"
INSERT INTO ""message_request_guard"" (""request_key"", ""source_system"", ""request_id"")
VALUES ({key}, {sourceSystem}, {requestId})", ct);
            }
            catch (DbException)
            {
                if (!await db.MessageRequestGuards.AsNoTracking().AnyAsync(x => x.RequestKey == key, ct)) throw;
            }
        }
        var transaction = await db.Database.BeginTransactionAsync(IsolationLevel.ReadCommitted, ct);
        try
        {
            var rows = await db.MessageRequestGuards.FromSqlInterpolated($@"
SELECT ""request_key"", ""source_system"", ""request_id""
FROM ""message_request_guard"" WHERE ""request_key"" = {key} FOR UPDATE").AsNoTracking().ToListAsync(ct);
            if (rows.Count != 1 || !string.Equals(rows[0].SourceSystem, sourceSystem, StringComparison.Ordinal)
                || !string.Equals(rows[0].RequestId, requestId, StringComparison.Ordinal))
                throw new InvalidOperationException("The original request guard does not match this request.");
            return transaction;
        }
        catch
        {
            await transaction.DisposeAsync();
            throw;
        }
    }
}
