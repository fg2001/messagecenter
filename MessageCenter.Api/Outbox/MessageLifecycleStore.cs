using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using MessageCenter.Api.Data;
using MessageCenter.Api.Services;
using Microsoft.EntityFrameworkCore;

namespace MessageCenter.Api.Outbox;

/// <summary>同 operationId 重放但载荷指纹不同（409）。</summary>
public sealed class MessageLifecycleConflictException : Exception
{
    public MessageLifecycleConflictException(string operationId)
        : base($"OperationId '{operationId}' was already used with a different payload.")
    {
    }
}

/// <summary>CONSUME 目标收件人行不存在（404）：本人尚未收到消息，无可消费条目。</summary>
public sealed class MessageLifecycleNotFoundException : Exception
{
    public MessageLifecycleNotFoundException(string requestId, string subscriberId)
        : base($"No recipient entry exists for request '{requestId}' and subscriber '{subscriberId}'.")
    {
    }
}

/// <summary>businessVersion 不大于当前值（422，防倒退/防并发旧写）。</summary>
public sealed class MessageLifecycleVersionRejectException : Exception
{
    public long AttemptedVersion { get; }
    public long CurrentVersion { get; }

    public MessageLifecycleVersionRejectException(
        string requestId, string subscriberId, long attempted, long current)
        : base(
            $"businessVersion {attempted} for request '{requestId}' subscriber '{subscriberId}' " +
            $"is not greater than the current version {current}.")
    {
        AttemptedVersion = attempted;
        CurrentVersion = current;
    }
}

public sealed record LifecycleApplyInput(
    string SourceSystem,
    string RequestId,
    string SubscriberId,
    string OperationId,
    string Action,
    long BusinessVersion,
    string? Reason,
    DateTime? OccurredAt);

public sealed record LifecycleApplyResult(
    string OperationId,
    string RequestId,
    string SubscriberId,
    string Action,
    long BusinessVersion,
    string ApplyStatus,
    string? ResultBusinessState,
    string? SendId,
    bool Replayed,
    DateTime? OccurredAt,
    DateTime RecordedAt);

/// <summary>MC 消息生命周期（T-INT-01 契约候选稿 §6）：
/// CONSUME/REVOKE/EXPIRE 按 (source, requestId, subscriberId) 原键作用；
/// REVOKE/EXPIRE 在 send 到达前按原键暂存（deferred 终止标记），迟到的 send 受理即套用终态且不再触发 Novu。</summary>
public sealed class MessageLifecycleStore
{
    private static readonly JsonSerializerOptions JsonOptions =
        new(JsonSerializerDefaults.Web);

    private readonly MessageCenterDbContext _db;

    public MessageLifecycleStore(MessageCenterDbContext db)
    {
        _db = db;
    }

    public async Task<LifecycleApplyResult> ApplyAsync(
        LifecycleApplyInput input,
        CancellationToken ct)
    {
        if (!MessageBusinessState.TryMapAction(input.Action, out var targetState))
        {
            throw new InvalidOperationException(
                $"Action must be CONSUME, REVOKE or EXPIRE (got '{input.Action}').");
        }

        var operationHash = ComputeOperationHash(input);
        var projection = new MessageProjectionAuthority(_db);
        await projection.EnsureAsync(input.SubscriberId, ct);
        await using var transaction = await MessageRequestGuard.LockAsync(_db, input.SourceSystem, input.RequestId, ct);
        var existing = await _db.MessageLifecycleOperations.FirstOrDefaultAsync(
            x => x.SourceSystem == input.SourceSystem && x.OperationId == input.OperationId,
            ct);
        if (existing is not null)
        {
            // 同 operationId 同载荷：返回原结果（不重复作用）；异载荷：409。
            if (!string.Equals(existing.OperationHash, operationHash, StringComparison.Ordinal))
            {
                throw new MessageLifecycleConflictException(input.OperationId);
            }

            return new LifecycleApplyResult(
                existing.OperationId,
                existing.RequestId,
                existing.SubscriberId,
                existing.Action,
                existing.BusinessVersion,
                existing.ApplyStatus,
                existing.ResultBusinessState,
                existing.SendId,
                Replayed: true,
                existing.OccurredAt,
                existing.RecordedAt);
        }

        var now = DateTime.UtcNow;
        var row = await _db.MessageOutbox.FirstOrDefaultAsync(
            x => x.SourceSystem == input.SourceSystem
                 && x.RequestId == input.RequestId
                 && x.SubscriberId == input.SubscriberId,
            ct);

        if (row is null)
        {
            if (input.Action == "CONSUME")
            {
                // 本人条目尚不存在：不可消费。REVOKE/EXPIRE 允许先行暂存为终止标记。
                throw new MessageLifecycleNotFoundException(input.RequestId, input.SubscriberId);
            }

            var version = await _db.MessageLifecycleOperations
                .Where(x => x.SourceSystem == input.SourceSystem && x.RequestId == input.RequestId
                    && x.SubscriberId == input.SubscriberId)
                .Select(x => (long?)x.BusinessVersion).MaxAsync(ct);
            if (version.HasValue && input.BusinessVersion <= version.Value)
                throw new MessageLifecycleVersionRejectException(
                    input.RequestId, input.SubscriberId, input.BusinessVersion, version.Value);

            var deferred = new MessageLifecycleOperationEntity
            {
                OperationId = input.OperationId,
                SourceSystem = input.SourceSystem,
                RequestId = input.RequestId,
                SubscriberId = input.SubscriberId,
                SendId = null,
                Action = input.Action,
                BusinessVersion = input.BusinessVersion,
                Reason = NormalizeReason(input.Reason),
                OperationHash = operationHash,
                ApplyStatus = MessageLifecycleApplyStatus.Deferred,
                ResultBusinessState = targetState,
                OccurredAt = input.OccurredAt ?? now,
                RecordedAt = now
            };
            _db.MessageLifecycleOperations.Add(deferred);
            await _db.SaveChangesAsync(ct);
            await transaction.CommitAsync(ct);
            return new LifecycleApplyResult(
                deferred.OperationId,
                deferred.RequestId,
                deferred.SubscriberId,
                deferred.Action,
                deferred.BusinessVersion,
                MessageLifecycleApplyStatus.Deferred,
                deferred.ResultBusinessState,
                null,
                Replayed: false,
                deferred.OccurredAt,
                deferred.RecordedAt);
        }

        // 防倒退：businessVersion ≤ 当前一律拒绝（等值亦拒，并发旧写同样落空）。
        if (input.BusinessVersion <= row.BusinessVersion)
        {
            throw new MessageLifecycleVersionRejectException(
                input.RequestId, input.SubscriberId, input.BusinessVersion, row.BusinessVersion);
        }

        var operation = new MessageLifecycleOperationEntity
        {
            OperationId = input.OperationId,
            SourceSystem = input.SourceSystem,
            RequestId = input.RequestId,
            SubscriberId = input.SubscriberId,
            SendId = row.SendId,
            Action = input.Action,
            BusinessVersion = input.BusinessVersion,
            Reason = NormalizeReason(input.Reason),
            OperationHash = operationHash,
            ApplyStatus = MessageLifecycleApplyStatus.Applied,
            ResultBusinessState = targetState,
            OccurredAt = input.OccurredAt ?? now,
            RecordedAt = now,
            AppliedAt = now
        };
        _db.MessageLifecycleOperations.Add(operation);

        row.BusinessState = targetState;
        row.BusinessVersion = input.BusinessVersion;
        row.TerminalAt = now;
        row.TerminalReason = NormalizeReason(input.Reason);

        if (row.Status is MessageOutboxState.Pending or MessageOutboxState.Accepted)
        {
            // 终态落在尚未投递/尚在补投的行上：抑制发送（不生成有效通知），释放租约与退避。
            row.Status = MessageOutboxState.Suppressed;
            row.NextRetryAt = null;
            row.LockedBy = null;
            row.LockedUntil = null;
        }

        var neverTriggered = await _db.MessageTerminalSync.AsNoTracking()
            .AnyAsync(x => x.SendId == row.SendId && x.NeverTriggered, ct);
        MessageTerminalSyncStore.Schedule(_db, row, now, neverTriggered);
        await projection.BumpAsync(row.SubscriberId, ct);
        await _db.SaveChangesAsync(ct);
        await transaction.CommitAsync(ct);
        return new LifecycleApplyResult(
            operation.OperationId,
            operation.RequestId,
            operation.SubscriberId,
            operation.Action,
            operation.BusinessVersion,
            MessageLifecycleApplyStatus.Applied,
            operation.ResultBusinessState,
            operation.SendId,
            Replayed: false,
            operation.OccurredAt,
            operation.RecordedAt);
    }

    public Task<MessageLifecycleOperationEntity?> GetOperationAsync(
        string sourceSystem,
        string requestId,
        string operationId,
        CancellationToken ct)
        => _db.MessageLifecycleOperations.AsNoTracking().FirstOrDefaultAsync(
            x => x.SourceSystem == sourceSystem
                 && x.RequestId == requestId
                 && x.OperationId == operationId,
            ct);

    public Task<MessageTerminalSyncEntity?> GetExternalSyncAsync(string sendId, long version, CancellationToken ct)
        => _db.MessageTerminalSync.AsNoTracking().FirstOrDefaultAsync(
            x => x.SendId == sendId && x.BusinessVersion == version, ct);

    /// <summary>迟到的 send 受理后套用暂存终止标记：取该收件人最高版本标记生效，
    /// 低版本标记置 superseded（保留审计）；命中行立即置终态并抑制发送。</summary>
    public static void ApplyDeferredMarkers(
        IReadOnlyList<MessageLifecycleOperationEntity> deferredMarkers,
        IReadOnlyList<MessageOutboxEntity> newRows,
        DateTime now)
    {
        foreach (var row in newRows)
        {
            var markers = deferredMarkers
                .Where(x => x.SubscriberId == row.SubscriberId)
                .OrderByDescending(x => x.BusinessVersion)
                .ToList();
            if (markers.Count == 0) continue;

            var winner = markers[0];
            row.BusinessState = winner.ResultBusinessState ?? MessageBusinessState.Active;
            row.BusinessVersion = winner.BusinessVersion;
            row.TerminalAt = now;
            row.TerminalReason = winner.Reason;
            row.Status = MessageOutboxState.Suppressed;
            row.NextRetryAt = null;

            winner.ApplyStatus = MessageLifecycleApplyStatus.Applied;
            winner.SendId = row.SendId;
            winner.AppliedAt = now;
            foreach (var loser in markers.Skip(1))
            {
                loser.ApplyStatus = MessageLifecycleApplyStatus.Superseded;
            }
        }
    }

    /// <summary>载荷指纹：业务语义字段（requestId/subscriberId/action/businessVersion/reason）。
    /// occurredAt 为信息性时间戳，不参与幂等判定（重放允许省略）。</summary>
    private static string ComputeOperationHash(LifecycleApplyInput input)
    {
        var canonical = JsonSerializer.Serialize(new
        {
            input.SourceSystem,
            input.RequestId,
            input.SubscriberId,
            input.Action,
            input.BusinessVersion,
            reason = NormalizeReason(input.Reason)
        }, JsonOptions);
        return Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(canonical)))
            .ToLowerInvariant();
    }

    private static string? NormalizeReason(string? reason)
        => string.IsNullOrWhiteSpace(reason) ? null : reason.Trim();
}
