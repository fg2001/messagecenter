using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using MessageCenter.Api.Data;
using MessageCenter.Api.HttpClients.Dtos;
using MessageCenter.Api.Models;
using MessageCenter.Api.Services;
using Microsoft.EntityFrameworkCore;

namespace MessageCenter.Api.Outbox;

public sealed record ClaimedMessageOutboxItem(
    string SendId,
    string SubscriberId,
    string WorkflowId,
    string Payload,
    string Status,
    int RetryCount,
    string? NovuTransactionId);

public sealed class MessageOutboxConflictException : Exception
{
    public MessageOutboxConflictException(string requestId)
        : base($"RequestId '{requestId}' was already used with a different payload.")
    {
    }
}

public sealed class MessageOutboxStore
{
    private static readonly JsonSerializerOptions JsonOptions =
        new(JsonSerializerDefaults.Web);

    private readonly MessageCenterDbContext _db;

    public MessageOutboxStore(MessageCenterDbContext db)
    {
        _db = db;
    }

    public async Task<SendMessageResponse> EnqueueAsync(
        string sourceSystem,
        SendMessageRequest request,
        IReadOnlyList<NovuTriggerPayload> triggers,
        IReadOnlyList<string> skipped,
        CancellationToken ct)
    {
        var requestHash = ComputeRequestHash(sourceSystem, request, triggers);
        var existing = await FindRequestAsync(sourceSystem, request.RequestId, ct);
        if (existing.Count > 0)
        {
            EnsureSameRequest(existing, request.RequestId, requestHash);
            return MapSendResponse(request.RequestId, existing, skipped);
        }

        var now = DateTime.UtcNow;
        var entities = triggers.Select(trigger =>
        {
            var sendId = Guid.NewGuid().ToString("N");
            var payload = new NovuTriggerPayload
            {
                WorkflowId = trigger.WorkflowId,
                SubscriberId = trigger.SubscriberId,
                Payload = new NovuPayloadFields
                {
                    RequestId = request.RequestId,
                    SendId = sendId,
                    SourceSystem = trigger.Payload.SourceSystem,
                    BusinessType = trigger.Payload.BusinessType,
                    BusinessId = trigger.Payload.BusinessId,
                    Title = trigger.Payload.Title,
                    Content = trigger.Payload.Content,
                    Url = trigger.Payload.Url
                }
            };

            return new MessageOutboxEntity
            {
                SendId = sendId,
                RequestId = request.RequestId,
                RequestHash = requestHash,
                SourceSystem = sourceSystem,
                SubscriberId = trigger.SubscriberId,
                WorkflowId = trigger.WorkflowId,
                Payload = JsonSerializer.Serialize(payload, JsonOptions),
                Status = MessageOutboxState.Pending,
                NextRetryAt = now,
                CreatedAt = now,
                UpdatedAt = now
            };
        }).ToList();

        await using var transaction = await _db.Database.BeginTransactionAsync(ct);
        try
        {
            _db.MessageOutbox.AddRange(entities);
            await _db.SaveChangesAsync(ct);
            await transaction.CommitAsync(ct);
            return MapSendResponse(request.RequestId, entities, skipped);
        }
        catch (DbUpdateException)
        {
            await transaction.RollbackAsync(ct);
            _db.ChangeTracker.Clear();
            existing = await FindRequestAsync(sourceSystem, request.RequestId, ct);
            if (existing.Count == 0) throw;

            EnsureSameRequest(existing, request.RequestId, requestHash);
            return MapSendResponse(request.RequestId, existing, skipped);
        }
    }

    public async Task<SendRequestStatusResponse?> GetRequestAsync(
        string sourceSystem,
        string requestId,
        CancellationToken ct)
    {
        var entities = await FindRequestAsync(sourceSystem, requestId, ct);
        return entities.Count == 0
            ? null
            : MapStatusResponse(requestId, entities);
    }

    public async Task<bool> RetryFailedAsync(
        string sourceSystem,
        string requestId,
        CancellationToken ct)
    {
        var entities = await _db.MessageOutbox
            .Where(x => x.SourceSystem == sourceSystem &&
                        x.RequestId == requestId &&
                        x.Status == MessageOutboxState.Failed)
            .ToListAsync(ct);
        if (entities.Count == 0) return false;

        var now = DateTime.UtcNow;
        foreach (var entity in entities)
        {
            entity.Status = MessageOutboxState.Pending;
            entity.RetryCount = 0;
            entity.NextRetryAt = now;
            entity.LockedBy = null;
            entity.LockedUntil = null;
            entity.LastError = null;
            entity.FailedAt = null;
            entity.UpdatedAt = now;
        }
        await _db.SaveChangesAsync(ct);
        return true;
    }

    public async Task<IReadOnlyList<ClaimedMessageOutboxItem>> ClaimDueAsync(
        string worker,
        int batchSize,
        TimeSpan lease,
        CancellationToken ct)
    {
        var now = DateTime.UtcNow;
        var candidateIds = await _db.MessageOutbox
            .AsNoTracking()
            .Where(x =>
                (x.Status == MessageOutboxState.Pending ||
                 x.Status == MessageOutboxState.Accepted) &&
                (x.NextRetryAt == null || x.NextRetryAt <= now) &&
                (x.LockedUntil == null || x.LockedUntil < now))
            .OrderBy(x => x.CreatedAt)
            .Select(x => x.SendId)
            .Take(batchSize)
            .ToListAsync(ct);

        var claimedIds = new List<string>();
        var lockedUntil = now.Add(lease);
        foreach (var sendId in candidateIds)
        {
            var affected = await _db.Database.ExecuteSqlInterpolatedAsync($@"
UPDATE ""message_send_outbox""
SET ""locked_by"" = {worker}, ""locked_until"" = {lockedUntil}, ""updated_at"" = {now}
WHERE ""send_id"" = {sendId}
  AND ""status"" IN ({MessageOutboxState.Pending}, {MessageOutboxState.Accepted})
  AND (""next_retry_at"" IS NULL OR ""next_retry_at"" <= {now})
  AND (""locked_until"" IS NULL OR ""locked_until"" < {now})", ct);
            if (affected == 1) claimedIds.Add(sendId);
        }

        if (claimedIds.Count == 0) return Array.Empty<ClaimedMessageOutboxItem>();

        return await _db.MessageOutbox
            .AsNoTracking()
            .Where(x => claimedIds.Contains(x.SendId) && x.LockedBy == worker)
            .Select(x => new ClaimedMessageOutboxItem(
                x.SendId,
                x.SubscriberId,
                x.WorkflowId,
                x.Payload,
                x.Status,
                x.RetryCount,
                x.NovuTransactionId))
            .ToListAsync(ct);
    }

    public async Task MarkAcceptedAsync(
        string sendId,
        string worker,
        TriggerResult result,
        CancellationToken ct)
    {
        var entity = await FindClaimedAsync(sendId, worker, ct);
        var now = DateTime.UtcNow;
        entity.Status = MessageOutboxState.Accepted;
        entity.NovuTransactionId = result.TransactionId;
        entity.NovuStatus = result.Status;
        entity.Acknowledged = result.Acknowledged;
        entity.AcceptedAt ??= now;
        entity.NextRetryAt = now;
        entity.LockedBy = null;
        entity.LockedUntil = null;
        entity.LastError = null;
        entity.UpdatedAt = now;
        await _db.SaveChangesAsync(ct);
    }

    public async Task MarkVisibleAsync(
        string sendId,
        string worker,
        CancellationToken ct)
    {
        var entity = await FindClaimedAsync(sendId, worker, ct);
        var now = DateTime.UtcNow;
        entity.Status = MessageOutboxState.Visible;
        entity.VisibleAt = now;
        entity.NextRetryAt = null;
        entity.LockedBy = null;
        entity.LockedUntil = null;
        entity.LastError = null;
        entity.UpdatedAt = now;
        await _db.SaveChangesAsync(ct);
    }

    public async Task ScheduleAcceptedCheckAsync(
        string sendId,
        string worker,
        DateTime nextRetryAt,
        string? error,
        CancellationToken ct)
    {
        var entity = await FindClaimedAsync(sendId, worker, ct);
        entity.Status = MessageOutboxState.Accepted;
        entity.NextRetryAt = nextRetryAt;
        entity.LockedBy = null;
        entity.LockedUntil = null;
        entity.LastError = Truncate(error);
        entity.UpdatedAt = DateTime.UtcNow;
        await _db.SaveChangesAsync(ct);
    }

    public async Task MarkSendFailureAsync(
        string sendId,
        string worker,
        int maxRetryCount,
        TimeSpan retryDelay,
        string error,
        CancellationToken ct)
    {
        var entity = await FindClaimedAsync(sendId, worker, ct);
        var now = DateTime.UtcNow;
        entity.RetryCount++;
        entity.LastError = Truncate(error);
        entity.LockedBy = null;
        entity.LockedUntil = null;
        entity.UpdatedAt = now;

        if (entity.RetryCount >= maxRetryCount)
        {
            entity.Status = MessageOutboxState.Failed;
            entity.NextRetryAt = null;
            entity.FailedAt = now;
        }
        else
        {
            entity.Status = MessageOutboxState.Pending;
            entity.NextRetryAt = now.Add(retryDelay);
        }

        await _db.SaveChangesAsync(ct);
    }

    public Task<bool> CanConnectAsync(CancellationToken ct)
        => _db.Database.CanConnectAsync(ct);

    private async Task<MessageOutboxEntity> FindClaimedAsync(
        string sendId,
        string worker,
        CancellationToken ct)
        => await _db.MessageOutbox.FirstOrDefaultAsync(
               x => x.SendId == sendId && x.LockedBy == worker,
               ct)
           ?? throw new InvalidOperationException(
               $"Outbox lease for send '{sendId}' is no longer owned by '{worker}'.");

    private Task<List<MessageOutboxEntity>> FindRequestAsync(
        string sourceSystem,
        string requestId,
        CancellationToken ct)
        => _db.MessageOutbox
            .AsNoTracking()
            .Where(x => x.SourceSystem == sourceSystem && x.RequestId == requestId)
            .OrderBy(x => x.CreatedAt)
            .ToListAsync(ct);

    private static void EnsureSameRequest(
        IReadOnlyList<MessageOutboxEntity> entities,
        string requestId,
        string requestHash)
    {
        if (entities.Any(x => x.RequestHash != requestHash))
        {
            throw new MessageOutboxConflictException(requestId);
        }
    }

    private static string ComputeRequestHash(
        string sourceSystem,
        SendMessageRequest request,
        IReadOnlyList<NovuTriggerPayload> triggers)
    {
        var canonical = JsonSerializer.Serialize(new
        {
            sourceSystem,
            request.BusinessType,
            request.BusinessId,
            request.Title,
            request.Content,
            request.Url,
            receivers = triggers
                .Select(x => x.SubscriberId)
                .OrderBy(x => x, StringComparer.Ordinal)
                .ToArray()
        }, JsonOptions);
        return Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(canonical)))
            .ToLowerInvariant();
    }

    private static SendMessageResponse MapSendResponse(
        string requestId,
        IReadOnlyList<MessageOutboxEntity> entities,
        IReadOnlyList<string> skipped)
        => new()
        {
            RequestId = requestId,
            State = AggregateState(entities),
            Recipients = entities.Select(MapRecipient).ToList(),
            Skipped = skipped
        };

    private static SendRequestStatusResponse MapStatusResponse(
        string requestId,
        IReadOnlyList<MessageOutboxEntity> entities)
        => new()
        {
            RequestId = requestId,
            State = AggregateState(entities),
            Recipients = entities.Select(MapRecipient).ToList()
        };

    private static SendRecipientResponse MapRecipient(MessageOutboxEntity entity)
        => new()
        {
            SendId = entity.SendId,
            SubscriberId = entity.SubscriberId,
            State = entity.Status,
            RetryCount = entity.RetryCount,
            TransactionId = entity.NovuTransactionId,
            Error = entity.LastError
        };

    private static string AggregateState(IReadOnlyList<MessageOutboxEntity> entities)
    {
        if (entities.All(x => x.Status == MessageOutboxState.Visible)) return MessageOutboxState.Visible;
        if (entities.Any(x => x.Status == MessageOutboxState.Failed)) return MessageOutboxState.Failed;
        if (entities.Any(x => x.Status == MessageOutboxState.Accepted)) return MessageOutboxState.Accepted;
        return MessageOutboxState.Pending;
    }

    private static string? Truncate(string? value)
        => string.IsNullOrEmpty(value) || value.Length <= 2000
            ? value
            : value[..2000];
}
