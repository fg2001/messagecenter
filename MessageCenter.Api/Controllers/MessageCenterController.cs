using System.Security.Claims;
using MessageCenter.Api.Audit;
using MessageCenter.Api.HttpClients;
using MessageCenter.Api.Models;
using MessageCenter.Api.Options;
using MessageCenter.Api.Services;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Options;

namespace MessageCenter.Api.Controllers;

[ApiController]
[Route("api/message-center")]
[Authorize]
public sealed class MessageCenterController : ControllerBase
{
    private static readonly HashSet<string> MessageViews =
        new(StringComparer.OrdinalIgnoreCase) { "all", "unread", "read" };

    private readonly NovuClient _novu;
    private readonly NovuOptions _options;
    private readonly SourceSystemOptions _sourceSystemOptions;
    private readonly IAuditSink _audit;
    private readonly MessageProjectionStore _projection;
    private readonly MessageWorkQueue _jobs;

    public MessageCenterController(
        NovuClient novu,
        IOptions<NovuOptions> options,
        IOptions<SourceSystemOptions> sourceSystemOptions,
        IAuditSink audit,
        MessageProjectionStore projection,
        MessageWorkQueue jobs)
    {
        _novu = novu;
        _options = options.Value;
        _sourceSystemOptions = sourceSystemOptions.Value;
        _audit = audit;
        _projection = projection;
        _jobs = jobs;
    }

    [HttpPost("send")]
    public async Task<IActionResult> Send(
        [FromBody] SendMessageRequest request,
        CancellationToken ct)
    {
        var clientId = GetPreferredUsername();
        if (clientId is null)
        {
            return Unauthorized(new { error = "preferred_username claim is missing from token." });
        }

        if (!_sourceSystemOptions.Names.TryGetValue(clientId, out var sourceSystem))
        {
            return StatusCode(StatusCodes.Status403Forbidden, new
            {
                error = $"Client '{clientId}' is not registered in SourceSystemNames.",
                clientId
            });
        }

        request.SourceSystem = sourceSystem;
        var triggers = MessageMapper.Map(request, _options.DefaultWorkflowId, out var skipped);
        if (triggers.Count == 0)
        {
            return BadRequest(new
            {
                error = "No actionable receivers. All receivers were skipped.",
                skipped
            });
        }

        var accepted = new List<string>();
        string? transactionId = null;
        string? status = null;
        var acknowledged = false;

        foreach (var trigger in triggers)
        {
            var result = await _novu.TriggerAsync(
                trigger.WorkflowId,
                trigger.SubscriberId,
                trigger.Payload,
                ct);

            accepted.Add(trigger.SubscriberId);
            transactionId = result.TransactionId;
            status = result.Status;
            acknowledged = result.Acknowledged;
            await _projection.RegisterActiveAsync(trigger.SubscriberId);
            await _jobs.EnqueueSyncAsync(trigger.SubscriberId);

            _audit.Record(new AuditEntry(
                result.TransactionId,
                sourceSystem,
                request.BusinessType,
                request.BusinessId,
                trigger.SubscriberId,
                StatusCodes.Status200OK,
                result.Status,
                result.Acknowledged,
                DateTime.UtcNow));
        }

        return StatusCode(StatusCodes.Status201Created, new SendMessageResponse
        {
            TransactionId = transactionId,
            Status = status,
            Acknowledged = acknowledged,
            Accepted = accepted,
            Skipped = skipped
        });
    }

    [HttpGet("my")]
    public async Task<IActionResult> GetMyMessages(
        [FromQuery] string view = "all",
        [FromQuery] int page = 1,
        [FromQuery] int limit = 100)
    {
        if (!MessageViews.Contains(view))
        {
            return BadRequest(new { error = "view must be one of: all, unread, read." });
        }
        if (page < 1)
        {
            return BadRequest(new { error = "page must be greater than or equal to 1." });
        }
        if (limit is < 1 or > 100)
        {
            return BadRequest(new { error = "limit must be between 1 and 100." });
        }

        var subscriberId = GetPreferredUsername();
        if (subscriberId is null)
        {
            return Unauthorized(new { error = "preferred_username claim is missing from token." });
        }

        await _projection.RegisterActiveAsync(subscriberId);
        var result = await _projection.GetPageAsync(
            subscriberId,
            view.ToLowerInvariant(),
            page,
            limit);
        if (result is not null) return Ok(result);

        await _jobs.EnqueueRebuildAsync(subscriberId);
        return Accepted(new MessagePageResponse(
            "building",
            Array.Empty<MessageItemResponse>(),
            page,
            limit,
            0,
            0));
    }

    [HttpGet("summary")]
    public async Task<IActionResult> GetSummary()
    {
        var subscriberId = GetPreferredUsername();
        if (subscriberId is null)
        {
            return Unauthorized(new { error = "preferred_username claim is missing from token." });
        }

        await _projection.RegisterActiveAsync(subscriberId);
        var result = await _projection.GetSummaryAsync(subscriberId);
        if (result is not null) return Ok(result);

        await _jobs.EnqueueRebuildAsync(subscriberId);
        return Accepted(new MessageSummaryResponse("building", 0, 0));
    }

    [HttpPost("messages/{messageId}/read")]
    public Task<IActionResult> MarkRead(string messageId, CancellationToken ct)
        => MarkAs(messageId, read: true, ct);

    [HttpPost("messages/{messageId}/unread")]
    public Task<IActionResult> MarkUnread(string messageId, CancellationToken ct)
        => MarkAs(messageId, read: false, ct);

    [HttpPost("my/read-all")]
    public async Task<IActionResult> MarkAllMyMessagesRead()
    {
        var subscriberId = GetPreferredUsername();
        if (subscriberId is null)
        {
            return Unauthorized(new { error = "preferred_username claim is missing from token." });
        }

        if (!await _projection.IsReadyAsync(subscriberId))
        {
            await _jobs.EnqueueRebuildAsync(subscriberId);
            return Conflict(new { error = "Message projection is building." });
        }

        var job = await _jobs.EnqueueReadAllAsync(subscriberId);
        return Accepted(job);
    }

    [HttpGet("my/read-all/{jobId}")]
    public async Task<IActionResult> GetReadAllJob(string jobId)
    {
        var subscriberId = GetPreferredUsername();
        if (subscriberId is null)
        {
            return Unauthorized(new { error = "preferred_username claim is missing from token." });
        }

        var status = await _jobs.GetReadAllStatusAsync(jobId, subscriberId);
        return status is null ? NotFound() : Ok(status);
    }

    private async Task<IActionResult> MarkAs(
        string messageId,
        bool read,
        CancellationToken ct)
    {
        var subscriberId = GetPreferredUsername();
        if (subscriberId is null)
        {
            return Unauthorized(new { error = "preferred_username claim is missing from token." });
        }

        if (!await _projection.IsReadyAsync(subscriberId))
        {
            await _jobs.EnqueueRebuildAsync(subscriberId);
            return Conflict(new { error = "Message projection is building." });
        }

        await _novu.MarkAsAsync(subscriberId, messageId, read, ct);
        await _projection.SetReadStateAsync(subscriberId, messageId, read);
        return Ok(new MessageStateResponse(messageId, read));
    }

    private string? GetPreferredUsername()
    {
        var username = User.FindFirstValue("preferred_username");
        return string.IsNullOrWhiteSpace(username) ? null : username;
    }
}
