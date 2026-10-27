using System.Security.Claims;
using MessageCenter.Api.HttpClients;
using MessageCenter.Api.Models;
using MessageCenter.Api.Options;
using MessageCenter.Api.Outbox;
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
    private readonly MessageOutboxStore _outbox;
    private readonly MessageProjectionStore _projection;
    private readonly MessageWorkQueue _jobs;

    public MessageCenterController(
        NovuClient novu,
        IOptions<NovuOptions> options,
        IOptions<SourceSystemOptions> sourceSystemOptions,
        MessageOutboxStore outbox,
        MessageProjectionStore projection,
        MessageWorkQueue jobs)
    {
        _novu = novu;
        _options = options.Value;
        _sourceSystemOptions = sourceSystemOptions.Value;
        _outbox = outbox;
        _projection = projection;
        _jobs = jobs;
    }

    [HttpPost("send")]
    public async Task<IActionResult> Send(
        [FromBody] SendMessageRequest request,
        CancellationToken ct)
    {
        var source = GetSourceSystem();
        if (source.Result is not null) return source.Result;
        var sourceSystem = source.Name!;

        request.SourceSystem = sourceSystem;
        if (!_options.TryGetWorkflow(sourceSystem, out var workflowId))
        {
            return StatusCode(StatusCodes.Status503ServiceUnavailable, new
            {
                code = "SOURCE_WORKFLOW_UNAVAILABLE",
                error = "The registered source workflow is not configured."
            });
        }
        var triggers = MessageMapper.Map(request, workflowId, out var skipped);
        if (triggers.Count == 0)
        {
            return BadRequest(new
            {
                error = "No actionable receivers. All receivers were skipped.",
                skipped
            });
        }

        try
        {
            var response = await _outbox.EnqueueAsync(
                sourceSystem,
                request,
                triggers,
                skipped,
                ct);
            return Accepted(response);
        }
        catch (MessageOutboxConflictException ex)
        {
            return Conflict(new { error = ex.Message, requestId = request.RequestId });
        }
    }

    [HttpGet("send/{requestId}")]
    public async Task<IActionResult> GetSendStatus(
        string requestId,
        CancellationToken ct)
    {
        var sourceSystem = GetSourceSystem();
        if (sourceSystem.Result is not null) return sourceSystem.Result;

        var response = await _outbox.GetRequestAsync(sourceSystem.Name!, requestId, ct);
        return response is null ? NotFound() : Ok(response);
    }

    [HttpPost("send/{requestId}/retry")]
    public async Task<IActionResult> RetrySend(
        string requestId,
        CancellationToken ct)
    {
        var sourceSystem = GetSourceSystem();
        if (sourceSystem.Result is not null) return sourceSystem.Result;

        var retried = await _outbox.RetryFailedAsync(sourceSystem.Name!, requestId, ct);
        if (!retried) return NotFound(new { error = "No failed recipients were found." });

        var response = await _outbox.GetRequestAsync(sourceSystem.Name!, requestId, ct);
        return Accepted(response);
    }

    /// <summary>严格来源 uncertain 触发的人工对账：原来源令牌方可操作本来源原键。
    /// CONFIRM_SENT 终结为已受理继续可见性投影；CONFIRM_NOT_SENT 以原 transactionId 恢复单次重发。</summary>
    [HttpPost("send/{requestId}/resolve-trigger")]
    public async Task<IActionResult> ResolveTrigger(
        string requestId,
        [FromBody] ResolveTriggerRequest body,
        CancellationToken ct)
    {
        var sourceSystem = GetSourceSystem();
        if (sourceSystem.Result is not null) return sourceSystem.Result;

        if (string.IsNullOrWhiteSpace(body?.SendId)
            || (body.Resolution != "CONFIRM_SENT" && body.Resolution != "CONFIRM_NOT_SENT"))
        {
            return BadRequest(new
            {
                error = "sendId is required and resolution must be CONFIRM_SENT or CONFIRM_NOT_SENT."
            });
        }

        try
        {
            await _outbox.ResolveTriggerUncertainAsync(
                sourceSystem.Name!, requestId, body.SendId, body.Resolution, ct);
        }
        catch (MessageOutboxConflictException)
        {
            return NotFound(new
            {
                error = "No recipient was found for this request and sendId under this source."
            });
        }
        catch (InvalidOperationException ex)
        {
            return Conflict(new { error = ex.Message });
        }

        var response = await _outbox.GetRequestAsync(sourceSystem.Name!, requestId, ct);
        return Ok(response);
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

    private (string? Name, IActionResult? Result) GetSourceSystem()
    {
        var clientId = GetPreferredUsername();
        if (clientId is null)
        {
            return (null, Unauthorized(new
            {
                error = "preferred_username claim is missing from token."
            }));
        }

        if (!_sourceSystemOptions.Names.TryGetValue(clientId, out var sourceSystem))
        {
            return (null, StatusCode(StatusCodes.Status403Forbidden, new
            {
                error = $"Client '{clientId}' is not registered in SourceSystemNames.",
                clientId
            }));
        }

        if (!_sourceSystemOptions.AllowsService(sourceSystem, User))
        {
            return (null, StatusCode(StatusCodes.Status403Forbidden, new
            {
                code = "SOURCE_SERVICE_IDENTITY_DENIED",
                error = "The token is not authorized for this source."
            }));
        }

        return (sourceSystem, null);
    }
}
