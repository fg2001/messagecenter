using System.Security.Claims;
using MessageCenter.Api.HttpClients;
using MessageCenter.Api.Models;
using MessageCenter.Api.Options;
using MessageCenter.Api.Outbox;
using MessageCenter.Api.Services;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Options;

namespace MessageCenter.Api.Controllers;

[ApiController]
[Route("api/message-center")]
[Authorize]
public sealed class MessageCenterController : ControllerBase
{
    private static readonly HashSet<string> MessageViews =
        new(StringComparer.OrdinalIgnoreCase) { "all", "unread", "read" };

    private static readonly HashSet<string> LifecycleActions =
        new(StringComparer.Ordinal) { "CONSUME", "REVOKE", "EXPIRE" };

    private readonly NovuClient _novu;
    private readonly NovuOptions _options;
    private readonly SourceSystemOptions _sourceSystemOptions;
    private readonly MessageOutboxStore _outbox;
    private readonly MessageLifecycleStore _lifecycle;
    private readonly MessageProjectionStore _projection;
    private readonly MessageWorkQueue _jobs;

    public MessageCenterController(
        NovuClient novu,
        IOptions<NovuOptions> options,
        IOptions<SourceSystemOptions> sourceSystemOptions,
        MessageOutboxStore outbox,
        MessageLifecycleStore lifecycle,
        MessageProjectionStore projection,
        MessageWorkQueue jobs)
    {
        _novu = novu;
        _options = options.Value;
        _sourceSystemOptions = sourceSystemOptions.Value;
        _outbox = outbox;
        _lifecycle = lifecycle;
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
        [FromQuery] string? include = null,
        CancellationToken ct = default)
    {
        var sourceSystem = GetSourceSystem();
        if (sourceSystem.Result is not null) return sourceSystem.Result;

        // include=terminal：收件人条目附带业务状态（businessState/businessVersion/terminalAt/terminalReason）。
        var includeTerminal = string.Equals(include, "terminal", StringComparison.OrdinalIgnoreCase);
        var response = await _outbox.GetRequestAsync(
            sourceSystem.Name!, requestId, ct, includeTerminal);
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
        catch (DbUpdateConcurrencyException)
        {
            return StatusCode(StatusCodes.Status503ServiceUnavailable,
                new { code = "MESSAGE_CONCURRENT_UPDATE", error = "State changed; retry reconciliation with the original request." });
        }

        var response = await _outbox.GetRequestAsync(sourceSystem.Name!, requestId, ct);
        return Ok(response);
    }

    /// <summary>T-INT-05 生命周期操作（T-INT-01 §6）：CONSUME/REVOKE/EXPIRE 按原键作用于收件人条目。
    /// 同 operationId 同载荷重放返回原结果；异载荷 409；businessVersion ≤ 当前 422；
    /// REVOKE/EXPIRE 允许在 send 到达前按原键暂存（deferred），迟到的 send 受理即套用终态且不生成有效通知。</summary>
    [HttpPost("send/{requestId}/recipients/{subscriberId}/lifecycle")]
    public async Task<IActionResult> ApplyLifecycle(
        string requestId,
        string subscriberId,
        [FromBody] LifecycleRequest? body,
        CancellationToken ct)
    {
        var source = GetSourceSystem();
        if (source.Result is not null) return source.Result;
        var sourceSystem = source.Name!;

        if (string.IsNullOrWhiteSpace(subscriberId))
        {
            return BadRequest(new { error = "subscriberId is required." });
        }
        if (body is null || string.IsNullOrWhiteSpace(body.OperationId) || body.OperationId.Length > 128)
        {
            return BadRequest(new
            {
                error = "operationId is required (max 128 characters)."
            });
        }
        if (!LifecycleActions.Contains(body.Action))
        {
            return BadRequest(new
            {
                error = "action must be one of: CONSUME, REVOKE, EXPIRE."
            });
        }
        if (body.BusinessVersion < 1)
        {
            return BadRequest(new { error = "businessVersion must be a positive integer." });
        }
        if (body.Reason?.Length > 2000)
        {
            return BadRequest(new { error = "reason must be at most 2000 characters." });
        }

        LifecycleApplyResult result;
        try
        {
            result = await _lifecycle.ApplyAsync(
                new LifecycleApplyInput(
                    sourceSystem,
                    requestId,
                    subscriberId,
                    body.OperationId,
                    body.Action,
                    body.BusinessVersion,
                    body.Reason,
                    body.OccurredAt),
                ct);
        }
        catch (MessageLifecycleConflictException ex)
        {
            return Conflict(new { error = ex.Message, operationId = body.OperationId });
        }
        catch (MessageLifecycleNotFoundException ex)
        {
            return NotFound(new { error = ex.Message });
        }
        catch (MessageLifecycleVersionRejectException ex)
        {
            return UnprocessableEntity(new
            {
                error = ex.Message,
                attemptedVersion = ex.AttemptedVersion,
                currentVersion = ex.CurrentVersion
            });
        }
        catch (DbUpdateConcurrencyException)
        {
            return StatusCode(StatusCodes.Status503ServiceUnavailable,
                new { code = "MESSAGE_CONCURRENT_UPDATE", error = "State changed; retry the original lifecycle operation." });
        }

        // 终态与持久任务已共同提交；请求不等待外部渠道，失败由Worker按原键继续补偿。
        var external = result.SendId is null ? null
            : await _lifecycle.GetExternalSyncAsync(result.SendId, result.BusinessVersion, ct);

        return Accepted(new LifecycleResponse
        {
            OperationId = result.OperationId,
            RequestId = result.RequestId,
            SubscriberId = result.SubscriberId,
            Action = result.Action,
            BusinessVersion = result.BusinessVersion,
            Status = result.Replayed ? "replayed" : result.ApplyStatus,
            BusinessState = result.ResultBusinessState,
            SendId = result.SendId,
            OccurredAt = result.OccurredAt,
            RecordedAt = result.RecordedAt,
            ExternalState = ExternalState(result.ApplyStatus, result.SendId, external)
        });
    }

    /// <summary>生命周期操作回读：同来源令牌按 requestId + operationId 查询操作记录。</summary>
    [HttpGet("send/{requestId}/operations/{operationId}")]
    public async Task<IActionResult> GetLifecycleOperation(
        string requestId,
        string operationId,
        CancellationToken ct)
    {
        var source = GetSourceSystem();
        if (source.Result is not null) return source.Result;

        var operation = await _lifecycle.GetOperationAsync(
            source.Name!, requestId, operationId, ct);
        var external = operation?.SendId is null ? null
            : await _lifecycle.GetExternalSyncAsync(operation.SendId, operation.BusinessVersion, ct);
        return operation is null
            ? NotFound()
            : Ok(new LifecycleOperationResponse
            {
                OperationId = operation.OperationId,
                RequestId = operation.RequestId,
                SubscriberId = operation.SubscriberId,
                SendId = operation.SendId,
                Action = operation.Action,
                BusinessVersion = operation.BusinessVersion,
                Reason = operation.Reason,
                ApplyStatus = operation.ApplyStatus,
                ResultBusinessState = operation.ResultBusinessState,
                OccurredAt = operation.OccurredAt,
                RecordedAt = operation.RecordedAt,
                AppliedAt = operation.AppliedAt,
                ExternalState = ExternalState(operation.ApplyStatus, operation.SendId, external),
                LastExternalConfirmedAt = external?.LastConfirmedAt,
                ExternalError = external?.LastError
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

    private static string ExternalState(string applyStatus, string? sendId, MessageTerminalSyncEntity? sync)
        => applyStatus == MessageLifecycleApplyStatus.Superseded ? "superseded"
            : sync?.State ?? (sendId is null ? "waiting-for-send" : "not-tracked");

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
