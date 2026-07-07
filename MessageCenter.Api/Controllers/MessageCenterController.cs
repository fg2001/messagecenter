using System.Security.Claims;
using MessageCenter.Api.Audit;
using MessageCenter.Api.HttpClients;
using MessageCenter.Api.HttpClients.Dtos;
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
public class MessageCenterController : ControllerBase
{
    private const int FeedPageSize = 100;

    private readonly NovuClient _novu;
    private readonly NovuOptions _options;
    private readonly SourceSystemOptions _sourceSystemOptions;
    private readonly IAuditSink _audit;

    public MessageCenterController(
        NovuClient novu,
        IOptions<NovuOptions> options,
        IOptions<SourceSystemOptions> sourceSystemOptions,
        IAuditSink audit)
    {
        _novu = novu;
        _options = options.Value;
        _sourceSystemOptions = sourceSystemOptions.Value;
        _audit = audit;
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
                error = $"Client '{clientId}' is not registered in SourceSystemNames. Contact the platform team to register this client before sending messages.",
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

        var response = new SendMessageResponse
        {
            TransactionId = transactionId,
            Status = status,
            Acknowledged = acknowledged,
            Accepted = accepted,
            Skipped = skipped
        };

        return StatusCode(StatusCodes.Status201Created, response);
    }

    [HttpGet("my")]
    public async Task<IActionResult> GetMyMessages(
        [FromQuery] int page = 1,
        [FromQuery] int limit = 100,
        [FromQuery] bool? read = null,
        CancellationToken ct = default)
    {
        if (page < 1)
        {
            return BadRequest(new { error = "page must be greater than or equal to 1." });
        }

        if (limit < 1)
        {
            return BadRequest(new { error = "limit must be greater than or equal to 1." });
        }

        var subscriberId = GetPreferredUsername();
        if (subscriberId is null)
        {
            return Unauthorized(new { error = "preferred_username claim is missing from token." });
        }

        List<NovuMessageItem> sourceMessages;
        if (read.HasValue)
        {
            sourceMessages = (await GetAllFeedMessagesAsync(subscriberId, ct))
                .Where(message => message.Read == read.Value)
                .Skip((page - 1) * limit)
                .Take(limit)
                .ToList();
        }
        else
        {
            var novuPage = page - 1;
            var feed = await _novu.GetFeedAsync(novuPage, limit, subscriberId, ct);
            sourceMessages = feed.Data;
        }

        var messages = MapMessages(sourceMessages);

        return Ok(messages);
    }

    [HttpGet("unread-count")]
    public async Task<IActionResult> GetUnreadCount(CancellationToken ct)
    {
        var subscriberId = GetPreferredUsername();
        if (subscriberId is null)
        {
            return Unauthorized(new { error = "preferred_username claim is missing from token." });
        }

        var messages = await GetAllFeedMessagesAsync(subscriberId, ct);
        var unreadCount = messages.Count(message => !message.Read);

        return Ok(new { unreadCount });
    }

    [HttpPost("messages/{messageId}/read")]
    public async Task<IActionResult> MarkRead(string messageId, CancellationToken ct)
        => await MarkAs(messageId, read: true, ct);

    [HttpPost("messages/{messageId}/unread")]
    public async Task<IActionResult> MarkUnread(string messageId, CancellationToken ct)
        => await MarkAs(messageId, read: false, ct);

    [HttpPost("my/read-all")]
    public async Task<IActionResult> MarkAllMyMessagesRead(CancellationToken ct)
    {
        var subscriberId = GetPreferredUsername();
        if (subscriberId is null)
        {
            return Unauthorized(new { error = "preferred_username claim is missing from token." });
        }

        var messages = await GetAllFeedMessagesAsync(subscriberId, ct);
        var unreadMessages = messages
            .Where(message => !message.Read && !string.IsNullOrWhiteSpace(message.Id))
            .ToList();

        foreach (var message in unreadMessages)
        {
            await _novu.MarkAsAsync(subscriberId, message.Id!, read: true, ct);
        }

        var refreshedMessages = await GetAllFeedMessagesAsync(subscriberId, ct);
        var unreadCount = refreshedMessages.Count(message => !message.Read);

        return Ok(new
        {
            markedCount = unreadMessages.Count,
            unreadCount
        });
    }

    private async Task<IActionResult> MarkAs(string messageId, bool read, CancellationToken ct)
    {
        var subscriberId = GetPreferredUsername();
        if (subscriberId is null)
        {
            return Unauthorized(new { error = "preferred_username claim is missing from token." });
        }

        await _novu.MarkAsAsync(subscriberId, messageId, read, ct);

        var messages = await GetAllFeedMessagesAsync(subscriberId, ct);
        var unreadCount = messages.Count(message => !message.Read);
        var updated = messages.FirstOrDefault(message => message.Id == messageId);

        return Ok(new
        {
            messageId,
            read = updated?.Read ?? read,
            unreadCount
        });
    }

    private string? GetPreferredUsername()
    {
        var username = User.FindFirstValue("preferred_username");
        return string.IsNullOrWhiteSpace(username) ? null : username;
    }

    private async Task<List<NovuMessageItem>> GetAllFeedMessagesAsync(string subscriberId, CancellationToken ct)
    {
        var page = 0;
        var messages = new List<NovuMessageItem>();

        while (true)
        {
            var feed = await _novu.GetFeedAsync(page, FeedPageSize, subscriberId, ct);
            messages.AddRange(feed.Data);

            if (!feed.HasMore || feed.Data.Count == 0)
            {
                return messages;
            }

            page++;
        }
    }

    private static List<object> MapMessages(IEnumerable<NovuMessageItem> messages)
        => messages.Select(message => new
        {
            messageId = message.Id,
            sourceSystem = message.Payload?.SourceSystem,
            businessType = message.Payload?.BusinessType,
            businessId = message.Payload?.BusinessId,
            title = message.Subject,
            content = message.Content,
            url = message.Cta?.Data?.Url,
            read = message.Read,
            seen = message.Seen,
            createdAt = message.CreatedAt
        }).Cast<object>().ToList();
}
