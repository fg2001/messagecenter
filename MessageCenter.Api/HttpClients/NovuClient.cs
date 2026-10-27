using System.Net;
using System.Net.Http.Headers;
using System.Net.Http.Json;
using System.Text.Json;
using MessageCenter.Api.HttpClients.Dtos;

namespace MessageCenter.Api.HttpClients;

public class NovuClient
{
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);

    private readonly HttpClient _http;

    public NovuClient(HttpClient http)
    {
        _http = http;
    }

    public async Task<TriggerResult> TriggerAsync(
        string workflowId,
        string subscriberId,
        object payload,
        string transactionId,
        CancellationToken ct = default)
    {
        var body = new
        {
            name = workflowId,
            to = new { subscriberId },
            payload,
            transactionId
        };

        using var request = new HttpRequestMessage(HttpMethod.Post, "/v1/events/trigger")
        {
            Content = JsonContent.Create(body, options: JsonOptions)
        };
        request.Headers.TryAddWithoutValidation("Idempotency-Key", transactionId);
        HttpResponseMessage response;
        try
        {
            response = await _http.SendAsync(request, ct);
        }
        catch (Exception ex) when (TriggerOutcomeMayBeUnknown(ex, ct))
        {
            // 请求可能已到达 Novu（超时/连接中断/5xx）：结果不确定，由调用方按严格策略处置，不得自动重发。
            throw new NovuTriggerUncertainException(
                $"Novu trigger outcome is uncertain for transaction '{transactionId}'.", ex);
        }

        try
        {
            response.EnsureSuccessStatusCode();
        }
        catch (HttpRequestException ex) when (ex.StatusCode is >= HttpStatusCode.InternalServerError)
        {
            throw new NovuTriggerUncertainException(
                $"Novu returned {ex.StatusCode} for transaction '{transactionId}'; the event may still have been recorded.", ex);
        }

        var envelope = await response.Content.ReadFromJsonAsync<NovuEnvelope<TriggerResult>>(
            JsonOptions,
            ct);

        var result = envelope?.Data ?? throw new InvalidOperationException("Empty trigger response from Novu.");
        if (!string.Equals(result.TransactionId, transactionId, StringComparison.Ordinal))
        {
            // 2xx 已返回但回执事务不符：Novu 可能已处理该事件，结果同样不确定。
            throw new NovuTriggerReceiptMismatchException("Novu trigger transaction mismatch.");
        }

        return result;
    }

    private static bool TriggerOutcomeMayBeUnknown(Exception ex, CancellationToken ct)
        => !ct.IsCancellationRequested
           && (ex is TaskCanceledException
               || (ex is HttpRequestException { StatusCode: null or >= HttpStatusCode.InternalServerError }));

    public async Task<FeedResult> GetFeedAsync(
        int page,
        int limit,
        string? subscriberId = null,
        CancellationToken ct = default)
    {
        var url = $"/v1/messages?page={page}&limit={limit}&pageSize={limit}";
        if (!string.IsNullOrWhiteSpace(subscriberId))
        {
            url += $"&subscriberId={Uri.EscapeDataString(subscriberId)}";
        }

        var response = await _http.GetAsync(url, ct);
        if (response.StatusCode == HttpStatusCode.NotFound)
        {
            return new FeedResult
            {
                Page = page,
                PageSize = limit
            };
        }

        response.EnsureSuccessStatusCode();

        var envelope = await response.Content.ReadFromJsonAsync<NovuEnvelope<List<NovuMessageItem>>>(
            JsonOptions,
            ct);

        return new FeedResult
        {
            Data = envelope?.Data ?? new List<NovuMessageItem>(),
            HasMore = envelope?.HasMore ?? false,
            TotalCount = envelope?.TotalCount ?? 0,
            PageSize = envelope?.PageSize ?? 0,
            Page = envelope?.Page ?? page
        };
    }

    public async Task MarkAsAsync(
        string subscriberId,
        string messageId,
        bool read,
        CancellationToken ct = default)
    {
        var body = new
        {
            messageId,
            markAs = read ? "read" : "unread"
        };

        var url = $"/v1/subscribers/{Uri.EscapeDataString(subscriberId)}/messages/mark-as";
        var response = await _http.PostAsJsonAsync(url, body, JsonOptions, ct);
        response.EnsureSuccessStatusCode();
    }

    private class NovuEnvelope<T>
    {
        public T? Data { get; set; }
        public int TotalCount { get; set; }
        public int PageSize { get; set; }
        public int Page { get; set; }
        public bool HasMore { get; set; }
    }
}
