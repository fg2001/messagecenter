using System.Net;
using System.Text;
using System.Text.Json;
using MessageCenter.Api.HttpClients;
using MessageCenter.Api.Models;
using MessageCenter.Api.Services;
using Xunit;

namespace MessageCenter.Tests;

public sealed class ReliableSendContractTests
{
    [Fact]
    public void Mapper_deduplicates_receivers_and_preserves_request_id()
    {
        var request = new SendMessageRequest
        {
            RequestId = "request-1",
            SourceSystem = "flow",
            BusinessType = "process_task",
            Title = "Task",
            Receivers = new List<Receiver>
            {
                new() { Type = "user", Id = "196045" },
                new() { Type = "USER", Id = "196045" }
            }
        };

        var triggers = MessageMapper.Map(request, "workflow-1", out var skipped);

        var trigger = Assert.Single(triggers);
        Assert.Empty(skipped);
        Assert.Equal("request-1", trigger.Payload.RequestId);
        Assert.Equal("196045", trigger.SubscriberId);
    }

    [Fact]
    public async Task Trigger_uses_stable_transaction_and_idempotency_key()
    {
        var handler = new RecordingHandler();
        var client = new NovuClient(new HttpClient(handler)
        {
            BaseAddress = new Uri("http://novu.test")
        });

        var result = await client.TriggerAsync(
            "workflow-1",
            "196045",
            new { title = "Task" },
            "send-1");

        Assert.True(result.Acknowledged);
        Assert.Equal("send-1", result.TransactionId);
        Assert.Equal("send-1", handler.IdempotencyKey);
        using var body = JsonDocument.Parse(handler.Body!);
        Assert.Equal(
            "send-1",
            body.RootElement.GetProperty("transactionId").GetString());
    }

    private sealed class RecordingHandler : HttpMessageHandler
    {
        public string? IdempotencyKey { get; private set; }
        public string? Body { get; private set; }

        protected override async Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request,
            CancellationToken cancellationToken)
        {
            IdempotencyKey = request.Headers.GetValues("Idempotency-Key").Single();
            Body = await request.Content!.ReadAsStringAsync(cancellationToken);
            return new HttpResponseMessage(HttpStatusCode.Created)
            {
                Content = new StringContent(
                    "{\"data\":{\"acknowledged\":true,\"status\":\"processed\",\"transactionId\":\"send-1\"}}",
                    Encoding.UTF8,
                    "application/json")
            };
        }
    }
}
