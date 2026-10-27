using System.Net;
using System.Text;
using System.Text.Json;
using MessageCenter.Api.HttpClients;
using Xunit;

namespace MessageCenter.Tests;

public sealed class TriggerReceiptTests
{
    [Theory]
    [InlineData("other-send")]
    [InlineData("SEND-1")]
    [InlineData(" send-1")]
    [InlineData("send-1 ")]
    [InlineData("")]
    [InlineData(" ")]
    [InlineData(null)]
    public async Task Trigger_rejects_receipt_for_another_transaction(string? returnedId)
    {
        var handler = new ReceiptHandler(returnedId);
        var client = Client(handler);

        var error = await Assert.ThrowsAnyAsync<InvalidOperationException>(() =>
            client.TriggerAsync("workflow-1", "subscriber-1", new { title = "Task" }, "send-1"));

        Assert.Equal("Novu trigger transaction mismatch.", error.Message);
        Assert.Equal(new[] { "send-1" }, handler.Keys);
    }

    [Fact]
    public async Task Trigger_preserves_original_key_and_payload_after_mismatched_receipt()
    {
        var handler = new ReceiptHandler("other-send", "send-1");
        var client = Client(handler);
        var payload = new { title = "Task", sendId = "send-1" };

        await Assert.ThrowsAnyAsync<InvalidOperationException>(() =>
            client.TriggerAsync("workflow-1", "subscriber-1", payload, "send-1"));
        var result = await client.TriggerAsync("workflow-1", "subscriber-1", payload, "send-1");

        Assert.True(result.Acknowledged);
        Assert.Equal("send-1", result.TransactionId);
        Assert.Equal(new[] { "send-1", "send-1" }, handler.Keys);
        Assert.Equal(handler.Bodies[0], handler.Bodies[1]);
        using var body = JsonDocument.Parse(handler.Bodies[0]);
        Assert.Equal("send-1", body.RootElement.GetProperty("transactionId").GetString());
    }

    private static NovuClient Client(HttpMessageHandler handler) => new(new HttpClient(handler)
    {
        BaseAddress = new Uri("http://novu.test")
    });

    private sealed class ReceiptHandler : HttpMessageHandler
    {
        private readonly Queue<string?> _returnedIds;
        public List<string> Keys { get; } = new();
        public List<string> Bodies { get; } = new();

        public ReceiptHandler(params string?[] returnedIds) => _returnedIds = new(returnedIds);

        protected override async Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request, CancellationToken cancellationToken)
        {
            Keys.Add(request.Headers.GetValues("Idempotency-Key").Single());
            Bodies.Add(await request.Content!.ReadAsStringAsync(cancellationToken));
            return new HttpResponseMessage(HttpStatusCode.Created)
            {
                Content = new StringContent(JsonSerializer.Serialize(new
                {
                    data = new { acknowledged = true, status = "processed", transactionId = _returnedIds.Dequeue() }
                }), Encoding.UTF8, "application/json")
            };
        }
    }
}
