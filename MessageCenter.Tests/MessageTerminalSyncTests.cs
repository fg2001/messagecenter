using System.Net;
using System.Text;
using System.Text.Json;
using MessageCenter.Api.Data;
using MessageCenter.Api.Controllers;
using MessageCenter.Api.HttpClients;
using MessageCenter.Api.Models;
using MessageCenter.Api.Options;
using MessageCenter.Api.Outbox;
using MessageCenter.Api.Services;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Options;
using System.Security.Claims;
using Xunit;

namespace MessageCenter.Tests;

public sealed class MessageTerminalSyncTests
{
    [Theory]
    [InlineData("superseded", null, "superseded")]
    [InlineData("deferred", null, "waiting-for-send")]
    [InlineData("applied", "historic-send", "not-tracked")]
    public async Task Operation_readback_distinguishes_superseded_deferred_and_missing_historical_task(
        string applyStatus, string? sendId, string externalState)
    {
        await using var db=Db();
        db.MessageLifecycleOperations.Add(new() { SourceSystem="micip",OperationId="op",RequestId="req",SubscriberId="person",
            SendId=sendId,Action="REVOKE",BusinessVersion=1,OperationHash="hash",ApplyStatus=applyStatus });
        await db.SaveChangesAsync();
        var controller=new MessageCenterController(null!,Options.Create(new NovuOptions()),
            Options.Create(new SourceSystemOptions { Names=new() { ["service"]="micip" } }),
            new MessageOutboxStore(db),new MessageLifecycleStore(db),null!,null!) {
            ControllerContext=new ControllerContext { HttpContext=new DefaultHttpContext {
                User=new ClaimsPrincipal(new ClaimsIdentity(new[] {new Claim("preferred_username","service")},"fixture"))
            }}
        };
        var result=Assert.IsType<OkObjectResult>(await controller.GetLifecycleOperation("req","op",default));
        Assert.Equal(externalState,Assert.IsType<LifecycleOperationResponse>(result.Value).ExternalState);
    }

    [Fact]
    public async Task Terminal_and_one_replayed_operation_keep_one_persistent_original_version_task()
    {
        await using var db = Db();
        db.MessageOutbox.Add(Row()); await db.SaveChangesAsync();
        var store = new MessageLifecycleStore(db);
        var input = new LifecycleApplyInput("micip", "req", "person", "op", "REVOKE", 1, null, null);
        await store.ApplyAsync(input, default);
        await store.ApplyAsync(input, default);
        var task = await db.MessageTerminalSync.SingleAsync();
        Assert.Equal("send", task.SendId); Assert.Equal(1, task.BusinessVersion);
        Assert.Equal("pending", task.State); Assert.NotNull(task.NextAttemptAt);
        Assert.Null(task.LastConfirmedAt);
    }

    [Fact]
    public async Task Deferred_marker_schedules_only_after_original_send_arrives_with_highest_version()
    {
        await using var db = Db();
        var lifecycle = new MessageLifecycleStore(db);
        await lifecycle.ApplyAsync(new("micip", "req", "person", "op1", "REVOKE", 1, null, null), default);
        await lifecycle.ApplyAsync(new("micip", "req", "person", "op2", "EXPIRE", 2, null, null), default);
        Assert.Empty(await db.MessageTerminalSync.ToListAsync());
        await new MessageOutboxStore(db).EnqueueAsync("micip", new SendMessageRequest { RequestId = "req" },
            new[] { new NovuTriggerPayload { WorkflowId = "fixture", SubscriberId = "person", Payload = new() } },
            Array.Empty<string>(), default);
        var row = await db.MessageOutbox.SingleAsync(); var task = await db.MessageTerminalSync.SingleAsync();
        Assert.Equal(MessageBusinessState.Expired, row.BusinessState);
        Assert.Equal(row.SendId, task.SendId); Assert.Equal(2, task.BusinessVersion);
        Assert.True(task.NeverTriggered);
        await lifecycle.ApplyAsync(new("micip", "req", "person", "op3", "EXPIRE", 3, null, null), default);
        Assert.True((await db.MessageTerminalSync.SingleAsync(x => x.BusinessVersion == 3)).NeverTriggered);
    }

    [Fact]
    public async Task New_terminal_version_keeps_older_task_auditable()
    {
        await using var db = Db(); db.MessageOutbox.Add(Row()); await db.SaveChangesAsync();
        var store = new MessageLifecycleStore(db);
        await store.ApplyAsync(new("micip", "req", "person", "op1", "CONSUME", 1, null, null), default);
        await store.ApplyAsync(new("micip", "req", "person", "op2", "REVOKE", 2, null, null), default);
        Assert.Equal(new long[] { 1, 2 }, await db.MessageTerminalSync.OrderBy(x => x.BusinessVersion)
            .Select(x => x.BusinessVersion).ToArrayAsync());
    }

    [Fact]
    public async Task Null_transaction_does_not_match_unrelated_message_and_scan_has_no_side_effect()
    {
        var handler = new Handler(_ => Json(new { data = new object[] {
            new { id = "other", transactionId = (string?)null },
            new { id = "target", payload = new { sendId = "send" } }
        }, hasMore = false }));
        var result = await MessageTerminalSyncWorker.FindTargetsAsync(Client(handler), Row(), new(), default);
        Assert.Equal(new[] { "target" }, result.MessageIds); Assert.True(result.Exhausted);
        Assert.All(handler.Requests, x => Assert.StartsWith("GET ", x));
        Assert.Contains("subscriberId=person", handler.Requests.Single());
    }

    [Fact]
    public async Task Bounded_scan_collects_all_matching_targets_and_does_not_claim_exhausted_feed()
    {
        var page = 0;
        var handler = new Handler(_ => Json(new { data = new[] {
            new { id = "target-" + page++, transactionId = "send" }
        }, hasMore = true }));
        var result = await MessageTerminalSyncWorker.FindTargetsAsync(Client(handler), Row(),
            new MessageWorkerOptions { OutboxVisibilityPages = 2 }, default);
        Assert.Equal(2, result.MessageIds.Count); Assert.False(result.Exhausted);
        Assert.Equal(2, result.NextPage);
        Assert.Equal(2, handler.Requests.Count);
    }

    [Fact]
    public async Task Deep_feed_scan_resumes_next_page_and_wraps_only_after_exhausting_feed()
    {
        var handler = new Handler(_ => Json(new { data = new[] { new { id = "deep", transactionId = "send" } }, hasMore = false }));
        var result = await MessageTerminalSyncWorker.FindTargetsAsync(Client(handler), Row(), new(), default, 9);
        Assert.Contains("page=9&", handler.Requests.Single());
        Assert.True(result.Exhausted); Assert.Equal(0, result.NextPage);
        Assert.Equal(new[] { "deep" }, result.MessageIds);
    }

    [Fact]
    public async Task Empty_feed_is_no_target_and_never_an_external_success()
    {
        var handler = new Handler(_ => Json(new { data = Array.Empty<object>(), hasMore = false }));
        var result = await MessageTerminalSyncWorker.FindTargetsAsync(Client(handler), Row(), new(), default);
        Assert.Empty(result.MessageIds); Assert.True(result.Exhausted);
        Assert.Single(handler.Requests);
    }

    [Theory]
    [InlineData("REVOKED", "DELETE")]
    [InlineData("EXPIRED", "DELETE")]
    [InlineData("CONSUMED", "POST")]
    public async Task Original_target_uses_idempotent_terminal_action(string state, string method)
    {
        string? body = null;
        var handler = new Handler(request => {
            body = request.Content?.ReadAsStringAsync().GetAwaiter().GetResult();
            return new HttpResponseMessage(method == "DELETE" ? HttpStatusCode.NotFound : HttpStatusCode.OK);
        });
        var row = Row(); row.BusinessState = state;
        await MessageTerminalSyncWorker.ApplyTargetAsync(Client(handler), row, "message/1", default);
        Assert.StartsWith(method + " ", handler.Requests.Single());
        if (method == "POST") Assert.Contains("\"markAs\":\"read\"", body);
        else Assert.Contains("message%2F1", handler.Requests.Single());
    }

    [Fact]
    public async Task External_failure_propagates_for_durable_retry_instead_of_fabricating_success()
    {
        var handler = new Handler(_ => new HttpResponseMessage(HttpStatusCode.ServiceUnavailable));
        await Assert.ThrowsAsync<HttpRequestException>(() =>
            MessageTerminalSyncWorker.FindTargetsAsync(Client(handler), Row(), new(), default));
    }

    private static MessageCenterDbContext Db() => new(new DbContextOptionsBuilder<MessageCenterDbContext>()
        .UseInMemoryDatabase("terminal-" + Guid.NewGuid())
        .ConfigureWarnings(x => x.Ignore(InMemoryEventId.TransactionIgnoredWarning)).Options);
    private static MessageOutboxEntity Row() => new() {
        SendId = "send", SourceSystem = "micip", RequestId = "req", RequestHash = "hash", SubscriberId = "person",
        Status = MessageOutboxState.Visible, WorkflowId = "fixture", Payload = "{}", CreatedAt = DateTime.UtcNow
    };
    private static NovuClient Client(HttpMessageHandler handler) => new(new HttpClient(handler) { BaseAddress = new("http://fixture.test") });
    private static HttpResponseMessage Json(object body) => new(HttpStatusCode.OK) {
        Content = new StringContent(JsonSerializer.Serialize(body), Encoding.UTF8, "application/json")
    };
    private sealed class Handler : HttpMessageHandler
    {
        private readonly Func<HttpRequestMessage,HttpResponseMessage> _reply;
        public readonly List<string> Requests = new();
        public Handler(Func<HttpRequestMessage,HttpResponseMessage> reply) => _reply = reply;
        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken ct)
        { Requests.Add(request.Method + " " + request.RequestUri!.AbsoluteUri); return Task.FromResult(_reply(request)); }
    }
}
