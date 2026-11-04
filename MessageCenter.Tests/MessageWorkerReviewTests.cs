using System.Net;
using System.Reflection;
using System.Text;
using System.Text.Json;
using MessageCenter.Api.Data;
using MessageCenter.Api.HttpClients;
using MessageCenter.Api.HttpClients.Dtos;
using MessageCenter.Api.Options;
using MessageCenter.Api.Outbox;
using MessageCenter.Api.Services;
using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using StackExchange.Redis;
using Xunit;

namespace MessageCenter.Tests;

public sealed class MessageWorkerReviewTests
{
    [Theory]
    [InlineData(null)]
    [InlineData("")]
    public async Task Visibility_requires_a_nonempty_original_key_before_matching_a_message(string? transaction)
    {
        var handler = new Handler(request => Json(new
        {
            data = request.RequestUri!.Query.Contains("page=0&")
                ? new object[] { new { id = "unrelated", transactionId = transaction } }
                : new object[] { new { id = "original", payload = new { sendId = "send" } } },
            hasMore = request.RequestUri.Query.Contains("page=0&")
        }));
        var worker = new MessageOutboxWorker(null!, Options.Create(new MessageWorkerOptions { OutboxVisibilityPages = 2 }),
            Options.Create(new NovuOptions()), NullLogger<MessageOutboxWorker>.Instance);
        var item = new ClaimedMessageOutboxItem("send", "person", "fixture", "{}", "accepted", "ACTIVE", 0, transaction, "fixture");
        var method = typeof(MessageOutboxWorker).GetMethod("FindMessageAsync", BindingFlags.NonPublic | BindingFlags.Instance)!;
        var result = await (Task<NovuMessageItem?>)method.Invoke(worker, new object[] { Client(handler), item, CancellationToken.None })!;
        Assert.Equal("original", result!.Id);
        Assert.Equal(2, handler.Requests.Count);
    }

    [Fact]
    public async Task Terminal_scan_persists_progress_without_deleting_or_confirming_a_partial_feed()
    {
        var connectionString = Environment.GetEnvironmentVariable("MESSAGE_CENTER_TEST_REDIS")
            ?? throw new InvalidOperationException("MESSAGE_CENTER_TEST_REDIS must point to a disposable Redis instance.");
        using var redis = await ConnectionMultiplexer.ConnectAsync(connectionString);
        await using var sql = new SqliteConnection("Data Source=:memory:");
        await sql.OpenAsync();
        var messages = Enumerable.Range(0, 101).Select(i => new
        {
            id = "message-" + i, transactionId = i is 0 or 100 ? "send" : "other"
        }).ToList();
        var handler = new Handler(request =>
        {
            if (request.Method == HttpMethod.Delete)
            {
                messages.RemoveAll(m => request.RequestUri!.AbsolutePath.EndsWith("/" + m.id));
                return new HttpResponseMessage(HttpStatusCode.NoContent);
            }
            var query = request.RequestUri!.Query;
            var page = int.Parse(query.TrimStart('?').Split('&').Single(p => p.StartsWith("page=")).Split('=')[1]);
            return Json(new { data = messages.Skip(page * 100).Take(100), hasMore = (page + 1) * 100 < messages.Count });
        });
        var services = new ServiceCollection();
        services.AddDbContext<MessageCenterDbContext>(o => o.UseSqlite(sql));
        services.AddScoped<MessageTerminalSyncStore>();
        services.AddSingleton(Client(handler));
        services.AddSingleton<IConnectionMultiplexer>(redis);
        services.AddSingleton<IOptions<RedisOptions>>(Options.Create(new RedisOptions
            { Database = 7, KeyPrefix = "terminal-review-" + Guid.NewGuid().ToString("N") }));
        services.AddSingleton<MessageProjectionStore>();
        await using var provider = services.BuildServiceProvider();
        using (var scope = provider.CreateScope())
        {
            var db = scope.ServiceProvider.GetRequiredService<MessageCenterDbContext>();
            await db.Database.EnsureCreatedAsync();
            db.MessageOutbox.Add(new() { SendId = "send", SourceSystem = "fixture", RequestId = "request",
                RequestHash = "hash", SubscriberId = "person", WorkflowId = "fixture", Payload = "{}",
                Status = "visible", BusinessState = MessageBusinessState.Revoked, BusinessVersion = 1, TriggerAttempts = 1 });
            db.MessageTerminalSync.Add(new() { SendId = "send", BusinessVersion = 1, LockedBy = "owner",
                LockedUntil = DateTime.UtcNow.AddMinutes(5), NextAttemptAt = DateTime.UtcNow });
            await db.SaveChangesAsync();
        }
        var worker = new MessageTerminalSyncWorker(provider.GetRequiredService<IServiceScopeFactory>(),
            Options.Create(new MessageWorkerOptions { OutboxVisibilityPages = 1 }), NullLogger<MessageTerminalSyncWorker>.Instance);
        var method = typeof(MessageTerminalSyncWorker).GetMethod("ProcessAsync", BindingFlags.NonPublic | BindingFlags.Instance)!;
        for (var pass = 0; pass < 2; pass++)
        {
            using var scope = provider.CreateScope();
            var db = scope.ServiceProvider.GetRequiredService<MessageCenterDbContext>();
            var task = await db.MessageTerminalSync.SingleAsync();
            task.LockedBy = "owner";
            task.LockedUntil = DateTime.UtcNow.AddMinutes(5);
            await db.SaveChangesAsync();
            await (Task)method.Invoke(worker, new object[] { task, CancellationToken.None })!;
            db.ChangeTracker.Clear();
            var persisted = await db.MessageTerminalSync.SingleAsync();
            if (pass == 0)
            {
                Assert.All(handler.Requests, r => Assert.StartsWith("GET ", r));
                Assert.Equal(101, messages.Count);
                Assert.Equal(1, persisted.NextPage);
                Assert.Contains("message-0", persisted.MessageIds!);
                Assert.Null(persisted.LastConfirmedAt);
            }
            else
            {
                Assert.Equal(99, messages.Count);
                Assert.Equal(2, handler.Requests.Count(r => r.StartsWith("DELETE ")));
                Assert.NotNull(persisted.LastConfirmedAt);
                Assert.Equal("watching", persisted.State);
            }
        }
        Assert.DoesNotContain(handler.Requests, r => r.StartsWith("POST "));
    }

    private static NovuClient Client(HttpMessageHandler handler) => new(new HttpClient(handler) { BaseAddress = new("http://fixture.test") });
    private static HttpResponseMessage Json(object body) => new(HttpStatusCode.OK)
        { Content = new StringContent(JsonSerializer.Serialize(body), Encoding.UTF8, "application/json") };
    private sealed class Handler : HttpMessageHandler
    {
        private readonly Func<HttpRequestMessage, HttpResponseMessage> _reply;
        public List<string> Requests { get; } = new();
        public Handler(Func<HttpRequestMessage, HttpResponseMessage> reply) => _reply = reply;
        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken ct)
        { Requests.Add(request.Method + " " + request.RequestUri); return Task.FromResult(_reply(request)); }
    }
}
