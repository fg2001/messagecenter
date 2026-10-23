using MessageCenter.Api.Data;
using MessageCenter.Api.HttpClients;
using MessageCenter.Api.Middleware;
using MessageCenter.Api.Options;
using MessageCenter.Api.Outbox;
using MessageCenter.Api.Services;
using Microsoft.AspNetCore.Authentication.JwtBearer;
using Microsoft.EntityFrameworkCore;
using System.Net.Http.Headers;
using Microsoft.Extensions.Options;
using Microsoft.IdentityModel.Tokens;
using StackExchange.Redis;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddControllers();
builder.Services.Configure<NovuOptions>(
    builder.Configuration.GetSection(NovuOptions.Section));
builder.Services.Configure<SourceSystemOptions>(options => options.Bind(builder.Configuration));
builder.Services.Configure<RedisOptions>(
    builder.Configuration.GetSection(RedisOptions.Section));
var messageCenterConnectionString =
    builder.Configuration.GetConnectionString("MessageCenter");
builder.Services.AddDbContext<MessageCenterDbContext>(options =>
    options.UseDm(messageCenterConnectionString));
builder.Services.AddHttpClient<NovuClient>((sp, client) =>
{
    var novuOptions = sp.GetRequiredService<IOptions<NovuOptions>>().Value;

    client.BaseAddress = new Uri(novuOptions.BaseUrl, UriKind.Absolute);
    client.Timeout = TimeSpan.FromSeconds(novuOptions.TimeoutSeconds);
    client.DefaultRequestHeaders.Authorization =
        new AuthenticationHeaderValue("ApiKey", novuOptions.ApiKey);
});
// Polly extension point: add .AddPolicyHandler(...) to the NovuClient IHttpClientBuilder when retries are introduced.
builder.Services.AddScoped<MessageOutboxStore>();
builder.Services.AddSingleton<IConnectionMultiplexer>(sp =>
{
    var options = sp.GetRequiredService<IOptions<RedisOptions>>().Value;
    return ConnectionMultiplexer.Connect(options.ConnectionString);
});
builder.Services.AddSingleton<MessageProjectionStore>();
builder.Services.AddSingleton<MessageWorkQueue>();
builder.Services
    .AddAuthentication(JwtBearerDefaults.AuthenticationScheme)
    .AddJwtBearer(options =>
    {
        options.Authority = builder.Configuration["Jwt:Authority"]
            ?? throw new InvalidOperationException("Jwt:Authority is not configured.");
        options.RequireHttpsMetadata = builder.Configuration.GetValue<bool>("Jwt:RequireHttpsMetadata");
        options.MapInboundClaims = false;
        options.TokenValidationParameters = new TokenValidationParameters
        {
            ValidateIssuer = true,
            ValidateAudience = false,
            ValidateLifetime = true,
            RequireSignedTokens = true,
            ClockSkew = TimeSpan.FromMinutes(1)
        };
    });
builder.Services.AddAuthorization();

var app = builder.Build();

if (string.IsNullOrWhiteSpace(messageCenterConnectionString))
{
    throw new InvalidOperationException(
        "ConnectionStrings:MessageCenter is required.");
}

var novu = app.Services.GetRequiredService<IOptions<NovuOptions>>().Value;
if (string.IsNullOrWhiteSpace(novu.BaseUrl))
{
    throw new InvalidOperationException("Novu:BaseUrl is required.");
}

if (string.IsNullOrWhiteSpace(novu.ApiKey))
{
    throw new InvalidOperationException("Novu:ApiKey is required.");
}

var sourceSystemOptions = app.Services.GetRequiredService<IOptions<SourceSystemOptions>>().Value;
if (sourceSystemOptions.Names.Count == 0)
{
    throw new InvalidOperationException("SourceSystemNames is empty. At least one client mapping is required.");
}

var redis = app.Services.GetRequiredService<IOptions<RedisOptions>>().Value;
if (string.IsNullOrWhiteSpace(redis.ConnectionString))
{
    throw new InvalidOperationException("Redis:ConnectionString is required.");
}

app.UseMiddleware<NovuExceptionMiddleware>();
app.UseAuthentication();
app.UseAuthorization();

app.MapGet("/health", () => Results.Ok(new { status = "ok" }));
app.MapGet("/health/outbox", async (MessageOutboxStore outbox, CancellationToken ct) =>
    await outbox.CanConnectAsync(ct)
        ? Results.Ok(new { status = "ok" })
        : Results.Json(
            new { status = "unavailable" },
            statusCode: StatusCodes.Status503ServiceUnavailable));
app.MapGet("/health/worker", async (MessageWorkQueue queue) =>
{
    var heartbeat = await queue.GetHeartbeatAsync();
    return heartbeat is null
        ? Results.Json(
            new { status = "unavailable" },
            statusCode: StatusCodes.Status503ServiceUnavailable)
        : Results.Ok(new
        {
            status = "ok",
            worker = heartbeat.Worker,
            at = heartbeat.At
        });
});
app.MapControllers();

app.Run();
