using System.Net.Http.Headers;
using MessageCenter.Api.HttpClients;
using MessageCenter.Api.Data;
using MessageCenter.Api.Options;
using MessageCenter.Api.Outbox;
using MessageCenter.Api.Services;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using StackExchange.Redis;

var host = Host.CreateDefaultBuilder(args)
    .UseContentRoot(AppContext.BaseDirectory)
    .ConfigureServices((context, services) =>
    {
        services.Configure<NovuOptions>(
            context.Configuration.GetSection(NovuOptions.Section));
        services.Configure<RedisOptions>(
            context.Configuration.GetSection(RedisOptions.Section));
        services.Configure<MessageWorkerOptions>(
            context.Configuration.GetSection(MessageWorkerOptions.Section));
        var messageCenterConnectionString =
            context.Configuration.GetConnectionString("MessageCenter");
        services.AddDbContext<MessageCenterDbContext>(options =>
            options.UseDm(messageCenterConnectionString));
        services.AddScoped<MessageOutboxStore>();
        services.AddScoped<MessageTerminalSyncStore>();
        services.AddScoped<MessageProjectionAuthority>();
        services.AddHttpClient<NovuClient>((sp, client) =>
        {
            var options = sp.GetRequiredService<IOptions<NovuOptions>>().Value;
            client.BaseAddress = new Uri(options.BaseUrl, UriKind.Absolute);
            client.Timeout = TimeSpan.FromSeconds(options.WorkerTimeoutSeconds);
            client.DefaultRequestHeaders.Authorization =
                new AuthenticationHeaderValue("ApiKey", options.ApiKey);
        });
        services.AddSingleton<IConnectionMultiplexer>(sp =>
        {
            var options = sp.GetRequiredService<IOptions<RedisOptions>>().Value;
            return ConnectionMultiplexer.Connect(options.ConnectionString);
        });
        services.AddSingleton<MessageProjectionStore>();
        services.AddSingleton<MessageWorkQueue>();
        services.AddHostedService<MessageBackgroundWorker>();
        services.AddHostedService<MessageOutboxWorker>();
        services.AddHostedService<MessageTerminalSyncWorker>();
    })
    .Build();

var novu = host.Services.GetRequiredService<IOptions<NovuOptions>>().Value;
if (string.IsNullOrWhiteSpace(novu.BaseUrl))
{
    throw new InvalidOperationException("Novu:BaseUrl is required.");
}

var databaseConnectionString = host.Services
    .GetRequiredService<IConfiguration>()
    .GetConnectionString("MessageCenter");
if (string.IsNullOrWhiteSpace(databaseConnectionString))
{
    throw new InvalidOperationException(
        "ConnectionStrings:MessageCenter is required.");
}
if (string.IsNullOrWhiteSpace(novu.ApiKey))
{
    throw new InvalidOperationException("Novu:ApiKey is required.");
}

var redis = host.Services.GetRequiredService<IOptions<RedisOptions>>().Value;
if (string.IsNullOrWhiteSpace(redis.ConnectionString))
{
    throw new InvalidOperationException("Redis:ConnectionString is required.");
}

await host.RunAsync();
