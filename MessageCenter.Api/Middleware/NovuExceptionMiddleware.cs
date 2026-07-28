using System.Text.Json;

namespace MessageCenter.Api.Middleware;

public class NovuExceptionMiddleware
{
    private readonly RequestDelegate _next;
    private readonly ILogger<NovuExceptionMiddleware> _logger;

    public NovuExceptionMiddleware(RequestDelegate next, ILogger<NovuExceptionMiddleware> logger)
    {
        _next = next;
        _logger = logger;
    }

    public async Task InvokeAsync(HttpContext context)
    {
        try
        {
            await _next(context);
        }
        catch (HttpRequestException ex)
        {
            var novuStatus = (int?)ex.StatusCode ?? 0;

            _logger.LogError(
                ex,
                "Novu request failed. NovuStatus={NovuStatus} Path={Path}",
                novuStatus,
                context.Request.Path);

            await WriteJson(context, StatusCodes.Status502BadGateway, new
            {
                error = "Novu request failed.",
                novuStatus
            });
        }
        catch (TaskCanceledException ex) when (!context.RequestAborted.IsCancellationRequested)
        {
            _logger.LogError(
                ex,
                "Novu request timed out. Path={Path}",
                context.Request.Path);

            await WriteJson(context, StatusCodes.Status504GatewayTimeout, new
            {
                error = "Novu request timed out."
            });
        }
        catch (OperationCanceledException) when (context.RequestAborted.IsCancellationRequested)
        {
            _logger.LogDebug(
                "MessageCenter request was canceled by the client. Path={Path}",
                context.Request.Path);
        }
    }

    private static async Task WriteJson(HttpContext context, int statusCode, object body)
    {
        context.Response.StatusCode = statusCode;
        context.Response.ContentType = "application/json";
        await context.Response.WriteAsync(JsonSerializer.Serialize(body));
    }
}
