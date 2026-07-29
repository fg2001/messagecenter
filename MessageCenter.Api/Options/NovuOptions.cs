namespace MessageCenter.Api.Options;

public class NovuOptions
{
    public const string Section = "Novu";

    public string BaseUrl { get; set; } = string.Empty;
    public string ApiKey { get; set; } = string.Empty;
    public string DefaultWorkflowId { get; set; } = "system-notification";
    public string InAppChannel { get; set; } = "in_app";
    public int TimeoutSeconds { get; set; } = 10;
    public int WorkerTimeoutSeconds { get; set; } = 30;
}
