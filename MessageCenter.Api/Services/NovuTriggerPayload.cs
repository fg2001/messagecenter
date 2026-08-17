namespace MessageCenter.Api.Services;

public class NovuTriggerPayload
{
    public string WorkflowId { get; init; } = string.Empty;
    public string SubscriberId { get; init; } = string.Empty;
    public NovuPayloadFields Payload { get; init; } = new();
}

public class NovuPayloadFields
{
    public string RequestId { get; init; } = string.Empty;
    public string SendId { get; init; } = string.Empty;
    public string SourceSystem { get; init; } = string.Empty;
    public string BusinessType { get; init; } = string.Empty;
    public string? BusinessId { get; init; }
    public string Title { get; init; } = string.Empty;
    public string? Content { get; init; }
    public string? Url { get; init; }
}
