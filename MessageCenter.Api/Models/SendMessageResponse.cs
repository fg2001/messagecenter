namespace MessageCenter.Api.Models;

public class SendMessageResponse
{
    public string RequestId { get; init; } = string.Empty;
    public string State { get; init; } = string.Empty;
    public IReadOnlyList<SendRecipientResponse> Recipients { get; init; } =
        Array.Empty<SendRecipientResponse>();
    public IReadOnlyList<string> Skipped { get; init; } = Array.Empty<string>();
}

public sealed class SendRecipientResponse
{
    public string SendId { get; init; } = string.Empty;
    public string SubscriberId { get; init; } = string.Empty;
    public string State { get; init; } = string.Empty;
    public int RetryCount { get; init; }
    public string? TransactionId { get; init; }
    public string? Error { get; init; }
}

public sealed class SendRequestStatusResponse
{
    public string RequestId { get; init; } = string.Empty;
    public string State { get; init; } = string.Empty;
    public IReadOnlyList<SendRecipientResponse> Recipients { get; init; } =
        Array.Empty<SendRecipientResponse>();
}
