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
    /// <summary>已向 Novu 发起的触发尝试次数；触发结果不确定期间 state=uncertain。</summary>
    public int TriggerAttempts { get; init; }
    public DateTime? UncertainAt { get; init; }
    /// <summary>不确定窗口的人工对账结论：CONFIRM_SENT / CONFIRM_NOT_SENT；未对账为空。</summary>
    public string? UncertainResolution { get; init; }
}

public sealed class ResolveTriggerRequest
{
    public string SendId { get; init; } = string.Empty;
    /// <summary>CONFIRM_SENT 或 CONFIRM_NOT_SENT。</summary>
    public string Resolution { get; init; } = string.Empty;
    public string? Note { get; init; }
}

public sealed class SendRequestStatusResponse
{
    public string RequestId { get; init; } = string.Empty;
    public string State { get; init; } = string.Empty;
    public IReadOnlyList<SendRecipientResponse> Recipients { get; init; } =
        Array.Empty<SendRecipientResponse>();
}
