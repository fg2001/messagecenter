namespace MessageCenter.Api.Outbox;

public sealed class MessageOutboxEntity
{
    public string SendId { get; set; } = string.Empty;
    public string RequestId { get; set; } = string.Empty;
    public string RequestHash { get; set; } = string.Empty;
    public string SourceSystem { get; set; } = string.Empty;
    public string SubscriberId { get; set; } = string.Empty;
    public string WorkflowId { get; set; } = string.Empty;
    public string Payload { get; set; } = string.Empty;
    public string Status { get; set; } = MessageOutboxState.Pending;
    public int RetryCount { get; set; }
    public DateTime? NextRetryAt { get; set; }
    public string? LockedBy { get; set; }
    public DateTime? LockedUntil { get; set; }
    public string? NovuTransactionId { get; set; }
    public string? NovuStatus { get; set; }
    public bool Acknowledged { get; set; }
    public string? LastError { get; set; }
    public DateTime CreatedAt { get; set; }
    public DateTime UpdatedAt { get; set; }
    public DateTime? AcceptedAt { get; set; }
    public DateTime? VisibleAt { get; set; }
    public DateTime? FailedAt { get; set; }

    /// <summary>触发尝试次数（每次真正 POST 前递增，含最终成功的一次）。</summary>
    public int TriggerAttempts { get; set; }
    public DateTime? LastTriggerAt { get; set; }
    /// <summary>进入 uncertain 的时间；触发结果不确定期间非空。</summary>
    public DateTime? UncertainAt { get; set; }
    /// <summary>人工对账结论：CONFIRM_SENT / CONFIRM_NOT_SENT；未对账为空。</summary>
    public string? UncertainResolution { get; set; }
}
