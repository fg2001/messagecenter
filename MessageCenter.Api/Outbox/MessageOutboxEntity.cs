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

    /// <summary>已登记触发许可次数；进程可能在POST前退出，不能当作网络调用或送达次数。</summary>
    public int TriggerAttempts { get; set; }
    public DateTime? LastTriggerAt { get; set; }
    /// <summary>进入 uncertain 的时间；触发结果不确定期间非空。</summary>
    public DateTime? UncertainAt { get; set; }
    /// <summary>人工对账结论：CONFIRM_SENT / CONFIRM_NOT_SENT；未对账为空。</summary>
    public string? UncertainResolution { get; set; }

    /// <summary>业务状态（T-INT-01 §6）：ACTIVE 默认；CONSUMED/REVOKED/EXPIRED 终态。
    /// 与回执状态正交：终态标记不回写回执历史，仅在未投递时抑制发送（suppressed）。</summary>
    public string BusinessState { get; set; } = MessageBusinessState.Active;
    /// <summary>最近一次生效的生命周期操作版本；防倒退基准（≤ 当前值的操作被 422 拒绝）。</summary>
    public long BusinessVersion { get; set; }
    public DateTime? TerminalAt { get; set; }
    public string? TerminalReason { get; set; }
}
