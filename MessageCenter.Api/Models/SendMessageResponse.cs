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
    /// <summary>业务状态（include=terminal 时返回）：ACTIVE/CONSUMED/REVOKED/EXPIRED。</summary>
    public string? BusinessState { get; init; }
    /// <summary>最近一次生效的生命周期操作版本（include=terminal 时返回）。</summary>
    public long? BusinessVersion { get; init; }
    public DateTime? TerminalAt { get; init; }
    public string? TerminalReason { get; init; }
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

/// <summary>T-INT-05 生命周期操作请求（T-INT-01 契约候选稿 §6）。
/// operationId 由原来源提供并负责幂等：同 operationId 同载荷重放返回原结果，异载荷 409。</summary>
public sealed class LifecycleRequest
{
    public string OperationId { get; init; } = string.Empty;
    /// <summary>CONSUME / REVOKE / EXPIRE。</summary>
    public string Action { get; init; } = string.Empty;
    /// <summary>必须大于当前值（防倒退；等值亦拒）。</summary>
    public long BusinessVersion { get; init; }
    public string? Reason { get; init; }
    /// <summary>业务事实发生时间（信息性，不参与幂等指纹）；缺省为服务端受理时间。</summary>
    public DateTime? OccurredAt { get; init; }
}

public sealed class LifecycleResponse
{
    /// <summary>本地终态生效不代表外部同步完成；pending/watching/not-required/superseded/waiting-for-send/not-tracked。</summary>
    public string ExternalState { get; init; } = "pending";
    public string OperationId { get; init; } = string.Empty;
    public string RequestId { get; init; } = string.Empty;
    public string SubscriberId { get; init; } = string.Empty;
    public string Action { get; init; } = string.Empty;
    public long BusinessVersion { get; init; }
    /// <summary>applied=已作用到收件人行；deferred=send 未到达按原键暂存；replayed=幂等重放返回原结果。</summary>
    public string Status { get; init; } = string.Empty;
    public string? BusinessState { get; init; }
    public string? SendId { get; init; }
    public DateTime? OccurredAt { get; init; }
    public DateTime RecordedAt { get; init; }
}

public sealed class LifecycleOperationResponse
{
    public string ExternalState { get; init; } = "pending";
    public DateTime? LastExternalConfirmedAt { get; init; }
    public string? ExternalError { get; init; }
    public string OperationId { get; init; } = string.Empty;
    public string RequestId { get; init; } = string.Empty;
    public string SubscriberId { get; init; } = string.Empty;
    public string? SendId { get; init; }
    public string Action { get; init; } = string.Empty;
    public long BusinessVersion { get; init; }
    public string? Reason { get; init; }
    /// <summary>deferred / applied / superseded。</summary>
    public string ApplyStatus { get; init; } = string.Empty;
    public string? ResultBusinessState { get; init; }
    public DateTime? OccurredAt { get; init; }
    public DateTime RecordedAt { get; init; }
    public DateTime? AppliedAt { get; init; }
}
