namespace MessageCenter.Api.Outbox;

/// <summary>生命周期操作记录（T-INT-05）：幂等回执 + 终止标记审计。
/// operationId 由原来源提供，按 (source_system, operation_id) 唯一；同 operationId 同载荷重放返回原结果，异载荷 409。</summary>
public sealed class MessageLifecycleOperationEntity
{
    public string OperationId { get; set; } = string.Empty;
    public string SourceSystem { get; set; } = string.Empty;
    public string RequestId { get; set; } = string.Empty;
    public string SubscriberId { get; set; } = string.Empty;
    /// <summary>落到的 outbox 发送行；send 尚未到达（deferred 终止标记）时为空。</summary>
    public string? SendId { get; set; }
    /// <summary>CONSUME / REVOKE / EXPIRE。</summary>
    public string Action { get; set; } = string.Empty;
    public long BusinessVersion { get; set; }
    public string? Reason { get; set; }
    /// <summary>载荷指纹（不含 occurredAt：时间戳不参与幂等判定）。</summary>
    public string OperationHash { get; set; } = string.Empty;
    /// <summary>deferred=按原键暂存等待迟到 send；applied=已作用；superseded=被更高版本标记覆盖。</summary>
    public string ApplyStatus { get; set; } = string.Empty;
    /// <summary>作用（或暂存）当时的业务状态结果，重放时原样返回。</summary>
    public string? ResultBusinessState { get; set; }
    public DateTime? OccurredAt { get; set; }
    public DateTime RecordedAt { get; set; }
    public DateTime? AppliedAt { get; set; }
}

public static class MessageLifecycleApplyStatus
{
    public const string Deferred = "deferred";
    public const string Applied = "applied";
    public const string Superseded = "superseded";
}
