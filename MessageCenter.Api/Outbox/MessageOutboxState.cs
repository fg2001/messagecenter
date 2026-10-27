namespace MessageCenter.Api.Outbox;

public static class MessageOutboxState
{
    public const string Pending = "pending";
    public const string Accepted = "accepted";
    public const string Visible = "visible";
    public const string Failed = "failed";

    /// <summary>触发结果不确定（可能已被 Novu 受理）：不再自动重发，等待查询原操作或人工对账。</summary>
    public const string Uncertain = "uncertain";

    /// <summary>受理但抑制发送：迟到 send 受理时已带终态（REVOKED/EXPIRED/CONSUMED），
    /// 或发送前套用终态——不向 Novu 生成有效通知，保留受理审计。</summary>
    public const string Suppressed = "suppressed";
}
