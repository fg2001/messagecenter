namespace MessageCenter.Api.Outbox;

public static class MessageOutboxState
{
    public const string Pending = "pending";
    public const string Accepted = "accepted";
    public const string Visible = "visible";
    public const string Failed = "failed";

    /// <summary>触发结果不确定（可能已被 Novu 受理）：不再自动重发，等待查询原操作或人工对账。</summary>
    public const string Uncertain = "uncertain";
}
