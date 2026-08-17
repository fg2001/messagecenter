namespace MessageCenter.Api.Outbox;

public static class MessageOutboxState
{
    public const string Pending = "pending";
    public const string Accepted = "accepted";
    public const string Visible = "visible";
    public const string Failed = "failed";
}
