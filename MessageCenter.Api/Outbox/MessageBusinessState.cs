namespace MessageCenter.Api.Outbox;

/// <summary>消息业务状态：与回执状态（pending/accepted/visible/failed/uncertain/suppressed）正交，
/// 由原来源按业务事实驱动（T-INT-01 契约候选稿 §6）。</summary>
public static class MessageBusinessState
{
    public const string Active = "ACTIVE";
    public const string Consumed = "CONSUMED";
    public const string Revoked = "REVOKED";
    public const string Expired = "EXPIRED";

    /// <summary>终态集合：进入终态后不再回退（仅更高 businessVersion 可再次转移）。</summary>
    public static bool IsTerminal(string? state)
        => state == Consumed || state == Revoked || state == Expired;

    /// <summary>生命周期动作 → 业务状态；仅接受 CONSUME/REVOKE/EXPIRE。</summary>
    public static bool TryMapAction(string? action, out string state)
    {
        switch (action)
        {
            case "CONSUME": state = Consumed; return true;
            case "REVOKE": state = Revoked; return true;
            case "EXPIRE": state = Expired; return true;
            default: state = Active; return false;
        }
    }
}
