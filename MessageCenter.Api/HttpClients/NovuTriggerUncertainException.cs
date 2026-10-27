namespace MessageCenter.Api.HttpClients;

/// <summary>触发请求可能已被 Novu 受理但结果未知（超时/连接中断/5xx/回执事务不符）。
/// 严格来源不得自动重发同键请求，只能查询原操作或走人工对账。</summary>
public sealed class NovuTriggerUncertainException : Exception
{
    public NovuTriggerUncertainException(string message, Exception? inner = null)
        : base(message, inner)
    {
    }
}

/// <summary>2xx 回执中携带了另一事务的 transactionId：事件可能已被处理，原始回执不可采信。</summary>
public sealed class NovuTriggerReceiptMismatchException : InvalidOperationException
{
    public NovuTriggerReceiptMismatchException(string message)
        : base(message)
    {
    }
}
