namespace MessageCenter.Api.Options;

public class NovuOptions
{
    public const string Section = "Novu";

    public string BaseUrl { get; set; } = string.Empty;
    public string ApiKey { get; set; } = string.Empty;
    public string DefaultWorkflowId { get; set; } = "system-notification";
    // 按服务端登记后的来源选择工作流，不接受请求正文指定工作流。
    public Dictionary<string, string> SourceWorkflows { get; set; } = new(StringComparer.Ordinal);

    public bool TryGetWorkflow(string sourceSystem, out string workflowId)
    {
        workflowId = SourceWorkflows.TryGetValue(sourceSystem, out var configured)
            ? configured
            : DefaultWorkflowId;
        return !string.IsNullOrWhiteSpace(workflowId);
    }
    public string InAppChannel { get; set; } = "in_app";
    public int TimeoutSeconds { get; set; } = 10;
    public int WorkerTimeoutSeconds { get; set; } = 30;

    // 严格触发来源（登记后的来源值）：结果不确定时冻结为 uncertain，不自动重发同键请求，
    // 只允许查询原操作或人工对账（resolve-trigger）收敛。未列入的来源保持既有重试行为。
    public HashSet<string> StrictTriggerSources { get; set; } = new(StringComparer.Ordinal);

    public bool IsStrictTriggerSource(string sourceSystem)
        => StrictTriggerSources.Contains(sourceSystem);
}
