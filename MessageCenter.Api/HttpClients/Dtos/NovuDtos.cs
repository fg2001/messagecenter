namespace MessageCenter.Api.HttpClients.Dtos;

public class TriggerResult
{
    public bool Acknowledged { get; set; }
    public string? Status { get; set; }
    public string? TransactionId { get; set; }
}

public class NovuMessageItem
{
    public string? Id { get; set; }
    public string? TransactionId { get; set; }
    public string? Subject { get; set; }
    public string? Content { get; set; }
    public NovuMessagePayload? Payload { get; set; }
    public NovuCta? Cta { get; set; }
    public bool Read { get; set; }
    public bool Seen { get; set; }
    public DateTime? CreatedAt { get; set; }
}

public class NovuMessagePayload
{
    public string? RequestId { get; set; }
    public string? SendId { get; set; }
    public string? SourceSystem { get; set; }
    public string? BusinessType { get; set; }
    public string? BusinessId { get; set; }
}

public class NovuCta
{
    public NovuCtaData? Data { get; set; }
}

public class NovuCtaData
{
    public string? Url { get; set; }
}

public class FeedResult
{
    public List<NovuMessageItem> Data { get; set; } = new();
    public bool HasMore { get; set; }
    public int TotalCount { get; set; }
    public int PageSize { get; set; }
    public int Page { get; set; }
}
