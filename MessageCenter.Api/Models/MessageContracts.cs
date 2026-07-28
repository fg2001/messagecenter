namespace MessageCenter.Api.Models;

public sealed class MessageItemResponse
{
    public string MessageId { get; init; } = string.Empty;
    public string? SourceSystem { get; init; }
    public string? BusinessType { get; init; }
    public string? BusinessId { get; init; }
    public string? Title { get; init; }
    public string? Content { get; init; }
    public string? Url { get; init; }
    public bool Read { get; init; }
    public bool Seen { get; init; }
    public DateTime? CreatedAt { get; init; }
}

public sealed record MessagePageResponse(
    string State,
    IReadOnlyList<MessageItemResponse> Items,
    int Page,
    int Limit,
    long TotalCount,
    long UnreadCount);

public sealed record MessageSummaryResponse(
    string State,
    long TotalCount,
    long UnreadCount);

public sealed record MessageStateResponse(
    string MessageId,
    bool Read);

public sealed record ReadAllAcceptedResponse(
    string JobId,
    string State);

public sealed record ReadAllJobResponse(
    string JobId,
    string State,
    long Total,
    long Processed,
    string? Error);
