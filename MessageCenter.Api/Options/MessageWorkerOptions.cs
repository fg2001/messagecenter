namespace MessageCenter.Api.Options;

public sealed class MessageWorkerOptions
{
    public const string Section = "MessageWorker";

    public int PageRetryCount { get; set; } = 3;
    public int JobRetryCount { get; set; } = 5;
    public int RetryBaseDelaySeconds { get; set; } = 2;
    public int LockSeconds { get; set; } = 60;
    public int PendingIdleSeconds { get; set; } = 90;
    public int ActiveSyncIntervalSeconds { get; set; } = 30;
    public int OutboxPollingIntervalMilliseconds { get; set; } = 500;
    public int OutboxBatchSize { get; set; } = 4;
    public int OutboxMaxRetryCount { get; set; } = 10;
    public int OutboxLeaseSeconds { get; set; } = 180;
    public int OutboxVisibilityRetrySeconds { get; set; } = 5;
    public int OutboxVisibilityPages { get; set; } = 3;
}
