namespace MessageCenter.Api.Options;

public sealed class RedisOptions
{
    public const string Section = "Redis";

    public string ConnectionString { get; set; } = string.Empty;
    public int Database { get; set; } = 7;
    public string KeyPrefix { get; set; } = "message-center";
}
