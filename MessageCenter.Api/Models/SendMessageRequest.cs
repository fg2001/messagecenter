using System.ComponentModel.DataAnnotations;

namespace MessageCenter.Api.Models;

public class SendMessageRequest
{
    [Required, MaxLength(128)]
    public string RequestId { get; set; } = string.Empty;

    public string? SourceSystem { get; set; }

    [Required]
    [MaxLength(128)]
    public string BusinessType { get; set; } = string.Empty;

    public string? BusinessId { get; set; }

    [Required]
    [MaxLength(500)]
    public string Title { get; set; } = string.Empty;

    public string? Content { get; set; }

    public string? Url { get; set; }

    [Required, MinLength(1)]
    public List<Receiver> Receivers { get; set; } = new();
}

public class Receiver
{
    [Required]
    public string Type { get; set; } = string.Empty;

    [Required]
    [MaxLength(128)]
    public string Id { get; set; } = string.Empty;
}
