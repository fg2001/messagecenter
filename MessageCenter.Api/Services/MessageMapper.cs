using MessageCenter.Api.Models;

namespace MessageCenter.Api.Services;

public static class MessageMapper
{
    private const string UserReceiverType = "user";

    public static IReadOnlyList<NovuTriggerPayload> Map(
        SendMessageRequest request,
        string workflowId,
        out IReadOnlyList<string> skipped)
    {
        var triggers = new List<NovuTriggerPayload>();
        var skippedList = new List<string>();

        var payload = new NovuPayloadFields
        {
            RequestId = request.RequestId,
            SourceSystem = request.SourceSystem ?? string.Empty,
            BusinessType = request.BusinessType,
            BusinessId = request.BusinessId,
            Title = request.Title,
            Content = request.Content,
            Url = request.Url
        };

        foreach (var receiver in request.Receivers
                     .GroupBy(x => new { Type = x.Type.ToLowerInvariant(), x.Id })
                     .Select(x => x.First()))
        {
            if (!string.Equals(receiver.Type, UserReceiverType, StringComparison.OrdinalIgnoreCase))
            {
                skippedList.Add($"Receiver type '{receiver.Type}' (id='{receiver.Id}') is not supported in MVP; only 'user' is accepted.");
                continue;
            }

            triggers.Add(new NovuTriggerPayload
            {
                WorkflowId = workflowId,
                SubscriberId = receiver.Id,
                Payload = payload
            });
        }

        skipped = skippedList;
        return triggers;
    }
}
