using System.Security.Claims;
using Microsoft.Extensions.Configuration;

namespace MessageCenter.Api.Options;

public class SourceSystemOptions
{
    public const string Section = "SourceSystemNames";

    public Dictionary<string, string> Names { get; set; } = new();
    public Dictionary<string, SourceServiceIdentity> ServiceIdentities { get; set; } = new();

    public void Bind(IConfiguration configuration)
    {
        configuration.GetSection(Section).Bind(Names);
        foreach (var source in configuration.GetSection("SourceServiceIdentities").GetChildren())
        {
            var identity = new SourceServiceIdentity();
            source.Bind(identity);
            ServiceIdentities[source.Key] = identity;
        }
    }

    public bool AllowsService(string sourceSystem, ClaimsPrincipal user)
        => !ServiceIdentities.TryGetValue(sourceSystem, out var identity) || identity.Matches(user);
}

public sealed class SourceServiceIdentity
{
    public string Issuer { get; set; } = string.Empty;
    public string Subject { get; set; } = string.Empty;
    public string ClientId { get; set; } = string.Empty;
    public string Audience { get; set; } = string.Empty;

    public bool Matches(ClaimsPrincipal user)
        => user.Identity?.IsAuthenticated == true
           && user.FindAll("preferred_username").Count() == 1
           && SingleClaim(user, "iss", Issuer)
           && SingleClaim(user, "sub", Subject)
           && SingleClaim(user, "azp", ClientId)
           && !string.IsNullOrWhiteSpace(Audience)
           && user.FindAll("aud").Any(claim => claim.Value == Audience);

    private static bool SingleClaim(ClaimsPrincipal user, string name, string expected)
    {
        var claims = user.FindAll(name).ToArray();
        return !string.IsNullOrWhiteSpace(expected) && claims.Length == 1 && claims[0].Value == expected;
    }
}
