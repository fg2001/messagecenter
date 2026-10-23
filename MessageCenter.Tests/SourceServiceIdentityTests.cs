using System.IdentityModel.Tokens.Jwt;
using System.Security.Claims;
using MessageCenter.Api.Controllers;
using MessageCenter.Api.Models;
using MessageCenter.Api.Options;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Options;
using Microsoft.IdentityModel.Tokens;
using Xunit;

namespace MessageCenter.Tests;

public sealed class SourceServiceIdentityTests
{
    private const string Issuer = "http://isolated-idp.test/realms/test";
    private static readonly SymmetricSecurityKey Key = new(new byte[32]
    {
        1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,
        17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32
    });

    private static SourceSystemOptions Settings() => new()
    {
        Names = new() { ["fixture-service"] = "micip", ["legacy-service"] = "legacy" },
        ServiceIdentities = new()
        {
            ["micip"] = new SourceServiceIdentity
            {
                Issuer = Issuer, Subject = "service-sub", ClientId = "micip-svc", Audience = "mc-api"
            }
        }
    };

    private static ClaimsPrincipal User(params Claim[] extra)
        => new(new ClaimsIdentity(new[]
        {
            new Claim("preferred_username", "fixture-service"), new Claim("iss", Issuer),
            new Claim("sub", "service-sub"), new Claim("azp", "micip-svc"), new Claim("aud", "mc-api")
        }.Concat(extra), "fixture"));

    [Theory]
    [InlineData("iss")]
    [InlineData("sub")]
    [InlineData("azp")]
    [InlineData("aud")]
    public void Missing_or_wrong_protected_claim_is_denied(string name)
    {
        var options = Settings();
        var claims = User().Claims.Where(c => c.Type != name).ToArray();
        Assert.False(options.AllowsService("micip", new(new ClaimsIdentity(claims, "fixture"))));
        Assert.False(options.AllowsService("micip", new(new ClaimsIdentity(
            claims.Append(new Claim(name, "wrong")), "fixture"))));
    }

    [Theory]
    [InlineData("iss")]
    [InlineData("sub")]
    [InlineData("azp")]
    [InlineData("preferred_username")]
    public void Duplicate_identity_claims_are_denied(string name)
        => Assert.False(Settings().AllowsService("micip", User(new Claim(name, "duplicate"))));

    [Fact]
    public void Unauthenticated_or_incomplete_binding_is_denied()
    {
        var options = Settings();
        Assert.False(options.AllowsService("micip", new(new ClaimsIdentity(User().Claims))));
        options.ServiceIdentities["micip"].Audience = "";
        Assert.False(options.AllowsService("micip", User()));
    }

    [Fact]
    public void Unconfigured_source_keeps_existing_compatibility()
    {
        Assert.True(Settings().AllowsService("legacy", User()));
        Assert.True(new SourceSystemOptions().AllowsService("legacy", User()));
    }

    [Fact]
    public void Dictionary_configuration_binds_all_identity_fields()
    {
        var configuration = new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string,string?>
        {
            ["SourceServiceIdentities:micip:Issuer"] = Issuer,
            ["SourceServiceIdentities:micip:Subject"] = "service-sub",
            ["SourceServiceIdentities:micip:ClientId"] = "micip-svc",
            ["SourceServiceIdentities:micip:Audience"] = "mc-api"
        }).Build();
        var options = Settings(); options.ServiceIdentities.Clear();
        options.Bind(configuration);
        Assert.True(options.AllowsService("micip", User()));
        Assert.False(options.AllowsService("micip", User(new Claim("sub", "person-sub"))));
    }

    [Theory]
    [InlineData("{}", "send")]
    [InlineData("{}", "query")]
    [InlineData("{}", "retry")]
    [InlineData("null", "send")]
    [InlineData("null", "query")]
    [InlineData("null", "retry")]
    [InlineData("\"\"", "send")]
    [InlineData("\"\"", "query")]
    [InlineData("\"\"", "retry")]
    public async Task Explicit_empty_json_binding_denies_all_source_endpoints(string value, string endpoint)
    {
        using var json = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(
            "{\"SourceServiceIdentities\":{\"micip\":" + value + "}}"));
        var configuration = new ConfigurationBuilder().AddJsonStream(json).Build();
        var options = Settings(); options.ServiceIdentities.Clear();
        options.Bind(configuration);
        foreach (var claim in new[] { "iss", "sub", "azp", "aud", "valid" })
        {
            var user = new ClaimsPrincipal(new ClaimsIdentity(User().Claims.Where(c => c.Type != claim)
                .Concat(claim == "valid" ? Array.Empty<Claim>() : new[] { new Claim(claim, "wrong") }), "fixture"));
            var controller = Controller(user, options);
            var result = endpoint switch
            {
                "send" => await controller.Send(new SendMessageRequest(), CancellationToken.None),
                "query" => await controller.GetSendStatus("original-request", CancellationToken.None),
                _ => await controller.RetrySend("original-request", CancellationToken.None)
            };
            var denied = Assert.IsType<ObjectResult>(result);
            Assert.Equal(403, denied.StatusCode);
            Assert.Contains("SOURCE_SERVICE_IDENTITY_DENIED", System.Text.Json.JsonSerializer.Serialize(denied.Value));
        }
    }

    [Theory]
    [InlineData("{}")]
    [InlineData("{\"SourceServiceIdentities\":{}}")]
    public void Absent_source_node_keeps_legacy_compatibility(string value)
    {
        using var json = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(value));
        var configuration = new ConfigurationBuilder().AddJsonStream(json).Build();
        var options = new SourceSystemOptions(); options.Bind(configuration);
        Assert.True(options.AllowsService("legacy", User()));
        Assert.Empty(options.ServiceIdentities);
    }

    [Fact]
    public void Json_binding_keeps_valid_and_empty_sources_distinct()
    {
        using var json = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(@"{
            ""SourceSystemNames"": { ""fixture-service"": ""micip"", ""legacy-service"": ""legacy"" },
            ""SourceServiceIdentities"": {
                ""micip"": {
                    ""Issuer"": ""http://isolated-idp.test/realms/test"", ""Subject"": ""service-sub"",
                    ""ClientId"": ""micip-svc"", ""Audience"": ""mc-api""
                },
                ""empty"": {}
            }
        }"));
        var configuration = new ConfigurationBuilder().AddJsonStream(json).Build();
        var options = new SourceSystemOptions(); options.Bind(configuration);
        Assert.Equal("micip", options.Names["fixture-service"]);
        Assert.Equal("legacy", options.Names["legacy-service"]);
        Assert.True(options.AllowsService("micip", User()));
        Assert.False(options.AllowsService("micip", User(new Claim("sub", "person-sub"))));
        Assert.False(options.AllowsService("empty", User()));
        Assert.True(options.AllowsService("legacy", User()));
    }

    [Fact]
    public void Signed_service_token_requires_mc_audience_even_when_global_audience_check_is_off()
    {
        var options = Settings();
        Assert.True(options.AllowsService("micip", Validate(Token(new[] { "mc-api", "other-api" }))));
        // 上游全局受众校验仍为false；来源准入必须补上这一边界。
        Assert.False(options.AllowsService("micip", Validate(Token(new[] { "other-api" }))));
    }

    [Fact]
    public void Global_signature_issuer_and_expiry_validation_remains_required()
    {
        Assert.Throws<SecurityTokenExpiredException>(() => Validate(Token(new[] { "mc-api" }, expired:true)));
        Assert.Throws<SecurityTokenInvalidIssuerException>(() => Validate(Token(new[] { "mc-api" }, issuer:"http://wrong.test")));
        var token = Token(new[] { "mc-api" });
        var validation = Parameters(); validation.IssuerSigningKey = new SymmetricSecurityKey(new byte[32]);
        Assert.ThrowsAny<SecurityTokenException>(() => Handler().ValidateToken(token, validation, out _));
    }

    [Theory]
    [InlineData("send")]
    [InlineData("query")]
    [InlineData("retry")]
    public async Task All_source_endpoints_reject_person_token_before_business_dependencies(string endpoint)
    {
        var person = new ClaimsPrincipal(new ClaimsIdentity(User().Claims
            .Where(c => c.Type != "sub").Append(new Claim("sub", "person-sub")), "fixture"));
        var controller = Controller(person, Settings());
        var result = endpoint switch
        {
            "send" => await controller.Send(new SendMessageRequest { SourceSystem = "legacy" }, CancellationToken.None),
            "query" => await controller.GetSendStatus("original-request", CancellationToken.None),
            _ => await controller.RetrySend("original-request", CancellationToken.None)
        };
        var denied = Assert.IsType<ObjectResult>(result);
        Assert.Equal(403, denied.StatusCode);
        Assert.Contains("SOURCE_SERVICE_IDENTITY_DENIED", System.Text.Json.JsonSerializer.Serialize(denied.Value));
    }

    [Fact]
    public async Task Valid_service_passes_source_gate_and_body_cannot_change_source()
    {
        var request = new SendMessageRequest { SourceSystem = "legacy" };
        var controller = Controller(Validate(Token(new[] { "mc-api" })), Settings());
        // 无接收人返回原400，说明合法服务已通过身份检查且没有访问DB/Redis/Novu。
        Assert.IsType<BadRequestObjectResult>(await controller.Send(request, CancellationToken.None));
        Assert.Equal("micip", request.SourceSystem);
    }

    [Fact]
    public async Task Missing_source_registration_is_not_granted_by_identity_binding()
    {
        var options = Settings(); options.Names.Clear();
        var result = Assert.IsType<ObjectResult>(await Controller(User(), options).Send(new(), CancellationToken.None));
        Assert.Equal(403, result.StatusCode);
    }

    private static MessageCenterController Controller(ClaimsPrincipal user, SourceSystemOptions options)
        => new(null!, Options.Create(new NovuOptions()), Options.Create(options), null!, null!, null!)
        {
            ControllerContext = new ControllerContext { HttpContext = new DefaultHttpContext { User = user } }
        };

    private static JwtSecurityTokenHandler Handler() => new() { MapInboundClaims = false };
    private static ClaimsPrincipal Validate(string token) => Handler().ValidateToken(token, Parameters(), out _);
    private static TokenValidationParameters Parameters() => new()
    {
        ValidIssuer = Issuer, IssuerSigningKey = Key, ValidateIssuer = true, ValidateAudience = false,
        ValidateLifetime = true, RequireSignedTokens = true, ClockSkew = TimeSpan.Zero
    };
    private static string Token(string[] audiences, bool expired = false, string issuer = Issuer)
    {
        var token = new JwtSecurityToken(issuer:issuer,
            claims:User().Claims.Where(c => c.Type != "iss" && c.Type != "aud")
                .Concat(audiences.Select(a => new Claim("aud", a))),
            notBefore:DateTime.UtcNow.AddMinutes(-5),
            expires:DateTime.UtcNow.AddMinutes(expired ? -1 : 5),
            signingCredentials:new SigningCredentials(Key, SecurityAlgorithms.HmacSha256));
        return Handler().WriteToken(token);
    }
}
