using MessageCenter.Api.Models;
using MessageCenter.Api.Controllers;
using MessageCenter.Api.Options;
using MessageCenter.Api.Services;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Options;
using System.Security.Claims;
using Xunit;

namespace MessageCenter.Tests;

public sealed class SourceWorkflowTests
{
    [Fact]
    public async Task Unregistered_service_cannot_select_workflow_by_forging_body_source()
    {
        var controller = Controller("unregistered", new NovuOptions());
        var result = Assert.IsType<ObjectResult>(await controller.Send(
            new SendMessageRequest { SourceSystem = "micip" }, CancellationToken.None));
        Assert.Equal(403, result.StatusCode);
    }

    [Fact]
    public async Task Registered_source_overwrites_body_before_rejecting_empty_override()
    {
        var options = new NovuOptions();
        options.SourceWorkflows["micip"] = "";
        var request = new SendMessageRequest { SourceSystem = "other-source" };
        var result = Assert.IsType<ObjectResult>(await Controller("fixture-service", options).Send(
            request, CancellationToken.None));
        Assert.Equal("micip", request.SourceSystem);
        Assert.Equal(503, result.StatusCode);
        Assert.Contains("SOURCE_WORKFLOW_UNAVAILABLE", System.Text.Json.JsonSerializer.Serialize(result.Value));
    }

    private static MessageCenterController Controller(string username, NovuOptions options)
    {
        // 这两条拒绝路径必须在入账/Novu/Redis之前结束，不提供这些依赖。
        return new MessageCenterController(null!, Options.Create(options),
            Options.Create(new SourceSystemOptions
            {
                Names = new() { ["fixture-service"] = "micip" }
            }), null!, null!, null!, null!)
        {
            ControllerContext = new ControllerContext
            {
                HttpContext = new DefaultHttpContext
                {
                    User = new ClaimsPrincipal(new ClaimsIdentity(new[]
                    {
                        new Claim("preferred_username", username)
                    }, "fixture"))
                }
            }
        };
    }

    [Fact]
    public void Existing_sources_keep_the_default_workflow()
    {
        var options = new NovuOptions();
        Assert.True(options.TryGetWorkflow("flow", out var workflow));
        Assert.Equal("system-notification", workflow);
    }

    [Fact]
    public void Configuration_binds_source_override_without_changing_other_sources()
    {
        var configuration = new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["Novu:DefaultWorkflowId"] = "legacy",
            ["Novu:SourceWorkflows:micip"] = "micip-reminder"
        }).Build();
        var options = configuration.GetSection("Novu").Get<NovuOptions>()!;
        Assert.True(options.TryGetWorkflow("micip", out var workflow));
        Assert.Equal("micip-reminder", workflow);
        Assert.True(options.TryGetWorkflow("flow", out workflow));
        Assert.Equal("legacy", workflow);
    }

    [Theory]
    [InlineData("")]
    [InlineData(" ")]
    public void Invalid_explicit_override_does_not_fall_back_to_shared_workflow(string configured)
    {
        var options = new NovuOptions();
        options.SourceWorkflows["micip"] = configured;
        Assert.False(options.TryGetWorkflow("micip", out _));
        Assert.True(options.TryGetWorkflow("flow", out _));
    }

    [Fact]
    public void Missing_default_is_rejected_but_valid_override_remains_available()
    {
        var options = new NovuOptions { DefaultWorkflowId = "" };
        options.SourceWorkflows["micip"] = "micip-reminder";
        Assert.False(options.TryGetWorkflow("flow", out _));
        Assert.True(options.TryGetWorkflow("micip", out _));
    }

    [Fact]
    public void Selected_workflow_is_applied_to_each_deduplicated_receiver()
    {
        var options = new NovuOptions();
        options.SourceWorkflows["micip"] = "micip-reminder";
        Assert.True(options.TryGetWorkflow("micip", out var workflow));
        var request = new SendMessageRequest
        {
            RequestId = "fixture-request", SourceSystem = "micip", Title = "待办",
            BusinessType = "TODO", Receivers = new()
            {
                new() { Type = "user", Id = "fixture-1" },
                new() { Type = "USER", Id = "fixture-1" },
                new() { Type = "user", Id = "fixture-2" }
            }
        };
        var triggers = MessageMapper.Map(request, workflow, out var skipped);
        Assert.Empty(skipped);
        Assert.Equal(2, triggers.Count);
        Assert.All(triggers, trigger => Assert.Equal("micip-reminder", trigger.WorkflowId));
    }
}
