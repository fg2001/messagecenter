using MessageCenter.Api.Data;
using MessageCenter.Api.Models;
using MessageCenter.Api.Outbox;
using MessageCenter.Api.Services;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Xunit;

namespace MessageCenter.Tests;

public sealed class MessageProjectionAuthorityTests
{
    [Theory]
    [InlineData("CONSUMED")]
    [InlineData("REVOKED")]
    [InlineData("EXPIRED")]
    public async Task Terminal_is_filtered_by_original_send_and_transaction_for_its_recipient(string state)
    {
        await using var db=Db();db.MessageOutbox.Add(Row("terminal",state));db.MessageOutbox.Add(Row("active"));await db.SaveChangesAsync();
        var authority=new MessageProjectionAuthority(db);
        var result=await authority.ActiveAsync("person",new[] {
            Message("by-send","terminal"),Message("by-transaction",null,"terminal"),Message("active","active")
        });
        Assert.Equal(new[]{"active"},result.Select(x=>x.Item.MessageId));
    }

    [Fact]
    public async Task Wrong_source_recipient_and_conflicting_identifiers_do_not_create_active_micip_entry()
    {
        await using var db=Db();db.MessageOutbox.Add(Row("one"));db.MessageOutbox.Add(Row("two"));await db.SaveChangesAsync();
        var authority=new MessageProjectionAuthority(db);
        Assert.Empty(await authority.ActiveAsync("other-person",new[]{Message("wrong-recipient","one")}));
        Assert.Empty(await authority.ActiveAsync("person",new[]{Message("wrong-source","one",source:"other-source")}));
        Assert.Empty(await authority.ActiveAsync("person",new[]{Message("mixed","one","two")}));
    }

    [Fact]
    public async Task Unbound_micip_message_is_hidden_but_legacy_other_source_is_preserved()
    {
        await using var db=Db();var authority=new MessageProjectionAuthority(db);
        var result=await authority.ActiveAsync("person",new[]{
            Message("unknown-micip",null),Message("legacy",null,source:"cooper"),Message("untagged",null,source:null)
        });
        Assert.Equal(new[]{"legacy","untagged"},result.Select(x=>x.Item.MessageId));
    }

    [Fact]
    public async Task Real_name_and_business_id_do_not_substitute_for_original_identity()
    {
        await using var db=Db();db.MessageOutbox.Add(Row("send"));await db.SaveChangesAsync();
        var result=await new MessageProjectionAuthority(db).ActiveAsync("person",new[]{new ProjectionMessage(
            new MessageItemResponse {MessageId="unbound",SourceSystem="micip",BusinessId="send",Title="send"},null,null)});
        Assert.Empty(result);
    }

    [Fact]
    public async Task Terminal_revision_increases_once_per_committed_operation_not_on_replay_or_deferred_marker()
    {
        await using var db=Db();db.MessageOutbox.Add(Row("send"));await db.SaveChangesAsync();
        var lifecycle=new MessageLifecycleStore(db);var authority=new MessageProjectionAuthority(db);
        var input=new LifecycleApplyInput("micip","send","person","op","REVOKE",1,null,null);
        Assert.Equal(0,await authority.RevisionAsync("person"));
        await lifecycle.ApplyAsync(input,default);Assert.Equal(1,await authority.RevisionAsync("person"));
        await lifecycle.ApplyAsync(input,default);Assert.Equal(1,await authority.RevisionAsync("person"));
        await lifecycle.ApplyAsync(new("micip","absent","person","deferred","EXPIRE",1,null,null),default);
        Assert.Equal(1,await authority.RevisionAsync("person"));
        Assert.Equal(0,await authority.RevisionAsync("other-person"));
    }

    private static MessageCenterDbContext Db()=>new(new DbContextOptionsBuilder<MessageCenterDbContext>()
        .UseInMemoryDatabase("projection-"+Guid.NewGuid()).ConfigureWarnings(x=>x.Ignore(InMemoryEventId.TransactionIgnoredWarning)).Options);
    private static MessageOutboxEntity Row(string id,string state=MessageBusinessState.Active)=>new() {
        SendId=id,RequestId=id,RequestHash="hash",SourceSystem="micip",SubscriberId="person",WorkflowId="fixture",Payload="{}",
        Status=MessageOutboxState.Visible,BusinessState=state,NovuTransactionId=id
    };
    private static ProjectionMessage Message(string id,string? send,string? transaction=null,string? source="micip")
        =>new(new MessageItemResponse {MessageId=id,SourceSystem=source},send,transaction);
}
