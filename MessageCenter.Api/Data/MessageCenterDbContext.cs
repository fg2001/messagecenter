using MessageCenter.Api.Outbox;
using Microsoft.EntityFrameworkCore;

namespace MessageCenter.Api.Data;

public sealed class MessageCenterDbContext : DbContext
{
    public MessageCenterDbContext(DbContextOptions<MessageCenterDbContext> options)
        : base(options)
    {
    }

    public DbSet<MessageOutboxEntity> MessageOutbox => Set<MessageOutboxEntity>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        var entity = modelBuilder.Entity<MessageOutboxEntity>();
        entity.ToTable("message_send_outbox");
        entity.HasKey(x => x.SendId);

        entity.Property(x => x.SendId).HasColumnName("send_id").HasMaxLength(64);
        entity.Property(x => x.RequestId).HasColumnName("request_id").HasMaxLength(128).IsRequired();
        entity.Property(x => x.RequestHash).HasColumnName("request_hash").HasMaxLength(64).IsRequired();
        entity.Property(x => x.SourceSystem).HasColumnName("source_system").HasMaxLength(128).IsRequired();
        entity.Property(x => x.SubscriberId).HasColumnName("subscriber_id").HasMaxLength(128).IsRequired();
        entity.Property(x => x.WorkflowId).HasColumnName("workflow_id").HasMaxLength(128).IsRequired();
        entity.Property(x => x.Payload).HasColumnName("payload").HasColumnType("CLOB").IsRequired();
        entity.Property(x => x.Status).HasColumnName("status").HasMaxLength(16).IsRequired();
        entity.Property(x => x.RetryCount).HasColumnName("retry_count");
        entity.Property(x => x.NextRetryAt).HasColumnName("next_retry_at");
        entity.Property(x => x.LockedBy).HasColumnName("locked_by").HasMaxLength(128);
        entity.Property(x => x.LockedUntil).HasColumnName("locked_until");
        entity.Property(x => x.NovuTransactionId).HasColumnName("novu_transaction_id").HasMaxLength(255);
        entity.Property(x => x.NovuStatus).HasColumnName("novu_status").HasMaxLength(64);
        entity.Property(x => x.Acknowledged).HasColumnName("acknowledged");
        entity.Property(x => x.LastError).HasColumnName("last_error").HasMaxLength(2000);
        entity.Property(x => x.CreatedAt).HasColumnName("created_at");
        entity.Property(x => x.UpdatedAt).HasColumnName("updated_at");
        entity.Property(x => x.AcceptedAt).HasColumnName("accepted_at");
        entity.Property(x => x.VisibleAt).HasColumnName("visible_at");
        entity.Property(x => x.FailedAt).HasColumnName("failed_at");
        entity.Property(x => x.TriggerAttempts).HasColumnName("trigger_attempts").HasDefaultValue(0);
        entity.Property(x => x.LastTriggerAt).HasColumnName("last_trigger_at");
        entity.Property(x => x.UncertainAt).HasColumnName("uncertain_at");
        entity.Property(x => x.UncertainResolution).HasColumnName("uncertain_resolution").HasMaxLength(32);

        entity.HasIndex(x => new { x.SourceSystem, x.RequestId, x.SubscriberId })
            .IsUnique()
            .HasDatabaseName("ux_msg_outbox_request_receiver");
        entity.HasIndex(x => new { x.Status, x.NextRetryAt })
            .HasDatabaseName("ix_msg_outbox_due");
        entity.HasIndex(x => new { x.SourceSystem, x.RequestId })
            .HasDatabaseName("ix_msg_outbox_request");
        entity.HasIndex(x => x.LockedUntil)
            .HasDatabaseName("ix_msg_outbox_lock");
    }
}
