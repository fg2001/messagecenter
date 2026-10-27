using MessageCenter.Api.Outbox;
using MessageCenter.Api.Services;
using Microsoft.EntityFrameworkCore;

namespace MessageCenter.Api.Data;

public sealed class MessageCenterDbContext : DbContext
{
    public MessageCenterDbContext(DbContextOptions<MessageCenterDbContext> options)
        : base(options)
    {
    }

    public DbSet<MessageOutboxEntity> MessageOutbox => Set<MessageOutboxEntity>();
    public DbSet<MessageLifecycleOperationEntity> MessageLifecycleOperations
        => Set<MessageLifecycleOperationEntity>();
    public DbSet<MessageRequestGuardEntity> MessageRequestGuards => Set<MessageRequestGuardEntity>();
    public DbSet<MessageTerminalSyncEntity> MessageTerminalSync => Set<MessageTerminalSyncEntity>();
    public DbSet<MessageSubscriberVersionEntity> MessageSubscriberVersions => Set<MessageSubscriberVersionEntity>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        var revision = modelBuilder.Entity<MessageSubscriberVersionEntity>();
        revision.ToTable("message_subscriber_version");
        revision.HasKey(x => x.SubscriberId);
        revision.Property(x => x.SubscriberId).HasColumnName("subscriber_id").HasMaxLength(128);
        revision.Property(x => x.Revision).HasColumnName("revision");

        var sync = modelBuilder.Entity<MessageTerminalSyncEntity>();
        sync.ToTable("message_terminal_sync");
        sync.HasKey(x => new { x.SendId, x.BusinessVersion });
        sync.Property(x => x.SendId).HasColumnName("send_id").HasMaxLength(64);
        sync.Property(x => x.BusinessVersion).HasColumnName("business_version");
        sync.Property(x => x.State).HasColumnName("state").HasMaxLength(16);
        sync.Property(x => x.Attempts).HasColumnName("attempts");
        sync.Property(x => x.NextAttemptAt).HasColumnName("next_attempt_at");
        sync.Property(x => x.LockedBy).HasColumnName("locked_by").HasMaxLength(128);
        sync.Property(x => x.LockedUntil).HasColumnName("locked_until");
        sync.Property(x => x.LastError).HasColumnName("last_error").HasMaxLength(128);
        sync.Property(x => x.LastConfirmedAt).HasColumnName("last_confirmed_at");
        sync.Property(x => x.MessageIds).HasColumnName("message_ids").HasColumnType("CLOB");
        sync.Property(x => x.NextPage).HasColumnName("next_page");
        sync.Property(x => x.NeverTriggered).HasColumnName("never_triggered");
        sync.HasIndex(x => new { x.State, x.NextAttemptAt }).HasDatabaseName("ix_msg_terminal_sync_due");

        var guard = modelBuilder.Entity<MessageRequestGuardEntity>();
        guard.ToTable("message_request_guard");
        guard.HasKey(x => x.RequestKey);
        guard.Property(x => x.RequestKey).HasColumnName("request_key").HasMaxLength(64);
        guard.Property(x => x.SourceSystem).HasColumnName("source_system").HasMaxLength(128).IsRequired();
        guard.Property(x => x.RequestId).HasColumnName("request_id").HasMaxLength(128).IsRequired();

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
        entity.Property(x => x.Status).HasColumnName("status").HasMaxLength(16).IsRequired().IsConcurrencyToken();
        entity.Property(x => x.RetryCount).HasColumnName("retry_count");
        entity.Property(x => x.NextRetryAt).HasColumnName("next_retry_at");
        entity.Property(x => x.LockedBy).HasColumnName("locked_by").HasMaxLength(128).IsConcurrencyToken();
        entity.Property(x => x.LockedUntil).HasColumnName("locked_until").IsConcurrencyToken();
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
        entity.Property(x => x.BusinessState).HasColumnName("business_state").HasMaxLength(16)
            .HasDefaultValue("ACTIVE");
        entity.Property(x => x.BusinessVersion).HasColumnName("business_version").HasDefaultValue(0L).IsConcurrencyToken();
        entity.Property(x => x.TerminalAt).HasColumnName("terminal_at");
        entity.Property(x => x.TerminalReason).HasColumnName("terminal_reason").HasMaxLength(2000);

        entity.HasIndex(x => new { x.SourceSystem, x.RequestId, x.SubscriberId })
            .IsUnique()
            .HasDatabaseName("ux_msg_outbox_request_receiver");
        entity.HasIndex(x => new { x.Status, x.NextRetryAt })
            .HasDatabaseName("ix_msg_outbox_due");
        entity.HasIndex(x => new { x.SourceSystem, x.RequestId })
            .HasDatabaseName("ix_msg_outbox_request");
        entity.HasIndex(x => x.LockedUntil)
            .HasDatabaseName("ix_msg_outbox_lock");

        var lifecycle = modelBuilder.Entity<MessageLifecycleOperationEntity>();
        lifecycle.ToTable("message_lifecycle_operation");
        lifecycle.HasKey(x => new { x.SourceSystem, x.OperationId });
        lifecycle.Property(x => x.OperationId).HasColumnName("operation_id").HasMaxLength(128);
        lifecycle.Property(x => x.SourceSystem).HasColumnName("source_system").HasMaxLength(128);
        lifecycle.Property(x => x.RequestId).HasColumnName("request_id").HasMaxLength(128);
        lifecycle.Property(x => x.SubscriberId).HasColumnName("subscriber_id").HasMaxLength(128);
        lifecycle.Property(x => x.SendId).HasColumnName("send_id").HasMaxLength(64);
        lifecycle.Property(x => x.Action).HasColumnName("action").HasMaxLength(16);
        lifecycle.Property(x => x.BusinessVersion).HasColumnName("business_version");
        lifecycle.Property(x => x.Reason).HasColumnName("reason").HasMaxLength(2000);
        lifecycle.Property(x => x.OperationHash).HasColumnName("operation_hash").HasMaxLength(64);
        lifecycle.Property(x => x.ApplyStatus).HasColumnName("apply_status").HasMaxLength(16);
        lifecycle.Property(x => x.ResultBusinessState).HasColumnName("result_business_state").HasMaxLength(16);
        lifecycle.Property(x => x.OccurredAt).HasColumnName("occurred_at");
        lifecycle.Property(x => x.RecordedAt).HasColumnName("recorded_at");
        lifecycle.Property(x => x.AppliedAt).HasColumnName("applied_at");
        lifecycle.HasIndex(x => new { x.SourceSystem, x.RequestId, x.SubscriberId })
            .HasDatabaseName("ix_msg_lifecycle_request");
        lifecycle.HasIndex(x => new { x.SourceSystem, x.RequestId, x.ApplyStatus })
            .HasDatabaseName("ix_msg_lifecycle_deferred");
    }
}
