-- PR5: message lifecycle (CONSUME/REVOKE/EXPIRE) migration for existing DM8 deployments.
-- Idempotent columns/constraints are re-declared; safe to run on schemas already migrated.

ALTER TABLE "message_send_outbox" ADD (
    "business_state"      VARCHAR2(16) DEFAULT 'ACTIVE' NOT NULL,
    "business_version"    NUMBER(19) DEFAULT 0 NOT NULL,
    "terminal_at"         TIMESTAMP(6) NULL,
    "terminal_reason"     VARCHAR2(2000) NULL
);

-- Widen the status check constraint to include 'suppressed' (accepted but never delivered).
ALTER TABLE "message_send_outbox" DROP CONSTRAINT "ck_message_send_outbox_status";
ALTER TABLE "message_send_outbox" ADD CONSTRAINT "ck_message_send_outbox_status"
    CHECK ("status" IN ('pending', 'accepted', 'visible', 'failed', 'uncertain', 'suppressed'));

ALTER TABLE "message_send_outbox" ADD CONSTRAINT "ck_message_send_outbox_business"
    CHECK ("business_state" IN ('ACTIVE', 'CONSUMED', 'REVOKED', 'EXPIRED'));

CREATE TABLE IF NOT EXISTS "message_lifecycle_operation" (
    "operation_id"          VARCHAR2(128) NOT NULL,
    "source_system"         VARCHAR2(128) NOT NULL,
    "request_id"            VARCHAR2(128) NOT NULL,
    "subscriber_id"         VARCHAR2(128) NOT NULL,
    "send_id"               VARCHAR2(64) NULL,
    "action"                VARCHAR2(16) NOT NULL,
    "business_version"      NUMBER(19) NOT NULL,
    "reason"                VARCHAR2(2000) NULL,
    "operation_hash"        VARCHAR2(64) NOT NULL,
    "apply_status"          VARCHAR2(16) NOT NULL,
    "result_business_state" VARCHAR2(16) NULL,
    "occurred_at"           TIMESTAMP(6) NULL,
    "recorded_at"           TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP NOT NULL,
    "applied_at"            TIMESTAMP(6) NULL,
    CONSTRAINT "pk_message_lifecycle_operation" PRIMARY KEY ("source_system", "operation_id"),
    CONSTRAINT "ck_message_lifecycle_action"
        CHECK ("action" IN ('CONSUME', 'REVOKE', 'EXPIRE')),
    CONSTRAINT "ck_message_lifecycle_apply"
        CHECK ("apply_status" IN ('deferred', 'applied', 'superseded'))
);

CREATE INDEX IF NOT EXISTS "ix_msg_lifecycle_request"
    ON "message_lifecycle_operation" ("source_system", "request_id", "subscriber_id");

CREATE INDEX IF NOT EXISTS "ix_msg_lifecycle_deferred"
    ON "message_lifecycle_operation" ("source_system", "request_id", "apply_status");
