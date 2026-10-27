-- PR4: strict-trigger uncertain state migration for existing DM8 deployments.
-- Idempotent: safe to run on schemas already migrated.

ALTER TABLE "message_send_outbox" ADD (
    "trigger_attempts"     INT DEFAULT 0 NOT NULL,
    "last_trigger_at"      TIMESTAMP(6) NULL,
    "uncertain_at"         TIMESTAMP(6) NULL,
    "uncertain_resolution" VARCHAR2(32) NULL
);

-- Widen the status check constraint to include 'uncertain'.
ALTER TABLE "message_send_outbox" DROP CONSTRAINT "ck_message_send_outbox_status";
ALTER TABLE "message_send_outbox" ADD CONSTRAINT "ck_message_send_outbox_status"
    CHECK ("status" IN ('pending', 'accepted', 'visible', 'failed', 'uncertain'));
