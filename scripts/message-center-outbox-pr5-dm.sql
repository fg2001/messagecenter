-- PR5：既有DM8生命周期升级；备份并停止全部API/Worker写入后，在PR4之后执行。
-- 逐列补缺，匿名块以独占一行的/结束；用DIsql或支持匿名块的执行器，不得直接按分号拆分。
-- DDL会提交；中断后保持写入暂停，完整重跑并核对约束后才能恢复服务。

ALTER TABLE "message_send_outbox" ADD COLUMN IF NOT EXISTS "business_state" VARCHAR2(16) DEFAULT 'ACTIVE' NOT NULL;
ALTER TABLE "message_send_outbox" ADD COLUMN IF NOT EXISTS "business_version" NUMBER(19) DEFAULT 0 NOT NULL;
ALTER TABLE "message_send_outbox" ADD COLUMN IF NOT EXISTS "terminal_at" TIMESTAMP(6) NULL;
ALTER TABLE "message_send_outbox" ADD COLUMN IF NOT EXISTS "terminal_reason" VARCHAR2(2000) NULL;

DECLARE
    existing_count INT;
BEGIN
    SELECT COUNT(*) INTO existing_count FROM ALL_CONSTRAINTS
      WHERE OWNER = SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA')
        AND TABLE_NAME = 'message_send_outbox' AND CONSTRAINT_NAME = 'ck_message_send_outbox_status';
    IF existing_count > 0 THEN
        EXECUTE IMMEDIATE 'ALTER TABLE "message_send_outbox" DROP CONSTRAINT "ck_message_send_outbox_status"';
    END IF;
    EXECUTE IMMEDIATE 'ALTER TABLE "message_send_outbox" ADD CONSTRAINT "ck_message_send_outbox_status" CHECK ("status" IN (''pending'', ''accepted'', ''visible'', ''failed'', ''uncertain'', ''suppressed''))';
END;
/

DECLARE
    existing_count INT;
BEGIN
    SELECT COUNT(*) INTO existing_count FROM ALL_CONSTRAINTS
      WHERE OWNER = SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA')
        AND TABLE_NAME = 'message_send_outbox' AND CONSTRAINT_NAME = 'ck_message_send_outbox_business';
    IF existing_count = 0 THEN
        EXECUTE IMMEDIATE 'ALTER TABLE "message_send_outbox" ADD CONSTRAINT "ck_message_send_outbox_business" CHECK ("business_state" IN (''ACTIVE'', ''CONSUMED'', ''REVOKED'', ''EXPIRED''))';
    END IF;
END;
/

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
