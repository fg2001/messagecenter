-- MessageCenter reliable-send outbox for DM8.
-- Run as the schema owner before starting MessageCenter.Api/Worker.

CREATE TABLE IF NOT EXISTS "message_send_outbox" (
    "send_id"             VARCHAR2(64) NOT NULL,
    "request_id"          VARCHAR2(128) NOT NULL,
    "request_hash"        VARCHAR2(64) NOT NULL,
    "source_system"       VARCHAR2(128) NOT NULL,
    "subscriber_id"       VARCHAR2(128) NOT NULL,
    "workflow_id"         VARCHAR2(128) NOT NULL,
    "payload"             CLOB NOT NULL,
    "status"              VARCHAR2(16) DEFAULT 'pending' NOT NULL,
    "retry_count"         INT DEFAULT 0 NOT NULL,
    "next_retry_at"       TIMESTAMP(6) NULL,
    "locked_by"           VARCHAR2(128) NULL,
    "locked_until"        TIMESTAMP(6) NULL,
    "novu_transaction_id" VARCHAR2(255) NULL,
    "novu_status"         VARCHAR2(64) NULL,
    "acknowledged"        NUMBER(1) DEFAULT 0 NOT NULL,
    "last_error"          VARCHAR2(2000) NULL,
    "created_at"          TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP NOT NULL,
    "updated_at"          TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP NOT NULL,
    "accepted_at"         TIMESTAMP(6) NULL,
    "visible_at"          TIMESTAMP(6) NULL,
    "failed_at"           TIMESTAMP(6) NULL,
    "trigger_attempts"    INT DEFAULT 0 NOT NULL,
    "last_trigger_at"     TIMESTAMP(6) NULL,
    "uncertain_at"        TIMESTAMP(6) NULL,
    "uncertain_resolution" VARCHAR2(32) NULL,
    "business_state"      VARCHAR2(16) DEFAULT 'ACTIVE' NOT NULL,
    "business_version"    NUMBER(19) DEFAULT 0 NOT NULL,
    "terminal_at"         TIMESTAMP(6) NULL,
    "terminal_reason"     VARCHAR2(2000) NULL,
    CONSTRAINT "pk_message_send_outbox" PRIMARY KEY ("send_id"),
    CONSTRAINT "ck_message_send_outbox_status"
        CHECK ("status" IN ('pending', 'accepted', 'visible', 'failed', 'uncertain', 'suppressed')),
    CONSTRAINT "ck_message_send_outbox_business"
        CHECK ("business_state" IN ('ACTIVE', 'CONSUMED', 'REVOKED', 'EXPIRED')),
    CONSTRAINT "ux_msg_outbox_request_receiver"
        UNIQUE ("source_system", "request_id", "subscriber_id")
);

CREATE INDEX IF NOT EXISTS "ix_msg_outbox_due"
    ON "message_send_outbox" ("status", "next_retry_at");

CREATE INDEX IF NOT EXISTS "ix_msg_outbox_request"
    ON "message_send_outbox" ("source_system", "request_id");

CREATE INDEX IF NOT EXISTS "ix_msg_outbox_lock"
    ON "message_send_outbox" ("locked_until");


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

-- PR7：先迁移后同时更新MC API与Worker；只增加原请求键串行化锁表，不改写历史消息。
-- 锁记录须与原请求/终态历史共同保留，不得在还有并发请求时清空或逐请求结束删除。
CREATE TABLE IF NOT EXISTS "message_request_guard" (
    "request_key"    VARCHAR2(64) NOT NULL,
    "source_system"  VARCHAR2(128) NOT NULL,
    "request_id"     VARCHAR2(128) NOT NULL,
    CONSTRAINT "pk_message_request_guard" PRIMARY KEY ("request_key")
);
