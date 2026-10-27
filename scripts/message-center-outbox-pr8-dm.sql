-- PR8：先迁移再升级全部API与Worker；历史终态补偿任务保留原sendId/版本。
CREATE TABLE IF NOT EXISTS "message_terminal_sync" (
    "send_id"             VARCHAR2(64) NOT NULL,
    "business_version"    NUMBER(19) NOT NULL,
    "state"               VARCHAR2(16) DEFAULT 'pending' NOT NULL,
    "attempts"            INT DEFAULT 0 NOT NULL,
    "next_attempt_at"     TIMESTAMP(6) NULL,
    "locked_by"           VARCHAR2(128) NULL,
    "locked_until"        TIMESTAMP(6) NULL,
    "last_error"          VARCHAR2(128) NULL,
    "last_confirmed_at"   TIMESTAMP(6) NULL,
    "message_ids"         CLOB NULL,
    "next_page"           INT DEFAULT 0 NOT NULL,
    "never_triggered"     INT DEFAULT 0 NOT NULL,
    CONSTRAINT "pk_message_terminal_sync" PRIMARY KEY ("send_id", "business_version"),
    CONSTRAINT "ck_message_terminal_sync" CHECK ("state" IN ('pending', 'watching', 'superseded', 'not-required'))
);

CREATE INDEX IF NOT EXISTS "ix_msg_terminal_sync_due"
    ON "message_terminal_sync" ("state", "next_attempt_at");

-- 重复迁移不重置已有任务、租约、重试和巡检结果；历史消息与生命周期操作不改写。
INSERT INTO "message_terminal_sync" ("send_id", "business_version", "next_attempt_at")
SELECT o."send_id", o."business_version", CURRENT_TIMESTAMP
FROM "message_send_outbox" o
WHERE o."business_state" IN ('CONSUMED', 'REVOKED', 'EXPIRED')
  AND NOT EXISTS (SELECT 1 FROM "message_terminal_sync" s
      WHERE s."send_id" = o."send_id" AND s."business_version" = o."business_version");
