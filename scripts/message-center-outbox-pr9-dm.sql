-- PR9：先迁移后统一升级API/Worker；新的投影格式强制重建，旧Redis内容不直接作为有效消息返回。
CREATE TABLE IF NOT EXISTS "message_subscriber_version" (
    "subscriber_id" VARCHAR2(128) NOT NULL,
    "revision"      NUMBER(19) DEFAULT 0 NOT NULL,
    CONSTRAINT "pk_message_subscriber_version" PRIMARY KEY ("subscriber_id")
);

-- 只补缺失版本行，不重置已有终态版本，也不改消息/补偿历史。
INSERT INTO "message_subscriber_version" ("subscriber_id", "revision")
SELECT DISTINCT o."subscriber_id", 0 FROM "message_send_outbox" o
WHERE NOT EXISTS (SELECT 1 FROM "message_subscriber_version" v WHERE v."subscriber_id" = o."subscriber_id");
