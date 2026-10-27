-- PR4：既有DM8严格触发升级；迁移前备份并停止全部API/Worker写入。
-- 列逐项补缺；重复/中断后可重跑。DM8 DDL会提交，不是原子事务，约束重建期间不得恢复写入。

ALTER TABLE "message_send_outbox" ADD COLUMN IF NOT EXISTS "trigger_attempts" INT DEFAULT 0 NOT NULL;
ALTER TABLE "message_send_outbox" ADD COLUMN IF NOT EXISTS "last_trigger_at" TIMESTAMP(6) NULL;
ALTER TABLE "message_send_outbox" ADD COLUMN IF NOT EXISTS "uncertain_at" TIMESTAMP(6) NULL;
ALTER TABLE "message_send_outbox" ADD COLUMN IF NOT EXISTS "uncertain_resolution" VARCHAR2(32) NULL;

-- 保留后续PR5的suppressed，防止全链重跑时收窄已有终态约束。
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
