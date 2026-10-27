-- PR7：先迁移后同时更新MC API与Worker；只增加原请求键串行化锁表，不改写历史消息。
-- 锁记录须与原请求/终态历史共同保留，不得在还有并发请求时清空或逐请求结束删除。
CREATE TABLE IF NOT EXISTS "message_request_guard" (
    "request_key"    VARCHAR2(64) NOT NULL,
    "source_system"  VARCHAR2(128) NOT NULL,
    "request_id"     VARCHAR2(128) NOT NULL,
    CONSTRAINT "pk_message_request_guard" PRIMARY KEY ("request_key")
);
