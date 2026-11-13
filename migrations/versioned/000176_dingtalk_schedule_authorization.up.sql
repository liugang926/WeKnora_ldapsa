CREATE TABLE dingtalk_schedule_authorizations (
    data_source_id TEXT PRIMARY KEY,
    tenant_id BIGINT NOT NULL,
    knowledge_base_id TEXT NOT NULL,
    source_revision BIGINT NOT NULL,
    schedule TEXT NOT NULL,
    grant_id TEXT NOT NULL,
    user_id TEXT NOT NULL
);
ALTER TABLE sync_logs ADD COLUMN dingtalk_schedule_grant TEXT NOT NULL DEFAULT '';
