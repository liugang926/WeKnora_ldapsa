CREATE TABLE dingtalk_run_claims (
 data_source_id TEXT PRIMARY KEY, tenant_id BIGINT NOT NULL, knowledge_base_id TEXT NOT NULL,
 sync_log_id TEXT NOT NULL, task_id TEXT NOT NULL, retry_attempt INTEGER NOT NULL,
 token TEXT NOT NULL, source_revision BIGINT NOT NULL, expires_at TIMESTAMP NOT NULL, active BOOLEAN NOT NULL);
ALTER TABLE dingtalk_document_publications ADD COLUMN tombstone BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE dingtalk_work_intents ADD COLUMN process_attempt BIGINT NOT NULL DEFAULT 1;
ALTER TABLE dingtalk_work_intents ADD COLUMN capacity_settled BOOLEAN NOT NULL DEFAULT FALSE;
