DROP INDEX IF EXISTS idx_sync_logs_nextcloud_recovery;
ALTER TABLE sync_logs DROP COLUMN IF EXISTS worker_attempt_token;
ALTER TABLE sync_logs DROP COLUMN IF EXISTS worker_active;
ALTER TABLE sync_logs DROP COLUMN IF EXISTS worker_started_at;
ALTER TABLE sync_logs DROP COLUMN IF EXISTS queue_task_id;
ALTER TABLE sync_logs DROP COLUMN IF EXISTS recovery_trigger;
ALTER TABLE sync_logs DROP COLUMN IF EXISTS recovery_version;
