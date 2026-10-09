DROP INDEX IF EXISTS idx_sync_logs_nextcloud_recovery;
ALTER TABLE sync_logs DROP COLUMN worker_attempt_token;
ALTER TABLE sync_logs DROP COLUMN worker_active;
ALTER TABLE sync_logs DROP COLUMN worker_started_at;
ALTER TABLE sync_logs DROP COLUMN queue_task_id;
ALTER TABLE sync_logs DROP COLUMN recovery_trigger;
ALTER TABLE sync_logs DROP COLUMN recovery_version;
