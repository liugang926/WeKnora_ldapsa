-- Version zero preserves all pre-upgrade running logs without recovery.
ALTER TABLE sync_logs ADD COLUMN recovery_version INTEGER NOT NULL DEFAULT 0;
ALTER TABLE sync_logs ADD COLUMN recovery_trigger TEXT NOT NULL DEFAULT '';
ALTER TABLE sync_logs ADD COLUMN queue_task_id TEXT NOT NULL DEFAULT '';
ALTER TABLE sync_logs ADD COLUMN worker_started_at DATETIME;
ALTER TABLE sync_logs ADD COLUMN worker_active BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE sync_logs ADD COLUMN worker_attempt_token TEXT NOT NULL DEFAULT '';
CREATE INDEX idx_sync_logs_nextcloud_recovery ON sync_logs (started_at, id)
    WHERE status = 'running' AND recovery_version = 1 AND worker_started_at IS NULL;
