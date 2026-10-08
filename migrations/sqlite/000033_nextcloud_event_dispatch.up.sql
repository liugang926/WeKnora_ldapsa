-- One durable dispatch cursor per event connection. Receipt is tracked by
-- nextcloud_event_checkpoint. The worker advances dispatched_id after queueing
-- a source reconciliation. applied_id remains zero until a later publication
-- proof mechanism is implemented.
CREATE TABLE nextcloud_event_dispatch (
    connection_id TEXT PRIMARY KEY REFERENCES nextcloud_event_connections (connection_id) ON DELETE RESTRICT,
    dispatched_id INTEGER NOT NULL DEFAULT 0 CHECK (dispatched_id >= 0),
    applied_id INTEGER NOT NULL DEFAULT 0 CHECK (applied_id >= 0 AND applied_id <= dispatched_id),
    state TEXT NOT NULL DEFAULT 'idle'
        CHECK (state IN ('idle', 'leased', 'queued', 'retry', 'blocked')),
    lease_token TEXT,
    lease_until DATETIME,
    target_event_id INTEGER NOT NULL DEFAULT 0 CHECK (target_event_id >= 0),
    last_sync_log_id TEXT,
    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    next_attempt_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_error_code TEXT NOT NULL DEFAULT '',
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CHECK ((lease_token IS NULL AND lease_until IS NULL)
           OR (lease_token IS NOT NULL AND lease_token <> '' AND lease_until IS NOT NULL))
);

CREATE INDEX idx_nextcloud_event_dispatch_due
    ON nextcloud_event_dispatch (state, next_attempt_at);

CREATE INDEX idx_nextcloud_event_dispatch_lease
    ON nextcloud_event_dispatch (state, lease_until);

-- Existing durable receipts remain pending for the dispatcher after upgrade.
INSERT INTO nextcloud_event_dispatch (connection_id)
SELECT connection_id FROM nextcloud_event_connections;

CREATE TRIGGER nextcloud_event_sync_admit_insert
BEFORE INSERT ON sync_logs
WHEN NEW.status = 'running'
 AND EXISTS (SELECT 1 FROM data_sources WHERE id = NEW.data_source_id AND type = 'nextcloud')
 AND EXISTS (SELECT 1 FROM sync_logs
              WHERE data_source_id = NEW.data_source_id
                AND status = 'running' AND id <> NEW.id)
BEGIN
    SELECT RAISE(ABORT, 'nextcloud_source_sync_busy');
END;

CREATE TRIGGER nextcloud_event_sync_admit_update
BEFORE UPDATE OF status, data_source_id ON sync_logs
WHEN NEW.status = 'running'
 AND EXISTS (SELECT 1 FROM data_sources WHERE id = NEW.data_source_id AND type = 'nextcloud')
 AND EXISTS (SELECT 1 FROM sync_logs
              WHERE data_source_id = NEW.data_source_id
                AND status = 'running' AND id <> NEW.id)
BEGIN
    SELECT RAISE(ABORT, 'nextcloud_source_sync_busy');
END;
