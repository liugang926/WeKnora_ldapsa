CREATE TABLE nextcloud_candidate_retry_jobs (
    tenant_id INTEGER NOT NULL,
    knowledge_base_id TEXT NOT NULL,
    datasource_id TEXT NOT NULL,
    external_id TEXT NOT NULL,
    desired_etag TEXT NOT NULL,
    failed_candidate_id TEXT NOT NULL,
    first_staged_at DATETIME NOT NULL,
    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    next_attempt_at DATETIME NOT NULL,
    state TEXT NOT NULL DEFAULT 'retry' CHECK (state IN ('retry', 'leased', 'manual')),
    lease_token TEXT,
    lease_until DATETIME,
    sync_log_id TEXT,
    last_error_code TEXT NOT NULL DEFAULT '',
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (tenant_id, knowledge_base_id, datasource_id, external_id),
    CHECK ((lease_token IS NULL AND lease_until IS NULL)
        OR (lease_token IS NOT NULL AND lease_until IS NOT NULL))
);

CREATE INDEX idx_nextcloud_candidate_retry_due
    ON nextcloud_candidate_retry_jobs (state, next_attempt_at);
CREATE INDEX idx_nextcloud_candidate_retry_source
    ON nextcloud_candidate_retry_jobs (datasource_id, state, lease_until);
