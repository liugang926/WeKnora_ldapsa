CREATE TABLE nextcloud_gc_jobs (
    id TEXT PRIMARY KEY,
    tenant_id INTEGER NOT NULL,
    knowledge_base_id TEXT NOT NULL,
    datasource_id TEXT NOT NULL,
    external_id TEXT NOT NULL,
    knowledge_id TEXT NOT NULL UNIQUE,
    reason TEXT NOT NULL CHECK (reason IN ('retired', 'tombstone')),
    state TEXT NOT NULL CHECK (state IN ('pending', 'retry', 'blocked', 'collected')),
    not_before DATETIME NOT NULL,
    original_not_before DATETIME NOT NULL,
    next_attempt_at DATETIME NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    last_error_code TEXT NOT NULL DEFAULT '',
    completed_at DATETIME,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_nextcloud_gc_due ON nextcloud_gc_jobs (state, next_attempt_at, not_before);
CREATE INDEX idx_nextcloud_gc_tenant ON nextcloud_gc_jobs (tenant_id, created_at DESC);

CREATE TABLE nextcloud_gc_items (
    job_id TEXT NOT NULL REFERENCES nextcloud_gc_jobs(id) ON DELETE CASCADE,
    kind TEXT NOT NULL CHECK (kind IN ('knowledge', 'source_file', 'extracted_image', 'derived_index')),
    object_ref TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('pending', 'blocked', 'collected')),
    estimated_bytes INTEGER NOT NULL DEFAULT 0,
    confirmed_released_bytes INTEGER NOT NULL DEFAULT 0,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (job_id, kind, object_ref)
);
