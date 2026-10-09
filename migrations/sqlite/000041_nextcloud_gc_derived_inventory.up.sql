-- SQLite rebuilds the child table to expand its kind CHECK without dropping
-- existing inventories or object-deletion claims.
CREATE TABLE nextcloud_gc_items_new (
    job_id TEXT NOT NULL REFERENCES nextcloud_gc_jobs(id) ON DELETE CASCADE,
    kind TEXT NOT NULL CHECK (kind IN ('knowledge', 'source_file', 'extracted_image', 'derived_index',
                                      'derived_chunk', 'postgres_embedding', 'sqlite_embedding')),
    object_ref TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('pending', 'blocked', 'deleting', 'collected')),
    estimated_bytes INTEGER NOT NULL DEFAULT 0,
    confirmed_released_bytes INTEGER NOT NULL DEFAULT 0,
    lease_token TEXT NOT NULL DEFAULT '',
    lease_until DATETIME,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (job_id, kind, object_ref)
);

INSERT INTO nextcloud_gc_items_new
    (job_id, kind, object_ref, state, estimated_bytes, confirmed_released_bytes,
     lease_token, lease_until, created_at, updated_at)
SELECT job_id, kind, object_ref, state, estimated_bytes, confirmed_released_bytes,
       lease_token, lease_until, created_at, updated_at
FROM nextcloud_gc_items;

DROP TABLE nextcloud_gc_items;
ALTER TABLE nextcloud_gc_items_new RENAME TO nextcloud_gc_items;

INSERT OR IGNORE INTO nextcloud_gc_items (job_id, kind, object_ref, state)
SELECT id, 'derived_index', knowledge_id, 'blocked'
FROM nextcloud_gc_jobs;

UPDATE nextcloud_gc_jobs
SET state = 'blocked', completed_at = NULL,
    last_error_code = 'derived_provenance_unverified',
    next_attempt_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP
WHERE state = 'collected';
