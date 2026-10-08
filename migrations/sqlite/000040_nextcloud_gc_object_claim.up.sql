-- SQLite cannot change a CHECK constraint in place. Rebuild only this child
-- table, preserving every existing inventory row from 000038.
CREATE TABLE nextcloud_gc_items_new (
    job_id TEXT NOT NULL REFERENCES nextcloud_gc_jobs(id) ON DELETE CASCADE,
    kind TEXT NOT NULL CHECK (kind IN ('knowledge', 'source_file', 'extracted_image', 'derived_index')),
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

INSERT INTO nextcloud_gc_items_new (
    job_id, kind, object_ref, state, estimated_bytes,
    confirmed_released_bytes, lease_token, lease_until, created_at, updated_at
)
SELECT job_id, kind, object_ref, state, estimated_bytes,
       confirmed_released_bytes, '', NULL, created_at, updated_at
FROM nextcloud_gc_items;

DROP TABLE nextcloud_gc_items;
ALTER TABLE nextcloud_gc_items_new RENAME TO nextcloud_gc_items;
