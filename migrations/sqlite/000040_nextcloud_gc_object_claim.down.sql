-- The INSERT fails its CHECK if any deletion claim is still in flight.
CREATE TABLE nextcloud_gc_items_old (
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

INSERT INTO nextcloud_gc_items_old (
    job_id, kind, object_ref, state, estimated_bytes,
    confirmed_released_bytes, created_at, updated_at
)
SELECT job_id, kind, object_ref, state, estimated_bytes,
       confirmed_released_bytes, created_at, updated_at
FROM nextcloud_gc_items;

DROP TABLE nextcloud_gc_items;
ALTER TABLE nextcloud_gc_items_old RENAME TO nextcloud_gc_items;
