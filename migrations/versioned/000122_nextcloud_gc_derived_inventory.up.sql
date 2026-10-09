-- Preserve exact local derived-row identities before any future index cleanup.
ALTER TABLE nextcloud_gc_items
    DROP CONSTRAINT nextcloud_gc_items_kind_check;
ALTER TABLE nextcloud_gc_items
    ADD CONSTRAINT nextcloud_gc_items_kind_check
    CHECK (kind IN ('knowledge', 'source_file', 'extracted_image', 'derived_index',
                   'derived_chunk', 'postgres_embedding', 'sqlite_embedding'));

-- Earlier workers could call an empty local-row inventory complete while a
-- historical graph/Wiki or external index still existed. Reopen those jobs;
-- resource items already acknowledged as collected remain idempotent.
INSERT INTO nextcloud_gc_items (job_id, kind, object_ref, state)
SELECT id, 'derived_index', knowledge_id, 'blocked'
FROM nextcloud_gc_jobs
ON CONFLICT (job_id, kind, object_ref) DO NOTHING;

UPDATE nextcloud_gc_jobs
SET state = 'blocked', completed_at = NULL,
    last_error_code = 'derived_provenance_unverified',
    next_attempt_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP
WHERE state = 'collected';
