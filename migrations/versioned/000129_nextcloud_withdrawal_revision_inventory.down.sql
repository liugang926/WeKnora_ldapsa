-- Never discard observed revision identities during a downgrade.
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM nextcloud_indexed_withdrawal_items
               WHERE kind = 'source_revision') THEN
        RAISE EXCEPTION 'nextcloud_withdrawal_revision_inventory_requires_audit_export';
    END IF;
END
$$;
ALTER TABLE nextcloud_indexed_withdrawal_items
    DROP CONSTRAINT nextcloud_indexed_withdrawal_items_kind_check;
ALTER TABLE nextcloud_indexed_withdrawal_items
    ADD CONSTRAINT nextcloud_indexed_withdrawal_items_kind_check
    CHECK (kind IN ('knowledge', 'source_file', 'extracted_image',
        'invalid_image_inventory', 'missing_local_embedding_table',
        'unverified_external_index', 'chunk', 'postgres_embedding',
        'gc_job', 'gc_item', 'source_version', 'sync_log'));
