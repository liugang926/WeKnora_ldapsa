-- The first-stage indexed withdrawal observes the append-only local source
-- revision ledger as well as the mutable current source-version row. This
-- extends private evidence only; it does not make the inventory complete or
-- authorize a retirement ACK or physical derived-index deletion.
ALTER TABLE nextcloud_indexed_withdrawal_items
    DROP CONSTRAINT nextcloud_indexed_withdrawal_items_kind_check;
ALTER TABLE nextcloud_indexed_withdrawal_items
    ADD CONSTRAINT nextcloud_indexed_withdrawal_items_kind_check
    CHECK (kind IN ('knowledge', 'source_file', 'extracted_image',
        'invalid_image_inventory', 'missing_local_embedding_table',
        'unverified_external_index', 'chunk', 'postgres_embedding',
        'gc_job', 'gc_item', 'source_version', 'source_revision', 'sync_log'));
