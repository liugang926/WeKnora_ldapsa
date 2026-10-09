-- SQLite retains the withdrawal tombstone and observed SQL inventory, but
-- indexed withdrawal admission currently requires the PostgreSQL write fence.
CREATE TABLE nextcloud_indexed_withdrawals (
    operation_id TEXT PRIMARY KEY,
    pair_operation_id TEXT NOT NULL UNIQUE REFERENCES nextcloud_source_pairings(operation_id) ON DELETE RESTRICT,
    tenant_id INTEGER NOT NULL,
    knowledge_base_id TEXT NOT NULL UNIQUE,
    datasource_id TEXT NOT NULL UNIQUE,
    nextcloud_instance_id TEXT NOT NULL,
    binding_id TEXT NOT NULL,
    publication_epoch INTEGER NOT NULL CHECK (publication_epoch >= 0),
    key_id TEXT NOT NULL,
    state TEXT NOT NULL DEFAULT 'withdrawn_inventory_incomplete'
        CHECK (state = 'withdrawn_inventory_incomplete'),
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (nextcloud_instance_id, binding_id)
);
CREATE TABLE nextcloud_indexed_withdrawal_items (
    operation_id TEXT NOT NULL REFERENCES nextcloud_indexed_withdrawals(operation_id) ON DELETE RESTRICT,
    kind TEXT NOT NULL CHECK (kind IN ('knowledge', 'source_file', 'extracted_image',
        'invalid_image_inventory', 'missing_local_embedding_table',
        'unverified_external_index', 'chunk',
        'postgres_embedding', 'gc_job', 'gc_item', 'source_version', 'sync_log')),
    object_ref TEXT NOT NULL,
    knowledge_id TEXT NOT NULL DEFAULT '',
    observed_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (operation_id, kind, object_ref)
);
CREATE TRIGGER nextcloud_indexed_withdrawal_immutable_update
BEFORE UPDATE ON nextcloud_indexed_withdrawals
BEGIN SELECT RAISE(ABORT, 'nextcloud_indexed_withdrawal_immutable'); END;
CREATE TRIGGER nextcloud_indexed_withdrawal_immutable_delete
BEFORE DELETE ON nextcloud_indexed_withdrawals
BEGIN SELECT RAISE(ABORT, 'nextcloud_indexed_withdrawal_immutable'); END;
CREATE TRIGGER nextcloud_indexed_withdrawal_item_immutable_update
BEFORE UPDATE ON nextcloud_indexed_withdrawal_items
BEGIN SELECT RAISE(ABORT, 'nextcloud_indexed_withdrawal_item_immutable'); END;
CREATE TRIGGER nextcloud_indexed_withdrawal_item_immutable_delete
BEFORE DELETE ON nextcloud_indexed_withdrawal_items
BEGIN SELECT RAISE(ABORT, 'nextcloud_indexed_withdrawal_item_immutable'); END;
CREATE TRIGGER nextcloud_indexed_excludes_empty_ack
BEFORE INSERT ON nextcloud_source_decommissions
WHEN EXISTS (SELECT 1 FROM nextcloud_indexed_withdrawals
    WHERE pair_operation_id = NEW.pair_operation_id)
BEGIN SELECT RAISE(ABORT, 'nextcloud_indexed_source_cannot_ack_empty'); END;
CREATE TRIGGER nextcloud_indexed_withdrawal_resume_guard BEFORE UPDATE OF status ON data_sources
WHEN NEW.status <> 'paused' AND EXISTS (
    SELECT 1 FROM nextcloud_indexed_withdrawals WHERE datasource_id = NEW.id)
BEGIN SELECT RAISE(ABORT, 'nextcloud_indexed_source_withdrawn'); END;
