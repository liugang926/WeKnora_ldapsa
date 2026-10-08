-- SQLite cannot admit indexed withdrawal yet, but preserve the same private
-- inventory schema for a future fenced implementation. Rebuild the child table
-- because SQLite cannot change an existing CHECK constraint in place.
DROP TRIGGER nextcloud_indexed_withdrawal_item_immutable_delete;
DROP TRIGGER nextcloud_indexed_withdrawal_item_immutable_update;
CREATE TABLE nextcloud_indexed_withdrawal_items_new (
    operation_id TEXT NOT NULL REFERENCES nextcloud_indexed_withdrawals(operation_id) ON DELETE RESTRICT,
    kind TEXT NOT NULL CHECK (kind IN ('knowledge', 'source_file', 'extracted_image',
        'invalid_image_inventory', 'missing_local_embedding_table',
        'unverified_external_index', 'chunk', 'postgres_embedding',
        'gc_job', 'gc_item', 'source_version', 'source_revision', 'sync_log')),
    object_ref TEXT NOT NULL,
    knowledge_id TEXT NOT NULL DEFAULT '',
    observed_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (operation_id, kind, object_ref)
);
INSERT INTO nextcloud_indexed_withdrawal_items_new
    (operation_id, kind, object_ref, knowledge_id, observed_at)
SELECT operation_id, kind, object_ref, knowledge_id, observed_at
FROM nextcloud_indexed_withdrawal_items;
DROP TABLE nextcloud_indexed_withdrawal_items;
ALTER TABLE nextcloud_indexed_withdrawal_items_new
    RENAME TO nextcloud_indexed_withdrawal_items;
CREATE TRIGGER nextcloud_indexed_withdrawal_item_immutable_update
BEFORE UPDATE ON nextcloud_indexed_withdrawal_items
BEGIN SELECT RAISE(ABORT, 'nextcloud_indexed_withdrawal_item_immutable'); END;
CREATE TRIGGER nextcloud_indexed_withdrawal_item_immutable_delete
BEFORE DELETE ON nextcloud_indexed_withdrawal_items
BEGIN SELECT RAISE(ABORT, 'nextcloud_indexed_withdrawal_item_immutable'); END;
