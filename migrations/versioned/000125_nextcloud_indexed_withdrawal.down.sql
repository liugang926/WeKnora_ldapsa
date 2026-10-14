-- A committed withdrawal is an irreversible safety boundary.
DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM nextcloud_indexed_withdrawals) THEN
        RAISE EXCEPTION 'cannot roll back committed Nextcloud indexed withdrawals';
    END IF;
END $$;
DROP TRIGGER nextcloud_indexed_withdrawal_resume_guard ON data_sources;
DROP FUNCTION nextcloud_indexed_withdrawal_resume_guard();
DROP TRIGGER nextcloud_indexed_excludes_empty_ack ON nextcloud_source_decommissions;
DROP FUNCTION nextcloud_indexed_excludes_empty_ack();
DO $$ BEGIN
    IF to_regclass('embeddings') IS NOT NULL THEN
        DROP TRIGGER IF EXISTS nextcloud_indexed_embedding_guard ON embeddings;
    END IF;
END $$;
DROP TRIGGER nextcloud_indexed_sync_guard ON sync_logs;
DROP TRIGGER nextcloud_indexed_event_guard ON nextcloud_event_connections;
DROP TRIGGER nextcloud_indexed_version_guard ON nextcloud_source_versions;
DROP TRIGGER nextcloud_indexed_chunk_guard ON chunks;
DROP TRIGGER nextcloud_indexed_knowledge_guard ON knowledges;
DROP FUNCTION nextcloud_indexed_withdrawal_write_guard();
DROP TRIGGER nextcloud_indexed_withdrawal_item_immutable ON nextcloud_indexed_withdrawal_items;
DROP TRIGGER nextcloud_indexed_withdrawal_immutable ON nextcloud_indexed_withdrawals;
DROP FUNCTION nextcloud_indexed_withdrawal_immutable();
DROP TABLE nextcloud_indexed_withdrawal_items;
DROP TABLE nextcloud_indexed_withdrawals;
