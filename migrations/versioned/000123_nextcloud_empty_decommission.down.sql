DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM nextcloud_source_decommissions) THEN
        RAISE EXCEPTION 'cannot downgrade a retired Nextcloud source';
    END IF;
END $$;
DROP TRIGGER IF EXISTS nextcloud_virgin_row_guard ON nextcloud_source_virgin;
DROP FUNCTION IF EXISTS nextcloud_virgin_row_guard();
DROP TRIGGER IF EXISTS nextcloud_decommission_row_guard ON nextcloud_source_decommissions;
DROP FUNCTION IF EXISTS nextcloud_decommission_row_guard();
DROP TRIGGER IF EXISTS nextcloud_decommission_resume_guard ON data_sources;
DROP FUNCTION IF EXISTS nextcloud_decommission_resume_guard();
DROP TRIGGER IF EXISTS nextcloud_virgin_event_connection ON nextcloud_event_connections;
DROP TRIGGER IF EXISTS nextcloud_virgin_version ON nextcloud_source_versions;
DROP TRIGGER IF EXISTS nextcloud_virgin_chunk ON chunks;
DROP TRIGGER IF EXISTS nextcloud_virgin_knowledge ON knowledges;
DROP TRIGGER IF EXISTS nextcloud_virgin_sync_log ON sync_logs;
DROP FUNCTION IF EXISTS nextcloud_virgin_touch();
DROP TRIGGER IF EXISTS nextcloud_virgin_pair_insert ON nextcloud_source_pairings;
DROP FUNCTION IF EXISTS nextcloud_virgin_pair_insert();
DROP TABLE IF EXISTS nextcloud_source_decommissions;
DROP TABLE IF EXISTS nextcloud_source_virgin;
