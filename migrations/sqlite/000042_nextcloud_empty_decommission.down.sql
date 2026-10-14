CREATE TEMP TABLE nextcloud_decommission_rollback_guard (value INTEGER CHECK (value = 0));
INSERT INTO nextcloud_decommission_rollback_guard(value)
    SELECT 1 FROM nextcloud_source_decommissions LIMIT 1;
DROP TABLE nextcloud_decommission_rollback_guard;
DROP TRIGGER IF EXISTS nextcloud_virgin_row_guard;
DROP TRIGGER IF EXISTS nextcloud_decommission_row_guard_delete;
DROP TRIGGER IF EXISTS nextcloud_decommission_row_guard_update;
DROP TRIGGER IF EXISTS nextcloud_decommission_resume_guard;
DROP TRIGGER IF EXISTS nextcloud_virgin_event_connection;
DROP TRIGGER IF EXISTS nextcloud_virgin_version;
DROP TRIGGER IF EXISTS nextcloud_virgin_chunk;
DROP TRIGGER IF EXISTS nextcloud_virgin_knowledge;
DROP TRIGGER IF EXISTS nextcloud_virgin_sync_log;
DROP TRIGGER IF EXISTS nextcloud_virgin_pair_insert;
DROP TABLE IF EXISTS nextcloud_source_decommissions;
DROP TABLE IF EXISTS nextcloud_source_virgin;
