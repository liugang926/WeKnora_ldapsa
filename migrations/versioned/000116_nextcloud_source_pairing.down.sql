DROP TRIGGER IF EXISTS nextcloud_paired_kb_delete_guard ON knowledge_bases;
DROP FUNCTION IF EXISTS nextcloud_paired_kb_delete_guard();
DROP TRIGGER IF EXISTS nextcloud_source_pairing_sync_update ON sync_logs;
DROP TRIGGER IF EXISTS nextcloud_source_pairing_sync_insert ON sync_logs;
DROP FUNCTION IF EXISTS nextcloud_source_pairing_sync_admit();
DROP TRIGGER IF EXISTS nextcloud_source_pairing_admit_update ON data_sources;
DROP TRIGGER IF EXISTS nextcloud_source_pairing_admit_insert ON data_sources;
DROP FUNCTION IF EXISTS nextcloud_source_pairing_admit();
DROP TABLE IF EXISTS nextcloud_source_pairings;
