DROP TRIGGER IF EXISTS nextcloud_event_sync_admit_update ON sync_logs;
DROP TRIGGER IF EXISTS nextcloud_event_sync_admit_insert ON sync_logs;
DROP FUNCTION IF EXISTS nextcloud_event_sync_admit();
DROP TABLE IF EXISTS nextcloud_event_dispatch;
