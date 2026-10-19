DROP TRIGGER IF EXISTS nextcloud_target_old_source_closed ON data_sources;
DROP FUNCTION IF EXISTS nextcloud_target_old_source_guard();
DROP TRIGGER IF EXISTS nextcloud_root_proof_immutable ON nextcloud_root_migrations;
DROP TRIGGER IF EXISTS nextcloud_target_history_immutable ON nextcloud_target_migrations;
DROP FUNCTION IF EXISTS nextcloud_directory_history_guard();
DROP TABLE IF EXISTS nextcloud_control_recovery_gates;
