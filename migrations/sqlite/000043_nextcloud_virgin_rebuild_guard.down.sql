CREATE TEMP TABLE nextcloud_virgin_downgrade_guard (value INTEGER CHECK (value = 0));
INSERT INTO nextcloud_virgin_downgrade_guard(value)
    SELECT 1 FROM nextcloud_pair_rebuild_history LIMIT 1;
INSERT INTO nextcloud_virgin_downgrade_guard(value)
    SELECT 1 FROM nextcloud_source_pairings LIMIT 1;
INSERT INTO nextcloud_virgin_downgrade_guard(value)
    SELECT 1 FROM nextcloud_source_decommissions LIMIT 1;
DROP TABLE nextcloud_virgin_downgrade_guard;
DROP TRIGGER IF EXISTS nextcloud_pair_rebuild_history_guard_delete;
DROP TRIGGER IF EXISTS nextcloud_pair_rebuild_history_guard_update;
DROP TRIGGER IF EXISTS nextcloud_pair_rebuild_remember;
DROP TRIGGER IF EXISTS nextcloud_virgin_pair_insert;
CREATE TRIGGER nextcloud_virgin_pair_insert AFTER INSERT ON nextcloud_source_pairings
BEGIN
    INSERT INTO nextcloud_source_virgin(pair_operation_id) VALUES (NEW.operation_id);
END;
DROP TABLE nextcloud_pair_rebuild_history;
