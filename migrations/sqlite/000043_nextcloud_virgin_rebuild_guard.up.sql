-- A 42-era false virgin marker may belong to an old pending pair rebuilt by
-- hash repair. Fail closed for all existing pairs; future new pairs can prove
-- virgin status through the guarded trigger below.
CREATE TEMP TABLE nextcloud_virgin_upgrade_guard (value INTEGER CHECK (value = 0));
INSERT INTO nextcloud_virgin_upgrade_guard(value)
    SELECT 1 FROM nextcloud_source_decommissions LIMIT 1;
DROP TABLE nextcloud_virgin_upgrade_guard;

CREATE TABLE nextcloud_pair_rebuild_history (operation_id TEXT PRIMARY KEY);
INSERT INTO nextcloud_pair_rebuild_history (operation_id)
    SELECT operation_id FROM nextcloud_source_pairings;
UPDATE nextcloud_source_virgin SET ever_touched = 1 WHERE ever_touched = 0;

CREATE TRIGGER nextcloud_pair_rebuild_remember
BEFORE DELETE ON nextcloud_source_pairings
BEGIN
    INSERT OR IGNORE INTO nextcloud_pair_rebuild_history(operation_id)
    VALUES (OLD.operation_id);
END;

DROP TRIGGER nextcloud_virgin_pair_insert;
CREATE TRIGGER nextcloud_virgin_pair_insert AFTER INSERT ON nextcloud_source_pairings
BEGIN
    INSERT INTO nextcloud_source_virgin(pair_operation_id, ever_touched)
    VALUES (NEW.operation_id, EXISTS (
        SELECT 1 FROM nextcloud_pair_rebuild_history
        WHERE operation_id = NEW.operation_id
    ));
END;

CREATE TRIGGER nextcloud_pair_rebuild_history_guard_update
BEFORE UPDATE ON nextcloud_pair_rebuild_history
BEGIN SELECT RAISE(ABORT, 'nextcloud_pair_rebuild_history_immutable'); END;
CREATE TRIGGER nextcloud_pair_rebuild_history_guard_delete
BEFORE DELETE ON nextcloud_pair_rebuild_history
BEGIN SELECT RAISE(ABORT, 'nextcloud_pair_rebuild_history_immutable'); END;
