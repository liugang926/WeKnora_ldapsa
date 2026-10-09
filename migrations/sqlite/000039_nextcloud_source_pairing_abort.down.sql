-- Rollback preserves evidence: operators must explicitly clear tombstones.
CREATE TABLE IF NOT EXISTS nextcloud_source_pairing_abort_down_guard (
    id INTEGER PRIMARY KEY CHECK (id = 0)
);
INSERT INTO nextcloud_source_pairing_abort_down_guard (id)
SELECT 1 WHERE EXISTS (SELECT 1 FROM nextcloud_source_pairing_aborts);
DROP TABLE nextcloud_source_pairing_abort_down_guard;
DROP TABLE nextcloud_source_pairing_aborts;
