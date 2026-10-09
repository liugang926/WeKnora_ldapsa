-- Proof invalidation and pair lineage cannot be safely reversed once a pair
-- has existed. Only an empty installation may downgrade this guard.
DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM nextcloud_pair_rebuild_history)
       OR EXISTS (SELECT 1 FROM nextcloud_source_pairings)
       OR EXISTS (SELECT 1 FROM nextcloud_source_decommissions) THEN
        RAISE EXCEPTION 'cannot downgrade Nextcloud virgin-proof lineage';
    END IF;
END $$;
DROP TRIGGER IF EXISTS nextcloud_pair_rebuild_history_guard ON nextcloud_pair_rebuild_history;
DROP FUNCTION IF EXISTS nextcloud_pair_rebuild_history_guard();
DROP TRIGGER IF EXISTS nextcloud_pair_rebuild_remember ON nextcloud_source_pairings;
DROP FUNCTION IF EXISTS nextcloud_pair_rebuild_remember();
CREATE OR REPLACE FUNCTION nextcloud_virgin_pair_insert() RETURNS trigger
LANGUAGE plpgsql AS $nextcloud_virgin_pair$
BEGIN
    INSERT INTO nextcloud_source_virgin(pair_operation_id) VALUES (NEW.operation_id);
    RETURN NEW;
END
$nextcloud_virgin_pair$;
DROP TABLE nextcloud_pair_rebuild_history;
