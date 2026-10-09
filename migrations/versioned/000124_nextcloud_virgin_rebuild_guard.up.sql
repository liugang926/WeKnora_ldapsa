-- Serialize the backfill with pair creation and JSONB repair. golang-migrate
-- sends this migration as one PostgreSQL query, so this lock lasts through
-- the trigger replacement and marker invalidation.
LOCK TABLE nextcloud_source_pairings IN SHARE ROW EXCLUSIVE MODE;

-- Migration 123 could not distinguish a newly created pair from an old pending
-- pair rebuilt by the PostgreSQL JSONB hash repair. Any proof admitted before
-- this migration is therefore unknown. A previously acknowledged withdrawal
-- needs manual review before the safe state can be established.
DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM nextcloud_source_decommissions) THEN
        RAISE EXCEPTION 'review existing Nextcloud decommissions before virgin-proof upgrade';
    END IF;
END $$;

CREATE TABLE nextcloud_pair_rebuild_history (
    operation_id TEXT PRIMARY KEY
);
INSERT INTO nextcloud_pair_rebuild_history (operation_id)
SELECT operation_id FROM nextcloud_source_pairings;
UPDATE nextcloud_source_virgin SET ever_touched = TRUE WHERE NOT ever_touched;

-- Keep lineage across a pending pair DELETE/INSERT repair. An operation ID
-- seen before this migration or before a later deletion never regains virgin
-- status just because its pairing row was recreated.
CREATE FUNCTION nextcloud_pair_rebuild_remember() RETURNS trigger
LANGUAGE plpgsql AS $nextcloud_pair_rebuild_remember$
BEGIN
    INSERT INTO nextcloud_pair_rebuild_history (operation_id)
    VALUES (OLD.operation_id) ON CONFLICT DO NOTHING;
    RETURN OLD;
END
$nextcloud_pair_rebuild_remember$;
CREATE TRIGGER nextcloud_pair_rebuild_remember
BEFORE DELETE ON nextcloud_source_pairings FOR EACH ROW
EXECUTE FUNCTION nextcloud_pair_rebuild_remember();

CREATE OR REPLACE FUNCTION nextcloud_virgin_pair_insert() RETURNS trigger
LANGUAGE plpgsql AS $nextcloud_virgin_pair$
BEGIN
    INSERT INTO nextcloud_source_virgin(pair_operation_id, ever_touched)
    VALUES (NEW.operation_id, EXISTS (
        SELECT 1 FROM nextcloud_pair_rebuild_history
        WHERE operation_id = NEW.operation_id
    ));
    RETURN NEW;
END
$nextcloud_virgin_pair$;

CREATE FUNCTION nextcloud_pair_rebuild_history_guard() RETURNS trigger
LANGUAGE plpgsql AS $nextcloud_pair_rebuild_history_guard$
BEGIN
    RAISE EXCEPTION 'nextcloud_pair_rebuild_history_immutable' USING ERRCODE = '23514';
END
$nextcloud_pair_rebuild_history_guard$;
CREATE TRIGGER nextcloud_pair_rebuild_history_guard
BEFORE UPDATE OR DELETE ON nextcloud_pair_rebuild_history FOR EACH ROW
EXECUTE FUNCTION nextcloud_pair_rebuild_history_guard();
