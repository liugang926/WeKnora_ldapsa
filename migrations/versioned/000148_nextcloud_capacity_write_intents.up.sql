ALTER TABLE nextcloud_capacity_reservations ADD COLUMN protocol_version INTEGER NOT NULL DEFAULT 0;
CREATE TABLE nextcloud_capacity_write_intents (
 token TEXT PRIMARY KEY REFERENCES nextcloud_capacity_reservations(token) ON DELETE RESTRICT,
 knowledge_id TEXT NOT NULL,writer_id TEXT NOT NULL,
 physical_path TEXT NOT NULL UNIQUE,root_identity TEXT NOT NULL,scope_id TEXT NOT NULL,
 state TEXT NOT NULL CHECK(state IN ('prepared','writing','written','cataloged','failed','charged','released')),
 observed_bytes BIGINT NOT NULL DEFAULT 0 CHECK(observed_bytes>=0),
 object_identity TEXT NOT NULL DEFAULT '',sha256 TEXT NOT NULL DEFAULT '',resource_id TEXT NOT NULL DEFAULT '',
 orphan_charge_bytes BIGINT NOT NULL DEFAULT 0 CHECK(orphan_charge_bytes>=0),
 updated_at_ms BIGINT NOT NULL
);
CREATE FUNCTION nextcloud_capacity_intent_identity_immutable() RETURNS trigger LANGUAGE plpgsql AS $guard$ BEGIN
 IF TG_OP='DELETE' OR NEW.token IS DISTINCT FROM OLD.token OR NEW.knowledge_id IS DISTINCT FROM OLD.knowledge_id OR NEW.writer_id IS DISTINCT FROM OLD.writer_id OR NEW.physical_path IS DISTINCT FROM OLD.physical_path OR NEW.root_identity IS DISTINCT FROM OLD.root_identity OR NEW.scope_id IS DISTINCT FROM OLD.scope_id OR OLD.state IN ('charged','released') OR (OLD.object_identity<>'' AND NEW.object_identity<>OLD.object_identity) THEN RAISE EXCEPTION 'capacity_write_intent_immutable'; END IF; RETURN NEW;
END $guard$;
CREATE TRIGGER nextcloud_capacity_intent_identity BEFORE UPDATE OR DELETE ON nextcloud_capacity_write_intents FOR EACH ROW EXECUTE FUNCTION nextcloud_capacity_intent_identity_immutable();
