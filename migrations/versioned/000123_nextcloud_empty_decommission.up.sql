-- Only pairs created after this migration have a durable never-touched proof.
-- Existing pairs are deliberately ineligible: historical sync logs may have
-- been pruned, so an empty current KB does not prove no old derived copies.
CREATE TABLE nextcloud_source_virgin (
    pair_operation_id TEXT PRIMARY KEY REFERENCES nextcloud_source_pairings(operation_id) ON DELETE CASCADE,
    ever_touched BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE TABLE nextcloud_source_decommissions (
    operation_id TEXT PRIMARY KEY,
    pair_operation_id TEXT NOT NULL UNIQUE REFERENCES nextcloud_source_pairings(operation_id) ON DELETE RESTRICT,
    tenant_id BIGINT NOT NULL,
    knowledge_base_id TEXT NOT NULL UNIQUE,
    datasource_id TEXT NOT NULL UNIQUE,
    nextcloud_instance_id TEXT NOT NULL,
    binding_id TEXT NOT NULL,
    publication_epoch BIGINT NOT NULL CHECK (publication_epoch >= 0),
    key_id TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('prepared', 'acknowledged')),
    inventory_sha256 TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (nextcloud_instance_id, binding_id)
);

CREATE FUNCTION nextcloud_virgin_pair_insert() RETURNS trigger
LANGUAGE plpgsql AS $nextcloud_virgin_pair$
BEGIN
    INSERT INTO nextcloud_source_virgin(pair_operation_id) VALUES (NEW.operation_id);
    RETURN NEW;
END
$nextcloud_virgin_pair$;
CREATE TRIGGER nextcloud_virgin_pair_insert
AFTER INSERT ON nextcloud_source_pairings FOR EACH ROW
EXECUTE FUNCTION nextcloud_virgin_pair_insert();

-- Every legitimate first content or event-admission path burns the virgin
-- proof in the same transaction. Locking the proof serializes each writer
-- with the decommission compare-and-swap.
CREATE FUNCTION nextcloud_virgin_touch() RETURNS trigger
LANGUAGE plpgsql AS $nextcloud_virgin_touch$
DECLARE
    pair_id TEXT;
    touched BOOLEAN;
BEGIN
    IF TG_TABLE_NAME = 'sync_logs' THEN
        SELECT operation_id INTO pair_id FROM nextcloud_source_pairings
        WHERE datasource_id = NEW.data_source_id;
    ELSIF TG_TABLE_NAME IN ('nextcloud_source_versions', 'nextcloud_event_connections') THEN
        SELECT operation_id INTO pair_id FROM nextcloud_source_pairings
        WHERE datasource_id = NEW.datasource_id;
    ELSE
        SELECT operation_id INTO pair_id FROM nextcloud_source_pairings
        WHERE knowledge_base_id = NEW.knowledge_base_id;
    END IF;
    IF pair_id IS NULL THEN
        RETURN NEW;
    END IF;
    SELECT ever_touched INTO touched FROM nextcloud_source_virgin
    WHERE pair_operation_id = pair_id FOR UPDATE;
    IF EXISTS (SELECT 1 FROM nextcloud_source_decommissions WHERE pair_operation_id = pair_id) THEN
        RAISE EXCEPTION 'nextcloud_source_decommissioned' USING ERRCODE = '23514';
    END IF;
    IF touched IS NOT NULL THEN
        UPDATE nextcloud_source_virgin SET ever_touched = TRUE WHERE pair_operation_id = pair_id;
    END IF;
    RETURN NEW;
END
$nextcloud_virgin_touch$;
CREATE TRIGGER nextcloud_virgin_sync_log
BEFORE INSERT ON sync_logs FOR EACH ROW EXECUTE FUNCTION nextcloud_virgin_touch();
CREATE TRIGGER nextcloud_virgin_knowledge
BEFORE INSERT ON knowledges FOR EACH ROW EXECUTE FUNCTION nextcloud_virgin_touch();
CREATE TRIGGER nextcloud_virgin_chunk
BEFORE INSERT ON chunks FOR EACH ROW EXECUTE FUNCTION nextcloud_virgin_touch();
CREATE TRIGGER nextcloud_virgin_version
BEFORE INSERT ON nextcloud_source_versions FOR EACH ROW EXECUTE FUNCTION nextcloud_virgin_touch();
CREATE TRIGGER nextcloud_virgin_event_connection
BEFORE INSERT ON nextcloud_event_connections FOR EACH ROW EXECUTE FUNCTION nextcloud_virgin_touch();

CREATE FUNCTION nextcloud_decommission_resume_guard() RETURNS trigger
LANGUAGE plpgsql AS $nextcloud_decommission_resume$
BEGIN
    IF NEW.status <> 'paused' AND EXISTS (
        SELECT 1 FROM nextcloud_source_decommissions WHERE datasource_id = NEW.id
    ) THEN
        RAISE EXCEPTION 'nextcloud_source_decommissioned' USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END
$nextcloud_decommission_resume$;
CREATE TRIGGER nextcloud_decommission_resume_guard
BEFORE UPDATE OF status ON data_sources FOR EACH ROW
EXECUTE FUNCTION nextcloud_decommission_resume_guard();

CREATE FUNCTION nextcloud_decommission_row_guard() RETURNS trigger
LANGUAGE plpgsql AS $nextcloud_decommission_row$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'nextcloud_source_decommission_immutable' USING ERRCODE = '23514';
    END IF;
    IF NEW.operation_id IS DISTINCT FROM OLD.operation_id
       OR NEW.pair_operation_id IS DISTINCT FROM OLD.pair_operation_id
       OR NEW.tenant_id IS DISTINCT FROM OLD.tenant_id
       OR NEW.knowledge_base_id IS DISTINCT FROM OLD.knowledge_base_id
       OR NEW.datasource_id IS DISTINCT FROM OLD.datasource_id
       OR NEW.nextcloud_instance_id IS DISTINCT FROM OLD.nextcloud_instance_id
       OR NEW.binding_id IS DISTINCT FROM OLD.binding_id
       OR NEW.publication_epoch IS DISTINCT FROM OLD.publication_epoch
       OR NEW.key_id IS DISTINCT FROM OLD.key_id
       OR (OLD.state <> 'prepared' AND NEW.state IS DISTINCT FROM OLD.state)
       OR (NEW.state = 'prepared' AND NEW.inventory_sha256 <> '')
       OR (NEW.state = 'acknowledged'
           AND NEW.inventory_sha256 <> 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855')
       OR (OLD.state = 'acknowledged' AND NEW.inventory_sha256 IS DISTINCT FROM OLD.inventory_sha256) THEN
        RAISE EXCEPTION 'nextcloud_source_decommission_immutable' USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END
$nextcloud_decommission_row$;
CREATE TRIGGER nextcloud_decommission_row_guard
BEFORE UPDATE OR DELETE ON nextcloud_source_decommissions FOR EACH ROW
EXECUTE FUNCTION nextcloud_decommission_row_guard();

CREATE FUNCTION nextcloud_virgin_row_guard() RETURNS trigger
LANGUAGE plpgsql AS $nextcloud_virgin_row$
BEGIN
    IF TG_OP = 'UPDATE' AND (NEW.pair_operation_id IS DISTINCT FROM OLD.pair_operation_id
        OR (OLD.ever_touched AND NOT NEW.ever_touched)) THEN
        RAISE EXCEPTION 'nextcloud_virgin_proof_immutable' USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END
$nextcloud_virgin_row$;
CREATE TRIGGER nextcloud_virgin_row_guard
BEFORE UPDATE ON nextcloud_source_virgin FOR EACH ROW
EXECUTE FUNCTION nextcloud_virgin_row_guard();
