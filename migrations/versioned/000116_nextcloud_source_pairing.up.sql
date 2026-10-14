-- A source is admitted only after Nextcloud confirms the exact tuple. Legacy
-- Nextcloud sources have no row here and remain explicitly unpaired.
CREATE TABLE nextcloud_source_pairings (
    operation_id TEXT PRIMARY KEY,
    tenant_id BIGINT NOT NULL,
    knowledge_base_id TEXT NOT NULL UNIQUE,
    datasource_id TEXT NOT NULL UNIQUE REFERENCES data_sources (id) ON DELETE RESTRICT,
    nextcloud_instance_id TEXT NOT NULL,
    binding_id TEXT NOT NULL,
    datasource_base_url TEXT NOT NULL,
    datasource_config_sha256 TEXT NOT NULL,
    publication_epoch BIGINT NOT NULL CHECK (publication_epoch > 0),
    key_id TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('pending', 'active')),
    last_error_code TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (nextcloud_instance_id, binding_id)
);

-- Includes manual, scheduled, and event-triggered admissions. A direct SQL
-- status change cannot resume a pending or unpaired source either.
CREATE FUNCTION nextcloud_source_pairing_admit() RETURNS trigger
LANGUAGE plpgsql AS $nextcloud_pairing$
BEGIN
    IF TG_OP = 'UPDATE' THEN
        IF EXISTS (SELECT 1 FROM nextcloud_source_pairings p WHERE p.datasource_id = OLD.id)
           AND (NEW.config IS DISTINCT FROM OLD.config OR NEW.type IS DISTINCT FROM OLD.type
             OR NEW.tenant_id IS DISTINCT FROM OLD.tenant_id
             OR NEW.knowledge_base_id IS DISTINCT FROM OLD.knowledge_base_id
             OR NEW.deleted_at IS DISTINCT FROM OLD.deleted_at) THEN
            RAISE EXCEPTION 'nextcloud_paired_source_immutable' USING ERRCODE = '23514';
        END IF;
    END IF;
    IF NEW.type = 'nextcloud' AND NEW.status <> 'paused' AND NOT EXISTS (
        SELECT 1 FROM nextcloud_source_pairings p
        WHERE p.datasource_id = NEW.id AND p.tenant_id = NEW.tenant_id
          AND p.knowledge_base_id = NEW.knowledge_base_id AND p.state = 'active'
    ) THEN
        RAISE EXCEPTION 'nextcloud_source_unpaired' USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END
$nextcloud_pairing$;

CREATE TRIGGER nextcloud_source_pairing_admit_insert
BEFORE INSERT ON data_sources FOR EACH ROW EXECUTE FUNCTION nextcloud_source_pairing_admit();
CREATE TRIGGER nextcloud_source_pairing_admit_update
BEFORE UPDATE OF status, type, tenant_id, knowledge_base_id, config, deleted_at ON data_sources
FOR EACH ROW EXECUTE FUNCTION nextcloud_source_pairing_admit();

CREATE FUNCTION nextcloud_source_pairing_sync_admit() RETURNS trigger
LANGUAGE plpgsql AS $nextcloud_pairing_sync$
BEGIN
    IF NEW.status = 'running' AND EXISTS (
        SELECT 1 FROM data_sources WHERE id = NEW.data_source_id AND type = 'nextcloud'
    ) AND NOT EXISTS (
        SELECT 1 FROM nextcloud_source_pairings p
        WHERE p.datasource_id = NEW.data_source_id AND p.tenant_id = NEW.tenant_id
          AND p.state = 'active'
    ) THEN
        RAISE EXCEPTION 'nextcloud_source_unpaired' USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END
$nextcloud_pairing_sync$;
CREATE TRIGGER nextcloud_source_pairing_sync_insert
BEFORE INSERT ON sync_logs FOR EACH ROW EXECUTE FUNCTION nextcloud_source_pairing_sync_admit();
CREATE TRIGGER nextcloud_source_pairing_sync_update
BEFORE UPDATE OF status, data_source_id ON sync_logs
FOR EACH ROW EXECUTE FUNCTION nextcloud_source_pairing_sync_admit();

CREATE FUNCTION nextcloud_paired_kb_delete_guard() RETURNS trigger
LANGUAGE plpgsql AS $nextcloud_kb_guard$
BEGIN
    IF OLD.deleted_at IS NULL AND NEW.deleted_at IS NOT NULL AND EXISTS (
        SELECT 1 FROM nextcloud_source_pairings p WHERE p.knowledge_base_id = OLD.id
    ) THEN
        RAISE EXCEPTION 'nextcloud_paired_kb_delete_forbidden' USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END
$nextcloud_kb_guard$;
CREATE TRIGGER nextcloud_paired_kb_delete_guard
BEFORE UPDATE OF deleted_at ON knowledge_bases FOR EACH ROW
EXECUTE FUNCTION nextcloud_paired_kb_delete_guard();
