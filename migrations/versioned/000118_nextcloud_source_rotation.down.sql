DO $nextcloud_rotation_rollback$
BEGIN
    IF EXISTS (SELECT 1 FROM nextcloud_source_rotations) THEN
        RAISE EXCEPTION 'cannot roll back source rotation with persisted operations';
    END IF;
END
$nextcloud_rotation_rollback$;
DROP TABLE nextcloud_source_rotations;

CREATE OR REPLACE FUNCTION nextcloud_source_pairing_admit() RETURNS trigger
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

CREATE OR REPLACE FUNCTION nextcloud_source_pairing_row_guard() RETURNS trigger
LANGUAGE plpgsql AS $nextcloud_pair_row$
BEGIN
    IF TG_OP = 'DELETE' THEN
        IF OLD.state = 'active' OR EXISTS (
            SELECT 1 FROM data_sources d
            WHERE d.id = OLD.datasource_id AND d.status <> 'paused'
        ) THEN
            RAISE EXCEPTION 'nextcloud_source_pairing_delete_forbidden' USING ERRCODE = '23514';
        END IF;
        RETURN OLD;
    END IF;
    IF NEW.operation_id IS DISTINCT FROM OLD.operation_id
       OR NEW.tenant_id IS DISTINCT FROM OLD.tenant_id
       OR NEW.knowledge_base_id IS DISTINCT FROM OLD.knowledge_base_id
       OR NEW.datasource_id IS DISTINCT FROM OLD.datasource_id
       OR NEW.nextcloud_instance_id IS DISTINCT FROM OLD.nextcloud_instance_id
       OR NEW.binding_id IS DISTINCT FROM OLD.binding_id
       OR NEW.datasource_base_url IS DISTINCT FROM OLD.datasource_base_url
       OR NEW.datasource_config_sha256 IS DISTINCT FROM OLD.datasource_config_sha256
       OR NEW.publication_epoch IS DISTINCT FROM OLD.publication_epoch
       OR NEW.key_id IS DISTINCT FROM OLD.key_id
       OR (OLD.state = 'active' AND NEW.state IS DISTINCT FROM OLD.state) THEN
        RAISE EXCEPTION 'nextcloud_source_pairing_immutable' USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END
$nextcloud_pair_row$;
