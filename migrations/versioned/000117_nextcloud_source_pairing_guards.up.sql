-- A newly published Nextcloud binding starts at epoch zero. Preserve that
-- exact epoch in the pinned source intent instead of inventing an epoch one.
ALTER TABLE nextcloud_source_pairings
    DROP CONSTRAINT nextcloud_source_pairings_publication_epoch_check,
    ADD CONSTRAINT nextcloud_source_pairings_publication_epoch_check
    CHECK (publication_epoch >= 0);

CREATE FUNCTION nextcloud_paired_kb_hard_delete_guard() RETURNS trigger
LANGUAGE plpgsql AS $nextcloud_kb_hard_delete$
BEGIN
    IF EXISTS (SELECT 1 FROM nextcloud_source_pairings p WHERE p.knowledge_base_id = OLD.id) THEN
        RAISE EXCEPTION 'nextcloud_paired_kb_delete_forbidden' USING ERRCODE = '23514';
    END IF;
    RETURN OLD;
END
$nextcloud_kb_hard_delete$;

CREATE TRIGGER nextcloud_paired_kb_hard_delete_guard
BEFORE DELETE ON knowledge_bases FOR EACH ROW
EXECUTE FUNCTION nextcloud_paired_kb_hard_delete_guard();

-- The tuple acknowledged by Nextcloud is immutable. A pending intent may be
-- discarded only while its source is paused; an active pair is permanent.
CREATE FUNCTION nextcloud_source_pairing_row_guard() RETURNS trigger
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

CREATE TRIGGER nextcloud_source_pairing_row_guard_update
BEFORE UPDATE ON nextcloud_source_pairings FOR EACH ROW
EXECUTE FUNCTION nextcloud_source_pairing_row_guard();

CREATE TRIGGER nextcloud_source_pairing_row_guard_delete
BEFORE DELETE ON nextcloud_source_pairings FOR EACH ROW
EXECUTE FUNCTION nextcloud_source_pairing_row_guard();
