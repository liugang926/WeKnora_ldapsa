CREATE TABLE nextcloud_source_rotations (
    operation_id TEXT PRIMARY KEY,
    pair_operation_id TEXT NOT NULL REFERENCES nextcloud_source_pairings (operation_id) ON DELETE RESTRICT,
    tenant_id BIGINT NOT NULL,
    knowledge_base_id TEXT NOT NULL,
    datasource_id TEXT NOT NULL REFERENCES data_sources (id) ON DELETE RESTRICT,
    nextcloud_instance_id TEXT NOT NULL,
    binding_id TEXT NOT NULL,
    old_key_id TEXT NOT NULL,
    new_key_id TEXT NOT NULL UNIQUE,
    old_config_sha256 TEXT NOT NULL,
    new_config_sha256 TEXT NOT NULL,
    old_config JSONB NOT NULL,
    new_config JSONB NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('pending', 'committed', 'switched', 'finalized', 'aborted')),
    last_error_code TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE UNIQUE INDEX nextcloud_source_rotation_live_pair
    ON nextcloud_source_rotations (pair_operation_id)
    WHERE state IN ('pending', 'committed', 'switched');

-- The sole exception to paired-source immutability is the exact encrypted
-- config prepared in a rotation whose Nextcloud commit has been acknowledged.
CREATE OR REPLACE FUNCTION nextcloud_source_pairing_admit() RETURNS trigger
LANGUAGE plpgsql AS $nextcloud_pairing$
BEGIN
    IF TG_OP = 'UPDATE' THEN
        IF EXISTS (SELECT 1 FROM nextcloud_source_pairings p WHERE p.datasource_id = OLD.id)
           AND (NEW.type IS DISTINCT FROM OLD.type
             OR NEW.tenant_id IS DISTINCT FROM OLD.tenant_id
             OR NEW.knowledge_base_id IS DISTINCT FROM OLD.knowledge_base_id
             OR NEW.deleted_at IS DISTINCT FROM OLD.deleted_at
             OR (NEW.config IS DISTINCT FROM OLD.config AND NOT EXISTS (
                 SELECT 1 FROM nextcloud_source_rotations r
                 WHERE r.datasource_id = OLD.id AND r.state = 'committed'
                   AND r.old_config = OLD.config AND r.new_config = NEW.config
             ))) THEN
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
       OR NEW.publication_epoch IS DISTINCT FROM OLD.publication_epoch
       OR (OLD.state = 'active' AND NEW.state IS DISTINCT FROM OLD.state)
       OR ((NEW.datasource_config_sha256 IS DISTINCT FROM OLD.datasource_config_sha256
         OR NEW.key_id IS DISTINCT FROM OLD.key_id) AND NOT EXISTS (
           SELECT 1 FROM nextcloud_source_rotations r
           WHERE r.pair_operation_id = OLD.operation_id AND r.state = 'committed'
             AND r.old_key_id = OLD.key_id AND r.new_key_id = NEW.key_id
             AND r.old_config_sha256 = OLD.datasource_config_sha256
             AND r.new_config_sha256 = NEW.datasource_config_sha256
       )) THEN
        RAISE EXCEPTION 'nextcloud_source_pairing_immutable' USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END
$nextcloud_pair_row$;
