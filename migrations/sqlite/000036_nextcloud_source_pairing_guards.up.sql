-- SQLite cannot alter a CHECK constraint in place. Rebuild the pairing table
-- while preserving all intent rows and their unique constraints.
CREATE TABLE nextcloud_source_pairings_v36 (
    operation_id TEXT PRIMARY KEY,
    tenant_id INTEGER NOT NULL,
    knowledge_base_id TEXT NOT NULL UNIQUE,
    datasource_id TEXT NOT NULL UNIQUE REFERENCES data_sources (id) ON DELETE RESTRICT,
    nextcloud_instance_id TEXT NOT NULL,
    binding_id TEXT NOT NULL,
    datasource_base_url TEXT NOT NULL,
    datasource_config_sha256 TEXT NOT NULL,
    publication_epoch INTEGER NOT NULL CHECK (publication_epoch >= 0),
    key_id TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('pending', 'active')),
    last_error_code TEXT NOT NULL DEFAULT '',
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (nextcloud_instance_id, binding_id)
);
INSERT INTO nextcloud_source_pairings_v36
    (operation_id, tenant_id, knowledge_base_id, datasource_id, nextcloud_instance_id,
     binding_id, datasource_base_url, datasource_config_sha256, publication_epoch,
     key_id, state, last_error_code, created_at, updated_at)
SELECT operation_id, tenant_id, knowledge_base_id, datasource_id, nextcloud_instance_id,
       binding_id, datasource_base_url, datasource_config_sha256, publication_epoch,
       key_id, state, last_error_code, created_at, updated_at
FROM nextcloud_source_pairings;
DROP TRIGGER IF EXISTS nextcloud_paired_kb_delete_guard;
DROP TRIGGER IF EXISTS nextcloud_source_pairing_sync_update;
DROP TRIGGER IF EXISTS nextcloud_source_pairing_sync_insert;
DROP TRIGGER IF EXISTS nextcloud_source_pairing_immutable_update;
DROP TRIGGER IF EXISTS nextcloud_source_pairing_admit_update;
DROP TRIGGER IF EXISTS nextcloud_source_pairing_admit_insert;
DROP TABLE nextcloud_source_pairings;
ALTER TABLE nextcloud_source_pairings_v36 RENAME TO nextcloud_source_pairings;

-- Reinstall the version 35 guards after the referenced table exists again.
CREATE TRIGGER nextcloud_source_pairing_admit_insert
BEFORE INSERT ON data_sources
WHEN NEW.type = 'nextcloud' AND NEW.status <> 'paused' AND NOT EXISTS (
    SELECT 1 FROM nextcloud_source_pairings p WHERE p.datasource_id = NEW.id
      AND p.tenant_id = NEW.tenant_id AND p.knowledge_base_id = NEW.knowledge_base_id
      AND p.state = 'active')
BEGIN SELECT RAISE(ABORT, 'nextcloud_source_unpaired'); END;

CREATE TRIGGER nextcloud_source_pairing_admit_update
BEFORE UPDATE OF status, type, tenant_id, knowledge_base_id, config, deleted_at ON data_sources
WHEN NEW.type = 'nextcloud' AND NEW.status <> 'paused' AND NOT EXISTS (
    SELECT 1 FROM nextcloud_source_pairings p WHERE p.datasource_id = NEW.id
      AND p.tenant_id = NEW.tenant_id AND p.knowledge_base_id = NEW.knowledge_base_id
      AND p.state = 'active')
BEGIN SELECT RAISE(ABORT, 'nextcloud_source_unpaired'); END;

CREATE TRIGGER nextcloud_source_pairing_immutable_update
BEFORE UPDATE OF config, type, tenant_id, knowledge_base_id, deleted_at ON data_sources
WHEN EXISTS (SELECT 1 FROM nextcloud_source_pairings p WHERE p.datasource_id = OLD.id)
 AND (NEW.config IS NOT OLD.config OR NEW.type IS NOT OLD.type
  OR NEW.tenant_id IS NOT OLD.tenant_id OR NEW.knowledge_base_id IS NOT OLD.knowledge_base_id
  OR NEW.deleted_at IS NOT OLD.deleted_at)
BEGIN SELECT RAISE(ABORT, 'nextcloud_paired_source_immutable'); END;

CREATE TRIGGER nextcloud_source_pairing_sync_insert
BEFORE INSERT ON sync_logs
WHEN NEW.status = 'running'
 AND EXISTS (SELECT 1 FROM data_sources WHERE id = NEW.data_source_id AND type = 'nextcloud')
 AND NOT EXISTS (SELECT 1 FROM nextcloud_source_pairings p
     WHERE p.datasource_id = NEW.data_source_id AND p.tenant_id = NEW.tenant_id
       AND p.state = 'active')
BEGIN SELECT RAISE(ABORT, 'nextcloud_source_unpaired'); END;

CREATE TRIGGER nextcloud_source_pairing_sync_update
BEFORE UPDATE OF status, data_source_id ON sync_logs
WHEN NEW.status = 'running'
 AND EXISTS (SELECT 1 FROM data_sources WHERE id = NEW.data_source_id AND type = 'nextcloud')
 AND NOT EXISTS (SELECT 1 FROM nextcloud_source_pairings p
     WHERE p.datasource_id = NEW.data_source_id AND p.tenant_id = NEW.tenant_id
       AND p.state = 'active')
BEGIN SELECT RAISE(ABORT, 'nextcloud_source_unpaired'); END;

CREATE TRIGGER nextcloud_paired_kb_delete_guard
BEFORE UPDATE OF deleted_at ON knowledge_bases
WHEN OLD.deleted_at IS NULL AND NEW.deleted_at IS NOT NULL
 AND EXISTS (SELECT 1 FROM nextcloud_source_pairings p WHERE p.knowledge_base_id = OLD.id)
BEGIN SELECT RAISE(ABORT, 'nextcloud_paired_kb_delete_forbidden'); END;

CREATE TRIGGER nextcloud_paired_kb_hard_delete_guard
BEFORE DELETE ON knowledge_bases
WHEN EXISTS (SELECT 1 FROM nextcloud_source_pairings p WHERE p.knowledge_base_id = OLD.id)
BEGIN SELECT RAISE(ABORT, 'nextcloud_paired_kb_delete_forbidden'); END;

CREATE TRIGGER nextcloud_source_pairing_row_guard_update
BEFORE UPDATE ON nextcloud_source_pairings
WHEN NEW.operation_id IS NOT OLD.operation_id
  OR NEW.tenant_id IS NOT OLD.tenant_id
  OR NEW.knowledge_base_id IS NOT OLD.knowledge_base_id
  OR NEW.datasource_id IS NOT OLD.datasource_id
  OR NEW.nextcloud_instance_id IS NOT OLD.nextcloud_instance_id
  OR NEW.binding_id IS NOT OLD.binding_id
  OR NEW.datasource_base_url IS NOT OLD.datasource_base_url
  OR NEW.datasource_config_sha256 IS NOT OLD.datasource_config_sha256
  OR NEW.publication_epoch IS NOT OLD.publication_epoch
  OR NEW.key_id IS NOT OLD.key_id
  OR (OLD.state = 'active' AND NEW.state IS NOT OLD.state)
BEGIN SELECT RAISE(ABORT, 'nextcloud_source_pairing_immutable'); END;

CREATE TRIGGER nextcloud_source_pairing_row_guard_delete
BEFORE DELETE ON nextcloud_source_pairings
WHEN OLD.state = 'active' OR EXISTS (
    SELECT 1 FROM data_sources d WHERE d.id = OLD.datasource_id AND d.status <> 'paused'
)
BEGIN SELECT RAISE(ABORT, 'nextcloud_source_pairing_delete_forbidden'); END;
