CREATE TABLE nextcloud_source_rotations (
    operation_id TEXT PRIMARY KEY,
    pair_operation_id TEXT NOT NULL REFERENCES nextcloud_source_pairings (operation_id) ON DELETE RESTRICT,
    tenant_id INTEGER NOT NULL,
    knowledge_base_id TEXT NOT NULL,
    datasource_id TEXT NOT NULL REFERENCES data_sources (id) ON DELETE RESTRICT,
    nextcloud_instance_id TEXT NOT NULL,
    binding_id TEXT NOT NULL,
    old_key_id TEXT NOT NULL,
    new_key_id TEXT NOT NULL UNIQUE,
    old_config_sha256 TEXT NOT NULL,
    new_config_sha256 TEXT NOT NULL,
    old_config TEXT NOT NULL,
    new_config TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('pending', 'committed', 'switched', 'finalized', 'aborted')),
    last_error_code TEXT NOT NULL DEFAULT '',
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE UNIQUE INDEX nextcloud_source_rotation_live_pair
    ON nextcloud_source_rotations (pair_operation_id)
    WHERE state IN ('pending', 'committed', 'switched');

DROP TRIGGER nextcloud_source_pairing_immutable_update;
CREATE TRIGGER nextcloud_source_pairing_immutable_update
BEFORE UPDATE OF config, type, tenant_id, knowledge_base_id, deleted_at ON data_sources
WHEN EXISTS (SELECT 1 FROM nextcloud_source_pairings p WHERE p.datasource_id = OLD.id)
 AND (NEW.type IS NOT OLD.type OR NEW.tenant_id IS NOT OLD.tenant_id
  OR NEW.knowledge_base_id IS NOT OLD.knowledge_base_id OR NEW.deleted_at IS NOT OLD.deleted_at
  OR (NEW.config IS NOT OLD.config AND NOT EXISTS (
      SELECT 1 FROM nextcloud_source_rotations r
      WHERE r.datasource_id = OLD.id AND r.state = 'committed'
        AND r.old_config = OLD.config AND r.new_config = NEW.config
  )))
BEGIN SELECT RAISE(ABORT, 'nextcloud_paired_source_immutable'); END;

DROP TRIGGER nextcloud_source_pairing_row_guard_update;
CREATE TRIGGER nextcloud_source_pairing_row_guard_update
BEFORE UPDATE ON nextcloud_source_pairings
WHEN NEW.operation_id IS NOT OLD.operation_id
  OR NEW.tenant_id IS NOT OLD.tenant_id
  OR NEW.knowledge_base_id IS NOT OLD.knowledge_base_id
  OR NEW.datasource_id IS NOT OLD.datasource_id
  OR NEW.nextcloud_instance_id IS NOT OLD.nextcloud_instance_id
  OR NEW.binding_id IS NOT OLD.binding_id
  OR NEW.datasource_base_url IS NOT OLD.datasource_base_url
  OR NEW.publication_epoch IS NOT OLD.publication_epoch
  OR (OLD.state = 'active' AND NEW.state IS NOT OLD.state)
  OR ((NEW.datasource_config_sha256 IS NOT OLD.datasource_config_sha256
    OR NEW.key_id IS NOT OLD.key_id) AND NOT EXISTS (
      SELECT 1 FROM nextcloud_source_rotations r
      WHERE r.pair_operation_id = OLD.operation_id AND r.state = 'committed'
        AND r.old_key_id = OLD.key_id AND r.new_key_id = NEW.key_id
        AND r.old_config_sha256 = OLD.datasource_config_sha256
        AND r.new_config_sha256 = NEW.datasource_config_sha256
  ))
BEGIN SELECT RAISE(ABORT, 'nextcloud_source_pairing_immutable'); END;
