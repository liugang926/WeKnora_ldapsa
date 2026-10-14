CREATE TEMP TABLE nextcloud_source_rotation_rollback_guard (count INTEGER NOT NULL CHECK (count = 0));
INSERT INTO nextcloud_source_rotation_rollback_guard SELECT COUNT(*) FROM nextcloud_source_rotations;
DROP TABLE nextcloud_source_rotation_rollback_guard;

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
  OR NEW.datasource_config_sha256 IS NOT OLD.datasource_config_sha256
  OR NEW.publication_epoch IS NOT OLD.publication_epoch
  OR NEW.key_id IS NOT OLD.key_id
  OR (OLD.state = 'active' AND NEW.state IS NOT OLD.state)
BEGIN SELECT RAISE(ABORT, 'nextcloud_source_pairing_immutable'); END;

DROP TRIGGER nextcloud_source_pairing_immutable_update;
CREATE TRIGGER nextcloud_source_pairing_immutable_update
BEFORE UPDATE OF config, type, tenant_id, knowledge_base_id, deleted_at ON data_sources
WHEN EXISTS (SELECT 1 FROM nextcloud_source_pairings p WHERE p.datasource_id = OLD.id)
 AND (NEW.config IS NOT OLD.config OR NEW.type IS NOT OLD.type
  OR NEW.tenant_id IS NOT OLD.tenant_id OR NEW.knowledge_base_id IS NOT OLD.knowledge_base_id
  OR NEW.deleted_at IS NOT OLD.deleted_at)
BEGIN SELECT RAISE(ABORT, 'nextcloud_paired_source_immutable'); END;

DROP TABLE nextcloud_source_rotations;
