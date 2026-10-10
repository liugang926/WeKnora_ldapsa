CREATE TABLE nextcloud_control_recovery_gates (
 instance_id TEXT PRIMARY KEY,plan_sha256 TEXT NOT NULL,inventory_sha256 TEXT NOT NULL,
 closed BOOLEAN NOT NULL DEFAULT TRUE,closed_at DATETIME NOT NULL);
-- Durable proof/history cannot be edited to revive retired source generations.
CREATE TRIGGER nextcloud_root_proof_immutable BEFORE UPDATE ON nextcloud_root_migrations
WHEN NEW.operation_id IS NOT OLD.operation_id OR NEW.pair_operation_id IS NOT OLD.pair_operation_id OR NEW.tenant_id IS NOT OLD.tenant_id OR NEW.knowledge_base_id IS NOT OLD.knowledge_base_id OR NEW.datasource_id IS NOT OLD.datasource_id OR NEW.proof_json IS NOT OLD.proof_json OR NEW.proof_sha256 IS NOT OLD.proof_sha256 OR NEW.sync_was_enabled IS NOT OLD.sync_was_enabled OR NEW.operation_revision IS NOT OLD.operation_revision OR NEW.created_at IS NOT OLD.created_at OR (OLD.state='finalized' AND NEW.state!='finalized')
BEGIN SELECT RAISE(ABORT,'nextcloud_root_proof_immutable'); END;
CREATE TRIGGER nextcloud_root_proof_retained BEFORE DELETE ON nextcloud_root_migrations BEGIN SELECT RAISE(ABORT,'nextcloud_root_proof_immutable'); END;
CREATE TRIGGER nextcloud_target_history_immutable BEFORE UPDATE ON nextcloud_target_migrations
WHEN NEW.operation_id IS NOT OLD.operation_id OR NEW.old_operation_id IS NOT OLD.old_operation_id OR NEW.new_operation_id IS NOT OLD.new_operation_id OR NEW.old_tenant_id IS NOT OLD.old_tenant_id OR NEW.old_knowledge_base_id IS NOT OLD.old_knowledge_base_id OR NEW.old_data_source_id IS NOT OLD.old_data_source_id OR NEW.new_tenant_id IS NOT OLD.new_tenant_id OR NEW.new_knowledge_base_id IS NOT OLD.new_knowledge_base_id OR NEW.new_binding_id IS NOT OLD.new_binding_id OR NEW.created_at IS NOT OLD.created_at OR (OLD.state='active' AND (NEW.state!='active' OR NEW.manifest_sha256 IS NOT OLD.manifest_sha256))
BEGIN SELECT RAISE(ABORT,'nextcloud_target_history_immutable'); END;
CREATE TRIGGER nextcloud_target_history_retained BEFORE DELETE ON nextcloud_target_migrations BEGIN SELECT RAISE(ABORT,'nextcloud_target_history_immutable'); END;
CREATE TRIGGER nextcloud_target_old_source_closed BEFORE UPDATE ON data_sources
WHEN NEW.status='active' AND EXISTS(SELECT 1 FROM nextcloud_target_migrations t WHERE t.old_tenant_id=NEW.tenant_id AND t.old_data_source_id=NEW.id)
BEGIN SELECT RAISE(ABORT,'nextcloud_target_old_source_closed'); END;
