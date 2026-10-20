CREATE TABLE nextcloud_onboarding_credentials (
 id TEXT PRIMARY KEY,token_sha256 TEXT NOT NULL UNIQUE,instance_id TEXT NOT NULL,base_url TEXT NOT NULL,
 tenant_id BIGINT NOT NULL,directory_id TEXT NOT NULL,template_kb_id TEXT NOT NULL,
 enabled BOOLEAN NOT NULL DEFAULT FALSE,max_knowledge_bases INTEGER NOT NULL DEFAULT 100 CHECK(max_knowledge_bases>0));
CREATE TABLE nextcloud_onboarding_delegations (tenant_id BIGINT NOT NULL,directory_id TEXT NOT NULL,object_guid TEXT NOT NULL,PRIMARY KEY(tenant_id,directory_id,object_guid));
CREATE TABLE nextcloud_onboarding_operations (
 operation_id TEXT PRIMARY KEY,credential_id TEXT NOT NULL REFERENCES nextcloud_onboarding_credentials(id),
 tenant_id BIGINT NOT NULL,instance_id TEXT NOT NULL,binding_id TEXT NOT NULL,knowledge_base_id TEXT NOT NULL,
 actor_directory_id TEXT NOT NULL,actor_object_guid TEXT NOT NULL,actor_user_id TEXT NOT NULL,
 mode TEXT NOT NULL CHECK(mode IN ('create','existing')),name TEXT NOT NULL,processing_template_id TEXT NOT NULL,
 request_sha256 TEXT NOT NULL,state TEXT NOT NULL CHECK(state IN ('reserved','active','retired')),
 revision BIGINT NOT NULL DEFAULT 1,permission_version BIGINT NOT NULL DEFAULT 0,permission_sha256 TEXT NOT NULL DEFAULT '',permission_synced_at DATETIME,
 created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
 UNIQUE(tenant_id,knowledge_base_id),UNIQUE(instance_id,binding_id));
CREATE TABLE nextcloud_source_grants (
 operation_id TEXT NOT NULL REFERENCES nextcloud_onboarding_operations(operation_id),source_relation_id TEXT NOT NULL,
 kind TEXT NOT NULL CHECK(kind IN ('user','group')),directory_id TEXT NOT NULL,object_guid TEXT NOT NULL,
 tenant_id BIGINT NOT NULL,knowledge_base_id TEXT NOT NULL,principal_id TEXT NOT NULL,
 permission_version BIGINT NOT NULL,publication_epoch BIGINT NOT NULL CHECK(publication_epoch>=0),origin TEXT NOT NULL CHECK(origin='nextcloud'),
 PRIMARY KEY(operation_id,source_relation_id,kind,directory_id,object_guid));
CREATE INDEX nextcloud_source_grants_principal ON nextcloud_source_grants(principal_id,tenant_id,knowledge_base_id);
-- Existing KBs deliberately receive no witness. Zero current rows cannot certify unknown history.
CREATE TABLE nextcloud_kb_blank_witnesses (tenant_id BIGINT NOT NULL,knowledge_base_id TEXT NOT NULL,virgin BOOLEAN NOT NULL DEFAULT TRUE,PRIMARY KEY(tenant_id,knowledge_base_id));
CREATE TRIGGER nextcloud_onboarding_new_kb AFTER INSERT ON knowledge_bases BEGIN
 INSERT INTO nextcloud_kb_blank_witnesses(tenant_id,knowledge_base_id,virgin) VALUES(NEW.tenant_id,NEW.id,NEW.generated_profile IS NULL); END;
CREATE TRIGGER nextcloud_onboarding_witness_immutable_update BEFORE UPDATE ON nextcloud_kb_blank_witnesses WHEN OLD.virgin=0 AND NEW.virgin=1 BEGIN SELECT RAISE(ABORT,'nextcloud_blank_history_immutable'); END;
CREATE TRIGGER nextcloud_onboarding_witness_immutable_delete BEFORE DELETE ON nextcloud_kb_blank_witnesses BEGIN SELECT RAISE(ABORT,'nextcloud_blank_history_immutable'); END;
CREATE TRIGGER nextcloud_onboarding_guard_knowledges_insert BEFORE INSERT ON knowledges WHEN EXISTS(SELECT 1 FROM nextcloud_onboarding_operations o WHERE o.tenant_id=NEW.tenant_id AND o.knowledge_base_id=NEW.knowledge_base_id AND (o.state <> 'active' OR NEW.channel <> 'nextcloud')) BEGIN SELECT RAISE(ABORT,'nextcloud_kb_reserved'); END;
CREATE TRIGGER nextcloud_onboarding_touch_knowledges_insert AFTER INSERT ON knowledges BEGIN UPDATE nextcloud_kb_blank_witnesses SET virgin=0 WHERE tenant_id=NEW.tenant_id AND knowledge_base_id=NEW.knowledge_base_id; END;
CREATE TRIGGER nextcloud_onboarding_guard_knowledges_update BEFORE UPDATE OF tenant_id,knowledge_base_id ON knowledges WHEN EXISTS(SELECT 1 FROM nextcloud_onboarding_operations o WHERE o.tenant_id=NEW.tenant_id AND o.knowledge_base_id=NEW.knowledge_base_id AND (o.state <> 'active' OR NEW.channel <> 'nextcloud')) BEGIN SELECT RAISE(ABORT,'nextcloud_kb_reserved'); END;
CREATE TRIGGER nextcloud_onboarding_touch_knowledges_update AFTER UPDATE OF tenant_id,knowledge_base_id ON knowledges BEGIN UPDATE nextcloud_kb_blank_witnesses SET virgin=0 WHERE tenant_id=NEW.tenant_id AND knowledge_base_id=NEW.knowledge_base_id; END;
CREATE TRIGGER nextcloud_onboarding_guard_data_sources_insert BEFORE INSERT ON data_sources WHEN EXISTS(SELECT 1 FROM nextcloud_onboarding_operations o WHERE o.tenant_id=NEW.tenant_id AND o.knowledge_base_id=NEW.knowledge_base_id AND (NEW.type <> 'nextcloud' OR json_extract(NEW.config,'$.settings.onboarding_operation_id') IS NOT o.operation_id)) BEGIN SELECT RAISE(ABORT,'nextcloud_kb_reserved'); END;
CREATE TRIGGER nextcloud_onboarding_touch_data_sources_insert AFTER INSERT ON data_sources BEGIN UPDATE nextcloud_kb_blank_witnesses SET virgin=0 WHERE tenant_id=NEW.tenant_id AND knowledge_base_id=NEW.knowledge_base_id; END;
CREATE TRIGGER nextcloud_onboarding_guard_data_sources_update BEFORE UPDATE OF tenant_id,knowledge_base_id ON data_sources WHEN EXISTS(SELECT 1 FROM nextcloud_onboarding_operations o WHERE o.tenant_id=NEW.tenant_id AND o.knowledge_base_id=NEW.knowledge_base_id AND (NEW.type <> 'nextcloud' OR json_extract(NEW.config,'$.settings.onboarding_operation_id') IS NOT o.operation_id)) BEGIN SELECT RAISE(ABORT,'nextcloud_kb_reserved'); END;
CREATE TRIGGER nextcloud_onboarding_touch_data_sources_update AFTER UPDATE OF tenant_id,knowledge_base_id ON data_sources BEGIN UPDATE nextcloud_kb_blank_witnesses SET virgin=0 WHERE tenant_id=NEW.tenant_id AND knowledge_base_id=NEW.knowledge_base_id; END;
CREATE TRIGGER nextcloud_onboarding_guard_wiki_pages_insert BEFORE INSERT ON wiki_pages WHEN EXISTS(SELECT 1 FROM nextcloud_onboarding_operations o WHERE o.tenant_id=NEW.tenant_id AND o.knowledge_base_id=NEW.knowledge_base_id) BEGIN SELECT RAISE(ABORT,'nextcloud_kb_reserved'); END;
CREATE TRIGGER nextcloud_onboarding_touch_wiki_pages_insert AFTER INSERT ON wiki_pages BEGIN UPDATE nextcloud_kb_blank_witnesses SET virgin=0 WHERE tenant_id=NEW.tenant_id AND knowledge_base_id=NEW.knowledge_base_id; END;
CREATE TRIGGER nextcloud_onboarding_guard_wiki_pages_update BEFORE UPDATE OF tenant_id,knowledge_base_id ON wiki_pages WHEN EXISTS(SELECT 1 FROM nextcloud_onboarding_operations o WHERE o.tenant_id=NEW.tenant_id AND o.knowledge_base_id=NEW.knowledge_base_id) BEGIN SELECT RAISE(ABORT,'nextcloud_kb_reserved'); END;
CREATE TRIGGER nextcloud_onboarding_touch_wiki_pages_update AFTER UPDATE OF tenant_id,knowledge_base_id ON wiki_pages BEGIN UPDATE nextcloud_kb_blank_witnesses SET virgin=0 WHERE tenant_id=NEW.tenant_id AND knowledge_base_id=NEW.knowledge_base_id; END;
CREATE TRIGGER nextcloud_onboarding_guard_chunks_insert BEFORE INSERT ON chunks WHEN EXISTS(SELECT 1 FROM nextcloud_onboarding_operations o WHERE o.tenant_id=NEW.tenant_id AND o.knowledge_base_id=NEW.knowledge_base_id AND o.state <> 'active') BEGIN SELECT RAISE(ABORT,'nextcloud_kb_reserved'); END;
CREATE TRIGGER nextcloud_onboarding_touch_chunks_insert AFTER INSERT ON chunks BEGIN UPDATE nextcloud_kb_blank_witnesses SET virgin=0 WHERE tenant_id=NEW.tenant_id AND knowledge_base_id=NEW.knowledge_base_id; END;
CREATE TRIGGER nextcloud_onboarding_guard_chunks_update BEFORE UPDATE OF tenant_id,knowledge_base_id ON chunks WHEN EXISTS(SELECT 1 FROM nextcloud_onboarding_operations o WHERE o.tenant_id=NEW.tenant_id AND o.knowledge_base_id=NEW.knowledge_base_id AND o.state <> 'active') BEGIN SELECT RAISE(ABORT,'nextcloud_kb_reserved'); END;
CREATE TRIGGER nextcloud_onboarding_touch_chunks_update AFTER UPDATE OF tenant_id,knowledge_base_id ON chunks BEGIN UPDATE nextcloud_kb_blank_witnesses SET virgin=0 WHERE tenant_id=NEW.tenant_id AND knowledge_base_id=NEW.knowledge_base_id; END;
CREATE TRIGGER nextcloud_onboarding_profile_guard BEFORE UPDATE OF generated_profile ON knowledge_bases WHEN NEW.generated_profile IS NOT NULL AND EXISTS(SELECT 1 FROM nextcloud_onboarding_operations o WHERE o.tenant_id=NEW.tenant_id AND o.knowledge_base_id=NEW.id) BEGIN SELECT RAISE(ABORT,'nextcloud_kb_reserved'); END;
CREATE TRIGGER nextcloud_onboarding_profile_touch AFTER UPDATE OF generated_profile ON knowledge_bases WHEN NEW.generated_profile IS NOT NULL BEGIN UPDATE nextcloud_kb_blank_witnesses SET virgin=0 WHERE tenant_id=NEW.tenant_id AND knowledge_base_id=NEW.id; END;
ALTER TABLE sessions ADD COLUMN nextcloud_scoped BOOLEAN NOT NULL DEFAULT FALSE;
CREATE TRIGGER nextcloud_onboarding_operation_identity_guard BEFORE UPDATE ON nextcloud_onboarding_operations
WHEN NEW.operation_id IS NOT OLD.operation_id OR NEW.credential_id IS NOT OLD.credential_id OR NEW.tenant_id IS NOT OLD.tenant_id OR NEW.instance_id IS NOT OLD.instance_id OR NEW.binding_id IS NOT OLD.binding_id OR NEW.knowledge_base_id IS NOT OLD.knowledge_base_id OR NEW.actor_directory_id IS NOT OLD.actor_directory_id OR NEW.actor_object_guid IS NOT OLD.actor_object_guid OR NEW.actor_user_id IS NOT OLD.actor_user_id OR NEW.mode IS NOT OLD.mode OR NEW.name IS NOT OLD.name OR NEW.processing_template_id IS NOT OLD.processing_template_id OR NEW.request_sha256 IS NOT OLD.request_sha256
BEGIN SELECT RAISE(ABORT,'nextcloud_onboarding_identity_immutable'); END;
CREATE TRIGGER nextcloud_onboarding_operation_generation_guard BEFORE UPDATE ON nextcloud_onboarding_operations
WHEN (OLD.state='retired' AND NEW.state<>'retired') OR (OLD.state='active' AND NEW.state='reserved') OR NEW.permission_version<OLD.permission_version OR NEW.revision<OLD.revision
BEGIN SELECT RAISE(ABORT,'nextcloud_onboarding_generation_regression'); END;
CREATE TRIGGER nextcloud_onboarding_operation_activation_guard BEFORE UPDATE OF state ON nextcloud_onboarding_operations
WHEN NEW.state='active' AND NOT EXISTS(SELECT 1 FROM nextcloud_source_pairings p WHERE p.operation_id=NEW.operation_id AND p.tenant_id=NEW.tenant_id AND p.knowledge_base_id=NEW.knowledge_base_id AND p.nextcloud_instance_id=NEW.instance_id AND p.binding_id=NEW.binding_id AND p.state='active')
BEGIN SELECT RAISE(ABORT,'nextcloud_onboarding_unpaired'); END;
CREATE TRIGGER nextcloud_onboarding_grant_insert_guard BEFORE INSERT ON nextcloud_source_grants
WHEN NOT EXISTS(SELECT 1 FROM nextcloud_onboarding_operations o WHERE o.operation_id=NEW.operation_id AND o.tenant_id=NEW.tenant_id AND o.knowledge_base_id=NEW.knowledge_base_id AND o.actor_directory_id=NEW.directory_id AND o.state='active')
BEGIN SELECT RAISE(ABORT,'nextcloud_grant_scope_mismatch'); END;
CREATE TRIGGER nextcloud_onboarding_grant_update_guard BEFORE UPDATE ON nextcloud_source_grants
WHEN NOT EXISTS(SELECT 1 FROM nextcloud_onboarding_operations o WHERE o.operation_id=NEW.operation_id AND o.tenant_id=NEW.tenant_id AND o.knowledge_base_id=NEW.knowledge_base_id AND o.actor_directory_id=NEW.directory_id AND o.state='active')
BEGIN SELECT RAISE(ABORT,'nextcloud_grant_scope_mismatch'); END;
