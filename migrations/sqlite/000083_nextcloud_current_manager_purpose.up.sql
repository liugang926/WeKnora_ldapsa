CREATE TABLE nextcloud_onboarding_manager_purposes (
 purpose_id TEXT PRIMARY KEY, operation_id TEXT NOT NULL REFERENCES nextcloud_onboarding_operations(operation_id),
 version BIGINT NOT NULL CHECK(version>0), previous_purpose_id TEXT NOT NULL,
 credential_id TEXT NOT NULL, credential_digest TEXT NOT NULL CHECK(length(credential_digest)=64),
 request_sha256 TEXT NOT NULL CHECK(length(request_sha256)=64), tenant_id BIGINT NOT NULL,knowledge_base_id TEXT NOT NULL,processing_template_id TEXT NOT NULL,datasource_id TEXT NOT NULL,
 instance_id TEXT NOT NULL,binding_id TEXT NOT NULL,config_sha256 TEXT NOT NULL CHECK(length(config_sha256)=64),
 actor_directory_id TEXT NOT NULL,actor_object_guid TEXT NOT NULL,
 action TEXT NOT NULL CHECK(action='pair'),action_digest TEXT NOT NULL CHECK(length(action_digest)=64),created_at_ms BIGINT NOT NULL CHECK(created_at_ms>0),
 UNIQUE(operation_id,version)
);
CREATE TABLE nextcloud_onboarding_manager_heads (
 operation_id TEXT PRIMARY KEY REFERENCES nextcloud_onboarding_operations(operation_id),
 purpose_id TEXT NOT NULL REFERENCES nextcloud_onboarding_manager_purposes(purpose_id),version BIGINT NOT NULL CHECK(version>0)
);
CREATE TRIGGER nextcloud_manager_purpose_update BEFORE UPDATE ON nextcloud_onboarding_manager_purposes BEGIN SELECT RAISE(ABORT,'immutable_manager_purpose'); END;
CREATE TRIGGER nextcloud_manager_purpose_delete BEFORE DELETE ON nextcloud_onboarding_manager_purposes BEGIN SELECT RAISE(ABORT,'immutable_manager_purpose'); END;
CREATE TRIGGER nextcloud_manager_head_insert BEFORE INSERT ON nextcloud_onboarding_manager_heads WHEN NOT EXISTS(SELECT 1 FROM nextcloud_onboarding_manager_purposes p WHERE p.purpose_id=NEW.purpose_id AND p.operation_id=NEW.operation_id AND p.version=NEW.version) BEGIN SELECT RAISE(ABORT,'unverified_manager_head'); END;
CREATE TRIGGER nextcloud_manager_head_update BEFORE UPDATE ON nextcloud_onboarding_manager_heads WHEN NEW.operation_id IS NOT OLD.operation_id OR NEW.version<=OLD.version OR NEW.purpose_id IS OLD.purpose_id OR NOT EXISTS(SELECT 1 FROM nextcloud_onboarding_manager_purposes p WHERE p.purpose_id=NEW.purpose_id AND p.operation_id=NEW.operation_id AND p.version=NEW.version) OR NOT EXISTS(SELECT 1 FROM nextcloud_onboarding_manager_purposes p WHERE p.purpose_id=NEW.purpose_id AND p.previous_purpose_id=OLD.purpose_id) BEGIN SELECT RAISE(ABORT,'manager_head_generation_regression'); END;
CREATE TRIGGER nextcloud_manager_head_delete BEFORE DELETE ON nextcloud_onboarding_manager_heads BEGIN SELECT RAISE(ABORT,'immutable_manager_head'); END;
