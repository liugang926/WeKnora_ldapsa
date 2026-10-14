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
CREATE FUNCTION nextcloud_manager_purpose_immutable() RETURNS trigger AS $$ BEGIN RAISE EXCEPTION 'immutable_manager_purpose'; END; $$ LANGUAGE plpgsql;
CREATE TRIGGER nextcloud_manager_purpose_immutable BEFORE UPDATE OR DELETE ON nextcloud_onboarding_manager_purposes FOR EACH ROW EXECUTE FUNCTION nextcloud_manager_purpose_immutable();
CREATE FUNCTION nextcloud_manager_head_guard() RETURNS trigger AS $$ BEGIN
 IF TG_OP='DELETE' THEN RAISE EXCEPTION 'immutable_manager_head'; END IF;
 IF NOT EXISTS(SELECT 1 FROM nextcloud_onboarding_manager_purposes p WHERE p.purpose_id=NEW.purpose_id AND p.operation_id=NEW.operation_id AND p.version=NEW.version) THEN RAISE EXCEPTION 'unverified_manager_head'; END IF;
 IF TG_OP='UPDATE' AND (NEW.operation_id IS DISTINCT FROM OLD.operation_id OR NEW.version<=OLD.version OR NEW.purpose_id IS NOT DISTINCT FROM OLD.purpose_id OR NOT EXISTS(SELECT 1 FROM nextcloud_onboarding_manager_purposes p WHERE p.purpose_id=NEW.purpose_id AND p.previous_purpose_id=OLD.purpose_id)) THEN RAISE EXCEPTION 'manager_head_generation_regression'; END IF;
 RETURN NEW; END; $$ LANGUAGE plpgsql;
CREATE TRIGGER nextcloud_manager_head_guard BEFORE INSERT OR UPDATE OR DELETE ON nextcloud_onboarding_manager_heads FOR EACH ROW EXECUTE FUNCTION nextcloud_manager_head_guard();
