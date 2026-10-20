-- Exact body storage manifest bound by the actual configuration producer.
-- This metadata is not a body, a read authorization, or a byte-release receipt.
CREATE TABLE knowledge_configuration_original_body_manifests (
 origin_id TEXT PRIMARY KEY,
 tenant_id BIGINT NOT NULL CHECK(tenant_id>0),
 knowledge_base_id TEXT NOT NULL,
 config_revision BIGINT NOT NULL CHECK(config_revision>0),
 config_digest TEXT NOT NULL CHECK(length(config_digest)=64),
 heads_json TEXT NOT NULL,empty_columns_json TEXT NOT NULL,parents_json TEXT NOT NULL,
 manifest_sha256 TEXT NOT NULL CHECK(length(manifest_sha256)=64),
 UNIQUE(tenant_id,knowledge_base_id,config_revision));
CREATE FUNCTION knowledge_config_body_manifest_guard() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
 IF TG_OP<>'INSERT' THEN RAISE EXCEPTION 'immutable_config_body_manifest'; END IF;
 IF NOT EXISTS(SELECT 1 FROM knowledge_config_input_origins o WHERE o.origin_id=NEW.origin_id AND o.tenant_id=NEW.tenant_id AND o.knowledge_base_id=NEW.knowledge_base_id AND o.config_revision=NEW.config_revision AND o.input_digest=NEW.config_digest) THEN RAISE EXCEPTION 'config_body_original_origin_missing'; END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER knowledge_config_body_manifest_guard BEFORE INSERT OR UPDATE OR DELETE ON knowledge_configuration_original_body_manifests FOR EACH ROW EXECUTE FUNCTION knowledge_config_body_manifest_guard();
