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
 revision BIGINT NOT NULL DEFAULT 1,permission_version BIGINT NOT NULL DEFAULT 0,permission_sha256 TEXT NOT NULL DEFAULT '',permission_synced_at TIMESTAMPTZ,
 created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
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
CREATE FUNCTION nextcloud_onboarding_new_kb() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN INSERT INTO nextcloud_kb_blank_witnesses(tenant_id,knowledge_base_id,virgin) VALUES(NEW.tenant_id,NEW.id,NEW.generated_profile IS NULL); RETURN NEW; END $$;
CREATE TRIGGER nextcloud_onboarding_new_kb AFTER INSERT ON knowledge_bases FOR EACH ROW EXECUTE FUNCTION nextcloud_onboarding_new_kb();
CREATE FUNCTION nextcloud_onboarding_witness_immutable() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN IF TG_OP='DELETE' OR (OLD.virgin=FALSE AND NEW.virgin=TRUE) THEN RAISE EXCEPTION 'nextcloud_blank_history_immutable'; END IF; RETURN NEW; END $$;
CREATE TRIGGER nextcloud_onboarding_witness_immutable BEFORE UPDATE OR DELETE ON nextcloud_kb_blank_witnesses FOR EACH ROW EXECUTE FUNCTION nextcloud_onboarding_witness_immutable();
CREATE FUNCTION nextcloud_onboarding_content_guard() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE op nextcloud_onboarding_operations; BEGIN
 -- The same owning KB writer lock is held by reservation, preventing a
 -- preflight transaction from committing behind a concurrently admitted write.
 UPDATE knowledge_bases SET ever_had_nextcloud_source=ever_had_nextcloud_source WHERE id=NEW.knowledge_base_id AND tenant_id=NEW.tenant_id;
 SELECT * INTO op FROM nextcloud_onboarding_operations WHERE tenant_id=NEW.tenant_id AND knowledge_base_id=NEW.knowledge_base_id;
 IF FOUND THEN
  IF TG_TABLE_NAME='data_sources' THEN
   IF NEW.type <> 'nextcloud' OR (NEW.config::jsonb->'settings'->>'onboarding_operation_id') IS DISTINCT FROM op.operation_id THEN RAISE EXCEPTION 'nextcloud_kb_reserved'; END IF;
  ELSIF op.state <> 'active' THEN RAISE EXCEPTION 'nextcloud_kb_reserved';
  ELSIF TG_TABLE_NAME='knowledges' THEN IF NEW.channel <> 'nextcloud' THEN RAISE EXCEPTION 'nextcloud_kb_reserved'; END IF;
  ELSIF TG_TABLE_NAME='wiki_pages' THEN RAISE EXCEPTION 'nextcloud_wiki_unsupported'; END IF;
 END IF;
 UPDATE nextcloud_kb_blank_witnesses SET virgin=FALSE WHERE tenant_id=NEW.tenant_id AND knowledge_base_id=NEW.knowledge_base_id;
 RETURN NEW; END $$;
CREATE TRIGGER nextcloud_onboarding_guard_knowledges BEFORE INSERT OR UPDATE OF tenant_id,knowledge_base_id ON knowledges FOR EACH ROW EXECUTE FUNCTION nextcloud_onboarding_content_guard();
CREATE TRIGGER nextcloud_onboarding_guard_data_sources BEFORE INSERT OR UPDATE OF tenant_id,knowledge_base_id ON data_sources FOR EACH ROW EXECUTE FUNCTION nextcloud_onboarding_content_guard();
CREATE TRIGGER nextcloud_onboarding_guard_wiki_pages BEFORE INSERT OR UPDATE OF tenant_id,knowledge_base_id ON wiki_pages FOR EACH ROW EXECUTE FUNCTION nextcloud_onboarding_content_guard();
CREATE TRIGGER nextcloud_onboarding_guard_chunks BEFORE INSERT OR UPDATE OF tenant_id,knowledge_base_id ON chunks FOR EACH ROW EXECUTE FUNCTION nextcloud_onboarding_content_guard();
CREATE FUNCTION nextcloud_onboarding_profile_guard() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
 IF NEW.generated_profile IS NOT NULL THEN
 IF EXISTS(SELECT 1 FROM nextcloud_onboarding_operations o WHERE o.tenant_id=NEW.tenant_id AND o.knowledge_base_id=NEW.id) THEN RAISE EXCEPTION 'nextcloud_kb_reserved'; END IF;
 UPDATE nextcloud_kb_blank_witnesses SET virgin=FALSE WHERE tenant_id=NEW.tenant_id AND knowledge_base_id=NEW.id; END IF; RETURN NEW; END $$;
CREATE TRIGGER nextcloud_onboarding_profile_guard BEFORE UPDATE OF generated_profile ON knowledge_bases FOR EACH ROW EXECUTE FUNCTION nextcloud_onboarding_profile_guard();
ALTER TABLE sessions ADD COLUMN nextcloud_scoped BOOLEAN NOT NULL DEFAULT FALSE;
CREATE FUNCTION nextcloud_onboarding_operation_guard() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
 IF (NEW.operation_id,NEW.credential_id,NEW.tenant_id,NEW.instance_id,NEW.binding_id,NEW.knowledge_base_id,NEW.actor_directory_id,NEW.actor_object_guid,NEW.actor_user_id,NEW.mode,NEW.name,NEW.processing_template_id,NEW.request_sha256) IS DISTINCT FROM (OLD.operation_id,OLD.credential_id,OLD.tenant_id,OLD.instance_id,OLD.binding_id,OLD.knowledge_base_id,OLD.actor_directory_id,OLD.actor_object_guid,OLD.actor_user_id,OLD.mode,OLD.name,OLD.processing_template_id,OLD.request_sha256) THEN RAISE EXCEPTION 'nextcloud_onboarding_identity_immutable'; END IF;
 IF OLD.state='retired' AND NEW.state<>'retired' OR OLD.state='active' AND NEW.state='reserved' OR NEW.permission_version<OLD.permission_version OR NEW.revision<OLD.revision THEN RAISE EXCEPTION 'nextcloud_onboarding_generation_regression'; END IF;
 IF NEW.state='active' AND NOT EXISTS(SELECT 1 FROM nextcloud_source_pairings p WHERE p.operation_id=NEW.operation_id AND p.tenant_id=NEW.tenant_id AND p.knowledge_base_id=NEW.knowledge_base_id AND p.nextcloud_instance_id=NEW.instance_id AND p.binding_id=NEW.binding_id AND p.state='active') THEN RAISE EXCEPTION 'nextcloud_onboarding_unpaired'; END IF;
 RETURN NEW; END $$;
CREATE TRIGGER nextcloud_onboarding_operation_guard BEFORE UPDATE ON nextcloud_onboarding_operations FOR EACH ROW EXECUTE FUNCTION nextcloud_onboarding_operation_guard();
CREATE FUNCTION nextcloud_onboarding_grant_guard() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
 IF NOT EXISTS(SELECT 1 FROM nextcloud_onboarding_operations o WHERE o.operation_id=NEW.operation_id AND o.tenant_id=NEW.tenant_id AND o.knowledge_base_id=NEW.knowledge_base_id AND o.actor_directory_id=NEW.directory_id AND o.state='active') THEN RAISE EXCEPTION 'nextcloud_grant_scope_mismatch'; END IF; RETURN NEW; END $$;
CREATE TRIGGER nextcloud_onboarding_grant_guard BEFORE INSERT OR UPDATE ON nextcloud_source_grants FOR EACH ROW EXECUTE FUNCTION nextcloud_onboarding_grant_guard();
