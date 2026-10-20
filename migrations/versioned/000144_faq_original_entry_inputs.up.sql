-- FAQ admissions are per entry. Legacy rows receive versions only.
CREATE TABLE faq_catalog_input_origins (knowledge_id TEXT PRIMARY KEY,producer_id TEXT NOT NULL,origin_kind TEXT NOT NULL CHECK(origin_kind='trusted_code'),input_digest TEXT NOT NULL CHECK(length(input_digest)=64),revision BIGINT NOT NULL CHECK(revision>0));
CREATE TABLE faq_catalog_control_versions (knowledge_id TEXT PRIMARY KEY,revision BIGINT NOT NULL CHECK(revision>0));
CREATE TABLE faq_input_row_versions (chunk_id TEXT PRIMARY KEY, revision BIGINT NOT NULL CHECK(revision>0));
CREATE TABLE faq_input_scope_versions (tenant_id BIGINT NOT NULL,knowledge_base_id TEXT NOT NULL,revision BIGINT NOT NULL CHECK(revision>0),PRIMARY KEY(tenant_id,knowledge_base_id));
CREATE TABLE faq_input_sessions (id TEXT PRIMARY KEY,tenant_id BIGINT NOT NULL,knowledge_base_id TEXT NOT NULL,operation TEXT NOT NULL,input_digest TEXT NOT NULL,input_body TEXT NOT NULL,origin_kind TEXT NOT NULL,source_lineage TEXT NOT NULL,original_inputs TEXT NOT NULL,original_snapshot TEXT NOT NULL,current_snapshot TEXT NOT NULL);
CREATE TABLE faq_input_actor_selectors (admission_id TEXT PRIMARY KEY REFERENCES faq_input_sessions(id),caller_tenant_id BIGINT NOT NULL,user_id TEXT NOT NULL,principal_type TEXT NOT NULL,principal_id TEXT NOT NULL,api_key_id BIGINT);
CREATE TABLE faq_entry_input_admissions (id TEXT PRIMARY KEY,session_id TEXT NOT NULL REFERENCES faq_input_sessions(id),tenant_id BIGINT NOT NULL,knowledge_base_id TEXT NOT NULL,knowledge_id TEXT NOT NULL,chunk_id TEXT NOT NULL,input_digest TEXT NOT NULL,config_digest TEXT NOT NULL,config_revision BIGINT NOT NULL,source_lineage TEXT NOT NULL);
CREATE TABLE faq_entry_material_receipts (chunk_id TEXT PRIMARY KEY,admission_id TEXT NOT NULL REFERENCES faq_entry_input_admissions(id),revision BIGINT NOT NULL,body_digest TEXT NOT NULL);
INSERT INTO faq_input_row_versions(chunk_id,revision) SELECT id,1 FROM chunks WHERE chunk_type='faq';
INSERT INTO faq_input_scope_versions(tenant_id,knowledge_base_id,revision) SELECT tenant_id,id,1 FROM knowledge_bases;
CREATE FUNCTION faq_input_immutable() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'immutable FAQ input'; END $$;
CREATE TRIGGER faq_entry_input_admissions_immutable BEFORE UPDATE OR DELETE ON faq_entry_input_admissions FOR EACH ROW EXECUTE FUNCTION faq_input_immutable();
CREATE FUNCTION faq_session_input_immutable() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF (to_jsonb(OLD)-'current_snapshot') IS DISTINCT FROM (to_jsonb(NEW)-'current_snapshot') THEN RAISE EXCEPTION 'immutable FAQ session input'; END IF; RETURN NEW; END $$;
CREATE TRIGGER faq_input_sessions_immutable BEFORE UPDATE ON faq_input_sessions FOR EACH ROW EXECUTE FUNCTION faq_session_input_immutable();
CREATE FUNCTION faq_input_chunk_checkpoint() RETURNS trigger LANGUAGE plpgsql AS $$ DECLARE row_old chunks; row_new chunks; BEGIN
 IF TG_OP != 'INSERT' THEN row_old := OLD; END IF;
 IF TG_OP != 'DELETE' THEN row_new := NEW; END IF;
 IF row_old.chunk_type='faq' OR row_new.chunk_type='faq' THEN
 INSERT INTO faq_input_row_versions(chunk_id,revision) VALUES(COALESCE(row_new.id,row_old.id),1) ON CONFLICT(chunk_id) DO UPDATE SET revision=faq_input_row_versions.revision+1;
 END IF;
 IF row_old.chunk_type='faq' THEN INSERT INTO faq_input_scope_versions(tenant_id,knowledge_base_id,revision) VALUES(row_old.tenant_id,row_old.knowledge_base_id,1) ON CONFLICT(tenant_id,knowledge_base_id) DO UPDATE SET revision=faq_input_scope_versions.revision+1; END IF;
 IF row_new.chunk_type='faq' AND (TG_OP='INSERT' OR row_old.chunk_type IS DISTINCT FROM 'faq' OR row_old.tenant_id IS DISTINCT FROM row_new.tenant_id OR row_old.knowledge_base_id IS DISTINCT FROM row_new.knowledge_base_id) THEN INSERT INTO faq_input_scope_versions(tenant_id,knowledge_base_id,revision) VALUES(row_new.tenant_id,row_new.knowledge_base_id,1) ON CONFLICT(tenant_id,knowledge_base_id) DO UPDATE SET revision=faq_input_scope_versions.revision+1; END IF;
 RETURN COALESCE(NEW,OLD); END $$;
CREATE TRIGGER faq_input_chunk_checkpoint AFTER INSERT OR UPDATE OR DELETE ON chunks FOR EACH ROW EXECUTE FUNCTION faq_input_chunk_checkpoint();
CREATE FUNCTION faq_input_kb_checkpoint() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN INSERT INTO faq_input_scope_versions(tenant_id,knowledge_base_id,revision) VALUES(NEW.tenant_id,NEW.id,1) ON CONFLICT(tenant_id,knowledge_base_id) DO UPDATE SET revision=faq_input_scope_versions.revision+1; RETURN NEW; END $$;
CREATE TRIGGER faq_input_kb_checkpoint AFTER INSERT ON knowledge_bases FOR EACH ROW EXECUTE FUNCTION faq_input_kb_checkpoint();

CREATE TRIGGER faq_catalog_input_origins_immutable BEFORE UPDATE OR DELETE ON faq_catalog_input_origins FOR EACH ROW EXECUTE FUNCTION faq_input_immutable();

CREATE TRIGGER faq_input_actor_selectors_immutable BEFORE UPDATE OR DELETE ON faq_input_actor_selectors FOR EACH ROW EXECUTE FUNCTION faq_input_immutable();

INSERT INTO faq_catalog_control_versions(knowledge_id,revision) SELECT id,1 FROM knowledges WHERE type='faq';
CREATE FUNCTION faq_catalog_control_checkpoint() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF (TG_OP='INSERT' AND NEW.type='faq') OR (TG_OP='UPDATE' AND (OLD.type='faq' OR NEW.type='faq') AND (OLD.parse_status IS DISTINCT FROM NEW.parse_status OR OLD.enable_status IS DISTINCT FROM NEW.enable_status)) THEN INSERT INTO faq_catalog_control_versions(knowledge_id,revision) VALUES(NEW.id,1) ON CONFLICT(knowledge_id) DO UPDATE SET revision=faq_catalog_control_versions.revision+1; END IF; RETURN NEW; END $$;
CREATE TRIGGER faq_catalog_control_checkpoint AFTER INSERT OR UPDATE ON knowledges FOR EACH ROW EXECUTE FUNCTION faq_catalog_control_checkpoint();

-- Actual import progress/result producers retain their own immutable bodies.
CREATE TABLE faq_import_output_admissions (id TEXT PRIMARY KEY,output_key TEXT NOT NULL,session_id TEXT NOT NULL REFERENCES faq_input_sessions(id),tenant_id BIGINT NOT NULL,knowledge_base_id TEXT NOT NULL,knowledge_id TEXT NOT NULL,task_id TEXT NOT NULL,kind TEXT NOT NULL CHECK(kind IN ('progress','result')),body_digest TEXT NOT NULL CHECK(length(body_digest)=64),body TEXT NOT NULL,source_lineage TEXT NOT NULL,original_inputs TEXT NOT NULL,material_revision BIGINT NOT NULL);
CREATE TABLE faq_import_output_heads (output_key TEXT PRIMARY KEY,admission_id TEXT NOT NULL REFERENCES faq_import_output_admissions(id),revision BIGINT NOT NULL CHECK(revision>0));
CREATE TABLE faq_import_result_versions (knowledge_id TEXT PRIMARY KEY,revision BIGINT NOT NULL CHECK(revision>0));
INSERT INTO faq_import_result_versions(knowledge_id,revision) SELECT id,1 FROM knowledges WHERE type='faq';
CREATE TRIGGER faq_import_output_admissions_immutable BEFORE UPDATE OR DELETE ON faq_import_output_admissions FOR EACH ROW EXECUTE FUNCTION faq_input_immutable();
CREATE FUNCTION faq_import_result_checkpoint() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF (TG_OP='INSERT' AND NEW.type='faq') OR (TG_OP='DELETE' AND OLD.type='faq') OR (TG_OP='UPDATE' AND (OLD.type='faq' OR NEW.type='faq') AND (OLD.last_faq_import_result::jsonb IS DISTINCT FROM NEW.last_faq_import_result::jsonb OR OLD.tenant_id IS DISTINCT FROM NEW.tenant_id OR OLD.knowledge_base_id IS DISTINCT FROM NEW.knowledge_base_id OR OLD.deleted_at IS DISTINCT FROM NEW.deleted_at)) THEN INSERT INTO faq_import_result_versions(knowledge_id,revision) VALUES(CASE WHEN TG_OP='DELETE' THEN OLD.id ELSE NEW.id END,1) ON CONFLICT(knowledge_id) DO UPDATE SET revision=faq_import_result_versions.revision+1; END IF; RETURN COALESCE(NEW,OLD); END $$;
CREATE TRIGGER faq_import_result_checkpoint AFTER INSERT OR UPDATE OR DELETE ON knowledges FOR EACH ROW EXECUTE FUNCTION faq_import_result_checkpoint();
CREATE FUNCTION faq_import_output_head_monotonic() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.revision!=OLD.revision+1 THEN RAISE EXCEPTION 'FAQ output head revision must advance'; END IF; RETURN NEW; END $$;
CREATE TRIGGER faq_import_output_head_monotonic BEFORE UPDATE ON faq_import_output_heads FOR EACH ROW EXECUTE FUNCTION faq_import_output_head_monotonic();
