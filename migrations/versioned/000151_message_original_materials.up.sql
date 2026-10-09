-- Only actual new message producers mint admissions. Legacy receives counters.
CREATE TABLE message_generation_origins (id TEXT PRIMARY KEY,message_id TEXT NOT NULL UNIQUE,session_id TEXT NOT NULL,tenant_id BIGINT NOT NULL,owner_id TEXT NOT NULL,owner_revision BIGINT NOT NULL,request_id TEXT NOT NULL,role TEXT NOT NULL,actor TEXT NOT NULL,initial_input TEXT NOT NULL);
CREATE TABLE message_material_admissions (id TEXT PRIMARY KEY,generation_id TEXT NOT NULL REFERENCES message_generation_origins(id),message_id TEXT NOT NULL,session_id TEXT NOT NULL,tenant_id BIGINT NOT NULL,owner_id TEXT NOT NULL,owner_revision BIGINT NOT NULL,revision BIGINT NOT NULL,body_digest TEXT NOT NULL,source_lineage TEXT NOT NULL,execution_context TEXT NOT NULL,agent_id TEXT NOT NULL,agent_tenant_id BIGINT NOT NULL);
CREATE TABLE message_material_receipts (message_id TEXT PRIMARY KEY,admission_id TEXT NOT NULL REFERENCES message_material_admissions(id));
CREATE TABLE message_material_versions (message_id TEXT PRIMARY KEY,revision BIGINT NOT NULL CHECK(revision>0));
CREATE TABLE message_session_owner_versions (session_id TEXT PRIMARY KEY,revision BIGINT NOT NULL CHECK(revision>0));
CREATE TABLE message_session_membership_versions (session_id TEXT PRIMARY KEY,revision BIGINT NOT NULL CHECK(revision>0));
INSERT INTO message_material_versions SELECT id,1 FROM messages;
INSERT INTO message_session_owner_versions SELECT id,1 FROM sessions;
INSERT INTO message_session_membership_versions SELECT id,1 FROM sessions;
CREATE FUNCTION message_material_immutable() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'immutable original message producer'; END $$;
CREATE TRIGGER message_generation_origins_immutable BEFORE UPDATE OR DELETE ON message_generation_origins FOR EACH ROW EXECUTE FUNCTION message_material_immutable();
CREATE TRIGGER message_material_admissions_immutable BEFORE UPDATE OR DELETE ON message_material_admissions FOR EACH ROW EXECUTE FUNCTION message_material_immutable();
CREATE FUNCTION message_material_checkpoint() RETURNS trigger LANGUAGE plpgsql AS $$ DECLARE target TEXT; BEGIN
 target:=CASE WHEN TG_OP='DELETE' THEN OLD.id ELSE NEW.id END;
 IF TG_OP!='UPDATE' OR (OLD.role='user' AND NEW.role='user' AND jsonb_build_array(OLD.id,OLD.session_id,OLD.request_id,OLD.role,OLD.content,OLD.is_completed,OLD.created_at,OLD.deleted_at,OLD.agent_id,OLD.agent_tenant_id,OLD.execution_context) IS DISTINCT FROM jsonb_build_array(NEW.id,NEW.session_id,NEW.request_id,NEW.role,NEW.content,NEW.is_completed,NEW.created_at,NEW.deleted_at,NEW.agent_id,NEW.agent_tenant_id,NEW.execution_context)) OR ((OLD.role!='user' OR NEW.role!='user') AND ((to_jsonb(OLD)-'knowledge_id'-'updated_at') IS DISTINCT FROM (to_jsonb(NEW)-'knowledge_id'-'updated_at'))) THEN INSERT INTO message_material_versions VALUES(target,1) ON CONFLICT(message_id) DO UPDATE SET revision=message_material_versions.revision+1; END IF;
 IF TG_OP!='UPDATE' OR OLD.session_id IS DISTINCT FROM NEW.session_id OR OLD.created_at IS DISTINCT FROM NEW.created_at OR OLD.deleted_at IS DISTINCT FROM NEW.deleted_at THEN
 IF TG_OP!='INSERT' THEN INSERT INTO message_session_membership_versions VALUES(OLD.session_id,1) ON CONFLICT(session_id) DO UPDATE SET revision=message_session_membership_versions.revision+1; END IF;
 IF TG_OP!='DELETE' AND (TG_OP='INSERT' OR OLD.session_id IS DISTINCT FROM NEW.session_id) THEN INSERT INTO message_session_membership_versions VALUES(NEW.session_id,1) ON CONFLICT(session_id) DO UPDATE SET revision=message_session_membership_versions.revision+1; END IF; END IF;
 RETURN COALESCE(NEW,OLD); END $$;
CREATE TRIGGER message_material_checkpoint AFTER INSERT OR UPDATE OR DELETE ON messages FOR EACH ROW EXECUTE FUNCTION message_material_checkpoint();
CREATE FUNCTION message_session_owner_checkpoint() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF TG_OP='INSERT' THEN INSERT INTO message_session_membership_versions VALUES(NEW.id,1) ON CONFLICT(session_id) DO UPDATE SET revision=message_session_membership_versions.revision+1; END IF; IF TG_OP!='UPDATE' OR OLD.tenant_id IS DISTINCT FROM NEW.tenant_id OR OLD.user_id IS DISTINCT FROM NEW.user_id OR OLD.deleted_at IS DISTINCT FROM NEW.deleted_at THEN INSERT INTO message_session_owner_versions VALUES(CASE WHEN TG_OP='DELETE' THEN OLD.id ELSE NEW.id END,1) ON CONFLICT(session_id) DO UPDATE SET revision=message_session_owner_versions.revision+1; END IF; RETURN COALESCE(NEW,OLD); END $$;
CREATE TRIGGER message_session_owner_checkpoint AFTER INSERT OR UPDATE OR DELETE ON sessions FOR EACH ROW EXECUTE FUNCTION message_session_owner_checkpoint();
-- Actual generated fields have independent immutable parents and material.
CREATE TABLE message_generated_field_admissions(id TEXT PRIMARY KEY,message_id TEXT NOT NULL,generation_id TEXT NOT NULL,session_id TEXT NOT NULL,tenant_id BIGINT NOT NULL,field TEXT NOT NULL,body_digest TEXT NOT NULL,source_lineage TEXT NOT NULL,parent_materials TEXT NOT NULL,actor TEXT NOT NULL,previous_admission_id TEXT NOT NULL,previous_revision BIGINT NOT NULL,previous_membership BIGINT NOT NULL,result_revision BIGINT NOT NULL,field_revision BIGINT NOT NULL);
CREATE TABLE message_generated_field_receipts(message_id TEXT NOT NULL,field TEXT NOT NULL,admission_id TEXT NOT NULL REFERENCES message_generated_field_admissions(id),PRIMARY KEY(message_id,field));
CREATE TRIGGER message_generated_field_admissions_immutable BEFORE UPDATE OR DELETE ON message_generated_field_admissions FOR EACH ROW EXECUTE FUNCTION message_material_immutable();

CREATE TABLE message_generated_field_versions(message_id TEXT NOT NULL,field TEXT NOT NULL,revision BIGINT NOT NULL,PRIMARY KEY(message_id,field));
CREATE FUNCTION message_generated_field_checkpoint() RETURNS trigger LANGUAGE plpgsql AS $$ DECLARE f TEXT;BEGIN FOREACH f IN ARRAY ARRAY['images','attachments','rendered_content','context_checkpoint'] LOOP IF TG_OP='INSERT' OR to_jsonb(OLD)->f IS DISTINCT FROM to_jsonb(NEW)->f THEN INSERT INTO message_generated_field_versions VALUES(NEW.id,f,1) ON CONFLICT(message_id,field) DO UPDATE SET revision=message_generated_field_versions.revision+1; END IF;END LOOP;RETURN NEW;END $$;
CREATE TRIGGER message_generated_field_checkpoint AFTER INSERT OR UPDATE ON messages FOR EACH ROW EXECUTE FUNCTION message_generated_field_checkpoint();

CREATE TABLE message_fork_parent_origins(generation_id TEXT PRIMARY KEY REFERENCES message_generation_origins(id),parent_materials TEXT NOT NULL);
CREATE TRIGGER message_fork_parent_origins_immutable BEFORE UPDATE OR DELETE ON message_fork_parent_origins FOR EACH ROW EXECUTE FUNCTION message_material_immutable();

CREATE TABLE message_producer_advances(admission_id TEXT PRIMARY KEY REFERENCES message_material_admissions(id),message_id TEXT NOT NULL,session_id TEXT NOT NULL,generation_id TEXT NOT NULL,previous_admission_id TEXT NOT NULL,previous_revision BIGINT NOT NULL,previous_membership BIGINT NOT NULL,result_revision BIGINT NOT NULL);
CREATE TRIGGER message_producer_advances_immutable BEFORE UPDATE OR DELETE ON message_producer_advances FOR EACH ROW EXECUTE FUNCTION message_material_immutable();
