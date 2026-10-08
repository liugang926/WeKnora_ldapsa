-- Additional original configuration and committed-message producer facts.
-- Legacy configurations/messages receive counters only, never source origins.
CREATE TABLE knowledge_config_input_origins (
    origin_id TEXT PRIMARY KEY CHECK(length(trim(origin_id)) > 0),
    tenant_id BIGINT NOT NULL CHECK(tenant_id > 0),
    knowledge_base_id TEXT NOT NULL CHECK(length(trim(knowledge_base_id)) > 0),
    config_revision BIGINT NOT NULL CHECK(config_revision > 0),
    origin_kind TEXT NOT NULL CHECK(origin_kind IN ('human','derived','code')),
    input_digest TEXT NOT NULL CHECK(length(input_digest) = 64),
    source_lineage TEXT NOT NULL CHECK(length(source_lineage) <= 262144),
    UNIQUE(knowledge_base_id,config_revision)
);
CREATE TABLE knowledge_input_message_versions (
    message_id TEXT PRIMARY KEY CHECK(length(trim(message_id)) > 0),
    revision BIGINT NOT NULL CHECK(revision > 0)
);
CREATE TABLE knowledge_input_history_parents (
    admission_id TEXT PRIMARY KEY REFERENCES knowledge_input_admissions(admission_id),
    tenant_id BIGINT NOT NULL CHECK(tenant_id > 0),
    session_id TEXT NOT NULL CHECK(length(trim(session_id)) > 0),
    assistant_message_id TEXT NOT NULL UNIQUE CHECK(length(trim(assistant_message_id)) > 0),
    user_message_id TEXT NOT NULL CHECK(length(trim(user_message_id)) > 0),
    request_id TEXT NOT NULL CHECK(length(trim(request_id)) > 0),
    assistant_revision BIGINT NOT NULL CHECK(assistant_revision > 0),
    user_revision BIGINT NOT NULL CHECK(user_revision > 0),
    assistant_digest TEXT NOT NULL CHECK(length(assistant_digest) = 64),
    user_digest TEXT NOT NULL CHECK(length(user_digest) = 64)
);
INSERT INTO knowledge_input_message_versions(message_id,revision) SELECT id,1 FROM messages;
CREATE FUNCTION knowledge_additional_origin_immutable() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'immutable producer origin'; END; $$;
CREATE TRIGGER knowledge_config_input_origins_immutable BEFORE UPDATE OR DELETE ON knowledge_config_input_origins FOR EACH ROW EXECUTE FUNCTION knowledge_additional_origin_immutable();
CREATE TRIGGER knowledge_input_history_parents_immutable BEFORE UPDATE OR DELETE ON knowledge_input_history_parents FOR EACH ROW EXECUTE FUNCTION knowledge_additional_origin_immutable();
CREATE FUNCTION knowledge_input_message_version() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF TG_OP = 'UPDATE' THEN IF NOT (OLD.session_id IS DISTINCT FROM NEW.session_id OR OLD.request_id IS DISTINCT FROM NEW.request_id OR OLD.content IS DISTINCT FROM NEW.content OR OLD.role IS DISTINCT FROM NEW.role OR OLD.source_lineage::jsonb IS DISTINCT FROM NEW.source_lineage::jsonb OR OLD.rendered_content IS DISTINCT FROM NEW.rendered_content OR OLD.is_completed IS DISTINCT FROM NEW.is_completed OR OLD.knowledge_id IS DISTINCT FROM NEW.knowledge_id OR OLD.agent_id IS DISTINCT FROM NEW.agent_id OR OLD.agent_tenant_id IS DISTINCT FROM NEW.agent_tenant_id OR OLD.model_id IS DISTINCT FROM NEW.model_id OR OLD.deleted_at IS DISTINCT FROM NEW.deleted_at) THEN RETURN NEW; END IF; END IF; INSERT INTO knowledge_input_message_versions(message_id,revision) VALUES(CASE WHEN TG_OP = 'DELETE' THEN OLD.id ELSE NEW.id END,1) ON CONFLICT(message_id) DO UPDATE SET revision=knowledge_input_message_versions.revision+1; IF TG_OP = 'DELETE' THEN RETURN OLD; ELSE RETURN NEW; END IF; END; $$;
CREATE TRIGGER knowledge_input_message_version AFTER INSERT OR UPDATE OR DELETE ON messages FOR EACH ROW EXECUTE FUNCTION knowledge_input_message_version();
ALTER TABLE knowledge_input_material_receipts ADD COLUMN graph_body_digest TEXT;
ALTER TABLE knowledge_input_material_receipts ADD COLUMN graph_row_revision BIGINT;
CREATE TABLE knowledge_input_graph_chunk_versions (
    chunk_id TEXT PRIMARY KEY CHECK(length(trim(chunk_id)) > 0),
    revision BIGINT NOT NULL CHECK(revision > 0)
);
INSERT INTO knowledge_input_graph_chunk_versions(chunk_id,revision) SELECT id,1 FROM chunks;
CREATE FUNCTION knowledge_input_graph_chunk_version() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF TG_OP = 'UPDATE' THEN IF NOT (OLD.tenant_id IS DISTINCT FROM NEW.tenant_id OR OLD.knowledge_base_id IS DISTINCT FROM NEW.knowledge_base_id OR OLD.knowledge_id IS DISTINCT FROM NEW.knowledge_id OR OLD.content IS DISTINCT FROM NEW.content OR OLD.content_revision IS DISTINCT FROM NEW.content_revision OR OLD.chunk_type IS DISTINCT FROM NEW.chunk_type OR OLD.parent_chunk_id IS DISTINCT FROM NEW.parent_chunk_id OR OLD.deleted_at IS DISTINCT FROM NEW.deleted_at) THEN RETURN NEW; END IF; END IF; INSERT INTO knowledge_input_graph_chunk_versions(chunk_id,revision) VALUES(CASE WHEN TG_OP = 'DELETE' THEN OLD.id ELSE NEW.id END,1) ON CONFLICT(chunk_id) DO UPDATE SET revision=knowledge_input_graph_chunk_versions.revision+1; IF TG_OP = 'DELETE' THEN RETURN OLD; ELSE RETURN NEW; END IF; END; $$;
CREATE TRIGGER knowledge_input_graph_chunk_version AFTER INSERT OR UPDATE OR DELETE ON chunks FOR EACH ROW EXECUTE FUNCTION knowledge_input_graph_chunk_version();
