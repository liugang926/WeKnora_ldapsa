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
CREATE TRIGGER knowledge_config_input_origins_immutable_update BEFORE UPDATE ON knowledge_config_input_origins BEGIN SELECT RAISE(ABORT, 'immutable producer origin'); END;
CREATE TRIGGER knowledge_config_input_origins_immutable_delete BEFORE DELETE ON knowledge_config_input_origins BEGIN SELECT RAISE(ABORT, 'immutable producer origin'); END;
CREATE TRIGGER knowledge_input_history_parents_immutable_update BEFORE UPDATE ON knowledge_input_history_parents BEGIN SELECT RAISE(ABORT, 'immutable producer origin'); END;
CREATE TRIGGER knowledge_input_history_parents_immutable_delete BEFORE DELETE ON knowledge_input_history_parents BEGIN SELECT RAISE(ABORT, 'immutable producer origin'); END;
CREATE TRIGGER knowledge_input_message_insert AFTER INSERT ON messages BEGIN INSERT INTO knowledge_input_message_versions(message_id,revision) VALUES(NEW.id,1) ON CONFLICT(message_id) DO UPDATE SET revision=revision+1; END;
CREATE TRIGGER knowledge_input_message_update AFTER UPDATE ON messages WHEN OLD.session_id IS NOT NEW.session_id OR OLD.request_id IS NOT NEW.request_id OR OLD.content IS NOT NEW.content OR OLD.role IS NOT NEW.role OR OLD.source_lineage IS NOT NEW.source_lineage OR OLD.rendered_content IS NOT NEW.rendered_content OR OLD.is_completed IS NOT NEW.is_completed OR OLD.knowledge_id IS NOT NEW.knowledge_id OR OLD.agent_id IS NOT NEW.agent_id OR OLD.agent_tenant_id IS NOT NEW.agent_tenant_id OR OLD.model_id IS NOT NEW.model_id OR OLD.execution_context IS NOT NEW.execution_context OR OLD.deleted_at IS NOT NEW.deleted_at BEGIN INSERT INTO knowledge_input_message_versions(message_id,revision) VALUES(NEW.id,1) ON CONFLICT(message_id) DO UPDATE SET revision=revision+1; END;
CREATE TRIGGER knowledge_input_message_delete AFTER DELETE ON messages BEGIN INSERT INTO knowledge_input_message_versions(message_id,revision) VALUES(OLD.id,1) ON CONFLICT(message_id) DO UPDATE SET revision=revision+1; END;
ALTER TABLE knowledge_input_material_receipts ADD COLUMN graph_body_digest TEXT;
ALTER TABLE knowledge_input_material_receipts ADD COLUMN graph_row_revision BIGINT;
CREATE TABLE knowledge_input_graph_chunk_versions (
    chunk_id TEXT PRIMARY KEY CHECK(length(trim(chunk_id)) > 0),
    revision BIGINT NOT NULL CHECK(revision > 0)
);
INSERT INTO knowledge_input_graph_chunk_versions(chunk_id,revision) SELECT id,1 FROM chunks;
CREATE TRIGGER knowledge_input_graph_chunk_insert AFTER INSERT ON chunks BEGIN INSERT INTO knowledge_input_graph_chunk_versions(chunk_id,revision) VALUES(NEW.id,1) ON CONFLICT(chunk_id) DO UPDATE SET revision=revision+1; END;
CREATE TRIGGER knowledge_input_graph_chunk_update AFTER UPDATE ON chunks WHEN OLD.tenant_id IS NOT NEW.tenant_id OR OLD.knowledge_base_id IS NOT NEW.knowledge_base_id OR OLD.knowledge_id IS NOT NEW.knowledge_id OR OLD.content IS NOT NEW.content OR OLD.content_revision IS NOT NEW.content_revision OR OLD.chunk_type IS NOT NEW.chunk_type OR OLD.parent_chunk_id IS NOT NEW.parent_chunk_id OR OLD.deleted_at IS NOT NEW.deleted_at BEGIN INSERT INTO knowledge_input_graph_chunk_versions(chunk_id,revision) VALUES(NEW.id,1) ON CONFLICT(chunk_id) DO UPDATE SET revision=revision+1; END;
CREATE TRIGGER knowledge_input_graph_chunk_delete AFTER DELETE ON chunks BEGIN INSERT INTO knowledge_input_graph_chunk_versions(chunk_id,revision) VALUES(OLD.id,1) ON CONFLICT(chunk_id) DO UPDATE SET revision=revision+1; END;
