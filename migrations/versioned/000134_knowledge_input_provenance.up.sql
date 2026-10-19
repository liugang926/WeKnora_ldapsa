-- Private server producer receipts. Legacy rows receive versions, never origins.
CREATE TABLE knowledge_input_row_versions (
    row_kind TEXT NOT NULL CHECK (row_kind IN ('knowledge','chunk','config')),
    row_id TEXT NOT NULL CHECK (length(trim(row_id)) > 0),
    revision BIGINT NOT NULL CHECK (revision > 0),
    PRIMARY KEY (row_kind, row_id)
);
CREATE TABLE knowledge_input_admissions (
    admission_id TEXT PRIMARY KEY CHECK (length(trim(admission_id)) > 0),
    tenant_id BIGINT NOT NULL CHECK (tenant_id > 0),
    knowledge_base_id TEXT NOT NULL CHECK (length(trim(knowledge_base_id)) > 0),
    knowledge_id TEXT NOT NULL CHECK (length(trim(knowledge_id)) > 0),
    build_generation BIGINT NOT NULL CHECK (build_generation > 0),
    origin_kind TEXT NOT NULL CHECK (origin_kind IN ('human','derived','history','nextcloud')),
    input_digest TEXT NOT NULL CHECK (input_digest ~ '^[0-9a-f]{64}$'),
    root_digest TEXT NOT NULL CHECK (root_digest ~ '^[0-9a-f]{64}$'),
    config_digest TEXT NOT NULL CHECK (config_digest ~ '^[0-9a-f]{64}$'),
    config_revision BIGINT NOT NULL CHECK (config_revision > 0),
    source_lineage TEXT NOT NULL CHECK (COALESCE(octet_length(source_lineage) <= 262144 AND jsonb_typeof(source_lineage::jsonb) = 'object' AND source_lineage::jsonb -> 'version' = '1'::jsonb AND source_lineage::jsonb ->> 'state' = 'complete' AND jsonb_typeof(source_lineage::jsonb -> 'sources') = 'array', false)),
    UNIQUE (knowledge_id, build_generation)
);
CREATE TABLE knowledge_input_material_receipts (
    row_kind TEXT NOT NULL CHECK (row_kind IN ('knowledge','chunk')),
    row_id TEXT NOT NULL CHECK (length(trim(row_id)) > 0),
    admission_id TEXT NOT NULL REFERENCES knowledge_input_admissions(admission_id),
    row_revision BIGINT NOT NULL CHECK (row_revision > 0),
    body_digest TEXT NOT NULL CHECK (body_digest ~ '^[0-9a-f]{64}$'),
    PRIMARY KEY (row_kind, row_id)
);
CREATE FUNCTION knowledge_input_admission_immutable() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'immutable knowledge admission'; END; $$;
CREATE TRIGGER knowledge_input_admission_immutable BEFORE UPDATE OR DELETE ON knowledge_input_admissions FOR EACH ROW EXECUTE FUNCTION knowledge_input_admission_immutable();
INSERT INTO knowledge_input_row_versions(row_kind,row_id,revision) SELECT 'knowledge',id,1 FROM knowledges;
CREATE FUNCTION knowledge_input_knowledge_version() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF TG_OP = 'UPDATE' THEN IF NOT (OLD.tenant_id IS DISTINCT FROM NEW.tenant_id OR OLD.knowledge_base_id IS DISTINCT FROM NEW.knowledge_base_id OR OLD.type IS DISTINCT FROM NEW.type OR OLD.title IS DISTINCT FROM NEW.title OR OLD.description IS DISTINCT FROM NEW.description OR OLD.source IS DISTINCT FROM NEW.source OR OLD.channel IS DISTINCT FROM NEW.channel OR OLD.profile::jsonb IS DISTINCT FROM NEW.profile::jsonb OR OLD.embedding_model_id IS DISTINCT FROM NEW.embedding_model_id OR OLD.file_name IS DISTINCT FROM NEW.file_name OR OLD.folder_path IS DISTINCT FROM NEW.folder_path OR OLD.file_type IS DISTINCT FROM NEW.file_type OR OLD.file_size IS DISTINCT FROM NEW.file_size OR OLD.file_hash IS DISTINCT FROM NEW.file_hash OR OLD.file_path IS DISTINCT FROM NEW.file_path OR (CASE WHEN OLD.channel = 'nextcloud' AND NEW.channel = 'nextcloud' THEN (OLD.metadata::jsonb - 'nextcloud_etag') IS DISTINCT FROM (NEW.metadata::jsonb - 'nextcloud_etag') ELSE OLD.metadata::jsonb IS DISTINCT FROM NEW.metadata::jsonb END) OR OLD.custom_metadata::jsonb IS DISTINCT FROM NEW.custom_metadata::jsonb OR OLD.deleted_at IS DISTINCT FROM NEW.deleted_at) THEN RETURN NEW; END IF; END IF; INSERT INTO knowledge_input_row_versions(row_kind,row_id,revision) VALUES ('knowledge',CASE WHEN TG_OP = 'DELETE' THEN OLD.id ELSE NEW.id END,1) ON CONFLICT(row_kind,row_id) DO UPDATE SET revision=knowledge_input_row_versions.revision+1; IF TG_OP = 'DELETE' THEN RETURN OLD; ELSE RETURN NEW; END IF; END; $$;
CREATE TRIGGER knowledge_input_knowledge_version AFTER INSERT OR UPDATE OR DELETE ON knowledges FOR EACH ROW EXECUTE FUNCTION knowledge_input_knowledge_version();
INSERT INTO knowledge_input_row_versions(row_kind,row_id,revision) SELECT 'chunk',id,1 FROM chunks;
CREATE FUNCTION knowledge_input_chunk_version() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF TG_OP = 'UPDATE' THEN IF NOT (OLD.tenant_id IS DISTINCT FROM NEW.tenant_id OR OLD.knowledge_base_id IS DISTINCT FROM NEW.knowledge_base_id OR OLD.knowledge_id IS DISTINCT FROM NEW.knowledge_id OR OLD.content IS DISTINCT FROM NEW.content OR OLD.source_content IS DISTINCT FROM NEW.source_content OR OLD.context_header IS DISTINCT FROM NEW.context_header OR OLD.chunk_type IS DISTINCT FROM NEW.chunk_type OR OLD.metadata::jsonb IS DISTINCT FROM NEW.metadata::jsonb OR OLD.image_info IS DISTINCT FROM NEW.image_info OR OLD.parent_chunk_id IS DISTINCT FROM NEW.parent_chunk_id OR OLD.pre_chunk_id IS DISTINCT FROM NEW.pre_chunk_id OR OLD.next_chunk_id IS DISTINCT FROM NEW.next_chunk_id OR OLD.start_at IS DISTINCT FROM NEW.start_at OR OLD.end_at IS DISTINCT FROM NEW.end_at OR OLD.chunk_index IS DISTINCT FROM NEW.chunk_index OR OLD.content_revision IS DISTINCT FROM NEW.content_revision OR OLD.tag_id IS DISTINCT FROM NEW.tag_id OR OLD.relation_chunks::jsonb IS DISTINCT FROM NEW.relation_chunks::jsonb OR OLD.indirect_relation_chunks::jsonb IS DISTINCT FROM NEW.indirect_relation_chunks::jsonb OR OLD.deleted_at IS DISTINCT FROM NEW.deleted_at) THEN RETURN NEW; END IF; END IF; INSERT INTO knowledge_input_row_versions(row_kind,row_id,revision) VALUES ('chunk',CASE WHEN TG_OP = 'DELETE' THEN OLD.id ELSE NEW.id END,1) ON CONFLICT(row_kind,row_id) DO UPDATE SET revision=knowledge_input_row_versions.revision+1; IF TG_OP = 'DELETE' THEN RETURN OLD; ELSE RETURN NEW; END IF; END; $$;
CREATE TRIGGER knowledge_input_chunk_version AFTER INSERT OR UPDATE OR DELETE ON chunks FOR EACH ROW EXECUTE FUNCTION knowledge_input_chunk_version();
INSERT INTO knowledge_input_row_versions(row_kind,row_id,revision) SELECT 'config',id,1 FROM knowledge_bases;
CREATE FUNCTION knowledge_input_config_version() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF TG_OP = 'UPDATE' THEN IF NOT (OLD.tenant_id IS DISTINCT FROM NEW.tenant_id OR OLD.name IS DISTINCT FROM NEW.name OR OLD.description IS DISTINCT FROM NEW.description OR OLD.type IS DISTINCT FROM NEW.type OR OLD.chunking_config::jsonb IS DISTINCT FROM NEW.chunking_config::jsonb OR OLD.image_processing_config::jsonb IS DISTINCT FROM NEW.image_processing_config::jsonb OR OLD.embedding_model_id IS DISTINCT FROM NEW.embedding_model_id OR OLD.summary_model_id IS DISTINCT FROM NEW.summary_model_id OR OLD.vlm_config::jsonb IS DISTINCT FROM NEW.vlm_config::jsonb OR OLD.asr_config::jsonb IS DISTINCT FROM NEW.asr_config::jsonb OR OLD.extract_config::jsonb IS DISTINCT FROM NEW.extract_config::jsonb OR OLD.faq_config::jsonb IS DISTINCT FROM NEW.faq_config::jsonb OR OLD.question_generation_config::jsonb IS DISTINCT FROM NEW.question_generation_config::jsonb OR OLD.auto_tag_config::jsonb IS DISTINCT FROM NEW.auto_tag_config::jsonb OR OLD.profile_config::jsonb IS DISTINCT FROM NEW.profile_config::jsonb OR OLD.wiki_config::jsonb IS DISTINCT FROM NEW.wiki_config::jsonb OR OLD.indexing_strategy::jsonb IS DISTINCT FROM NEW.indexing_strategy::jsonb OR OLD.deleted_at IS DISTINCT FROM NEW.deleted_at) THEN RETURN NEW; END IF; END IF; INSERT INTO knowledge_input_row_versions(row_kind,row_id,revision) VALUES ('config',CASE WHEN TG_OP = 'DELETE' THEN OLD.id ELSE NEW.id END,1) ON CONFLICT(row_kind,row_id) DO UPDATE SET revision=knowledge_input_row_versions.revision+1; IF TG_OP = 'DELETE' THEN RETURN OLD; ELSE RETURN NEW; END IF; END; $$;
CREATE TRIGGER knowledge_input_config_version AFTER INSERT OR UPDATE OR DELETE ON knowledge_bases FOR EACH ROW EXECUTE FUNCTION knowledge_input_config_version();
