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
    input_digest TEXT NOT NULL CHECK (length(input_digest) = 64 AND input_digest NOT GLOB '*[^0-9a-f]*'),
    root_digest TEXT NOT NULL CHECK (length(root_digest) = 64 AND root_digest NOT GLOB '*[^0-9a-f]*'),
    config_digest TEXT NOT NULL CHECK (length(config_digest) = 64 AND config_digest NOT GLOB '*[^0-9a-f]*'),
    config_revision BIGINT NOT NULL CHECK (config_revision > 0),
    source_lineage TEXT NOT NULL CHECK (length(CAST(source_lineage AS BLOB)) <= 262144 AND CASE WHEN json_valid(source_lineage) THEN json_type(source_lineage) IS 'object' AND json_type(source_lineage,'$.version') IS 'integer' AND json_extract(source_lineage,'$.version') IS 1 AND json_extract(source_lineage,'$.state') IS 'complete' AND json_type(source_lineage,'$.sources') IS 'array' ELSE 0 END),
    UNIQUE (knowledge_id, build_generation)
);
CREATE TABLE knowledge_input_material_receipts (
    row_kind TEXT NOT NULL CHECK (row_kind IN ('knowledge','chunk')),
    row_id TEXT NOT NULL CHECK (length(trim(row_id)) > 0),
    admission_id TEXT NOT NULL REFERENCES knowledge_input_admissions(admission_id),
    row_revision BIGINT NOT NULL CHECK (row_revision > 0),
    body_digest TEXT NOT NULL CHECK (length(body_digest) = 64 AND body_digest NOT GLOB '*[^0-9a-f]*'),
    PRIMARY KEY (row_kind, row_id)
);
CREATE TRIGGER knowledge_input_admission_immutable_update BEFORE UPDATE ON knowledge_input_admissions BEGIN SELECT RAISE(ABORT, 'immutable knowledge admission'); END;
CREATE TRIGGER knowledge_input_admission_immutable_delete BEFORE DELETE ON knowledge_input_admissions BEGIN SELECT RAISE(ABORT, 'immutable knowledge admission'); END;
INSERT INTO knowledge_input_row_versions(row_kind,row_id,revision) SELECT 'knowledge',id,1 FROM knowledges;
CREATE TRIGGER knowledge_input_knowledge_insert AFTER INSERT ON knowledges BEGIN INSERT INTO knowledge_input_row_versions(row_kind,row_id,revision) VALUES('knowledge',NEW.id,1) ON CONFLICT(row_kind,row_id) DO UPDATE SET revision=revision+1; END;
CREATE TRIGGER knowledge_input_knowledge_update AFTER UPDATE ON knowledges WHEN OLD.tenant_id IS NOT NEW.tenant_id OR OLD.knowledge_base_id IS NOT NEW.knowledge_base_id OR OLD.type IS NOT NEW.type OR OLD.title IS NOT NEW.title OR OLD.description IS NOT NEW.description OR OLD.source IS NOT NEW.source OR OLD.channel IS NOT NEW.channel OR OLD.profile IS NOT NEW.profile OR OLD.embedding_model_id IS NOT NEW.embedding_model_id OR OLD.file_name IS NOT NEW.file_name OR OLD.folder_path IS NOT NEW.folder_path OR OLD.file_type IS NOT NEW.file_type OR OLD.file_size IS NOT NEW.file_size OR OLD.file_hash IS NOT NEW.file_hash OR OLD.file_path IS NOT NEW.file_path OR (CASE WHEN OLD.channel = 'nextcloud' AND NEW.channel = 'nextcloud' AND json_valid(OLD.metadata) AND json_valid(NEW.metadata) THEN json_remove(OLD.metadata,'$.nextcloud_etag') IS NOT json_remove(NEW.metadata,'$.nextcloud_etag') ELSE OLD.metadata IS NOT NEW.metadata END) OR OLD.custom_metadata IS NOT NEW.custom_metadata OR OLD.deleted_at IS NOT NEW.deleted_at BEGIN INSERT INTO knowledge_input_row_versions(row_kind,row_id,revision) VALUES('knowledge',NEW.id,1) ON CONFLICT(row_kind,row_id) DO UPDATE SET revision=revision+1; END;
CREATE TRIGGER knowledge_input_knowledge_delete AFTER DELETE ON knowledges BEGIN INSERT INTO knowledge_input_row_versions(row_kind,row_id,revision) VALUES('knowledge',OLD.id,1) ON CONFLICT(row_kind,row_id) DO UPDATE SET revision=revision+1; END;
INSERT INTO knowledge_input_row_versions(row_kind,row_id,revision) SELECT 'chunk',id,1 FROM chunks;
CREATE TRIGGER knowledge_input_chunk_insert AFTER INSERT ON chunks BEGIN INSERT INTO knowledge_input_row_versions(row_kind,row_id,revision) VALUES('chunk',NEW.id,1) ON CONFLICT(row_kind,row_id) DO UPDATE SET revision=revision+1; END;
CREATE TRIGGER knowledge_input_chunk_update AFTER UPDATE ON chunks WHEN OLD.tenant_id IS NOT NEW.tenant_id OR OLD.knowledge_base_id IS NOT NEW.knowledge_base_id OR OLD.knowledge_id IS NOT NEW.knowledge_id OR OLD.content IS NOT NEW.content OR OLD.source_content IS NOT NEW.source_content OR OLD.context_header IS NOT NEW.context_header OR OLD.chunk_type IS NOT NEW.chunk_type OR OLD.metadata IS NOT NEW.metadata OR OLD.image_info IS NOT NEW.image_info OR OLD.parent_chunk_id IS NOT NEW.parent_chunk_id OR OLD.pre_chunk_id IS NOT NEW.pre_chunk_id OR OLD.next_chunk_id IS NOT NEW.next_chunk_id OR OLD.start_at IS NOT NEW.start_at OR OLD.end_at IS NOT NEW.end_at OR OLD.chunk_index IS NOT NEW.chunk_index OR OLD.content_revision IS NOT NEW.content_revision OR OLD.tag_id IS NOT NEW.tag_id OR OLD.relation_chunks IS NOT NEW.relation_chunks OR OLD.indirect_relation_chunks IS NOT NEW.indirect_relation_chunks OR OLD.deleted_at IS NOT NEW.deleted_at BEGIN INSERT INTO knowledge_input_row_versions(row_kind,row_id,revision) VALUES('chunk',NEW.id,1) ON CONFLICT(row_kind,row_id) DO UPDATE SET revision=revision+1; END;
CREATE TRIGGER knowledge_input_chunk_delete AFTER DELETE ON chunks BEGIN INSERT INTO knowledge_input_row_versions(row_kind,row_id,revision) VALUES('chunk',OLD.id,1) ON CONFLICT(row_kind,row_id) DO UPDATE SET revision=revision+1; END;
INSERT INTO knowledge_input_row_versions(row_kind,row_id,revision) SELECT 'config',id,1 FROM knowledge_bases;
CREATE TRIGGER knowledge_input_config_insert AFTER INSERT ON knowledge_bases BEGIN INSERT INTO knowledge_input_row_versions(row_kind,row_id,revision) VALUES('config',NEW.id,1) ON CONFLICT(row_kind,row_id) DO UPDATE SET revision=revision+1; END;
CREATE TRIGGER knowledge_input_config_update AFTER UPDATE ON knowledge_bases WHEN OLD.tenant_id IS NOT NEW.tenant_id OR OLD.name IS NOT NEW.name OR OLD.description IS NOT NEW.description OR OLD.type IS NOT NEW.type OR OLD.chunking_config IS NOT NEW.chunking_config OR OLD.image_processing_config IS NOT NEW.image_processing_config OR OLD.embedding_model_id IS NOT NEW.embedding_model_id OR OLD.summary_model_id IS NOT NEW.summary_model_id OR OLD.vlm_config IS NOT NEW.vlm_config OR OLD.asr_config IS NOT NEW.asr_config OR OLD.extract_config IS NOT NEW.extract_config OR OLD.faq_config IS NOT NEW.faq_config OR OLD.question_generation_config IS NOT NEW.question_generation_config OR OLD.auto_tag_config IS NOT NEW.auto_tag_config OR OLD.profile_config IS NOT NEW.profile_config OR OLD.wiki_config IS NOT NEW.wiki_config OR OLD.indexing_strategy IS NOT NEW.indexing_strategy OR OLD.deleted_at IS NOT NEW.deleted_at BEGIN INSERT INTO knowledge_input_row_versions(row_kind,row_id,revision) VALUES('config',NEW.id,1) ON CONFLICT(row_kind,row_id) DO UPDATE SET revision=revision+1; END;
CREATE TRIGGER knowledge_input_config_delete AFTER DELETE ON knowledge_bases BEGIN INSERT INTO knowledge_input_row_versions(row_kind,row_id,revision) VALUES('config',OLD.id,1) ON CONFLICT(row_kind,row_id) DO UPDATE SET revision=revision+1; END;
