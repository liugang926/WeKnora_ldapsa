CREATE TEMP TABLE c1_down_guard(ok INTEGER CHECK(ok=1)); INSERT INTO c1_down_guard SELECT CASE WHEN EXISTS(SELECT 1 FROM dingtalk_original_input_receipts) OR EXISTS(SELECT 1 FROM knowledge_input_admissions WHERE origin_kind='dingtalk_original') THEN 0 ELSE 1 END; DROP TABLE c1_down_guard;
DROP TABLE dingtalk_original_input_receipts;
DROP TRIGGER dingtalk_source_revision_insert;
DROP TRIGGER dingtalk_source_revision_update;
DROP TRIGGER dingtalk_source_revision_delete;
DROP TABLE dingtalk_source_input_revisions;
CREATE TEMP TABLE c1_backup_knowledge_input_admissions AS SELECT * FROM knowledge_input_admissions;
CREATE TEMP TABLE c1_backup_knowledge_input_material_receipts AS SELECT * FROM knowledge_input_material_receipts;
CREATE TEMP TABLE c1_backup_knowledge_input_history_parents AS SELECT * FROM knowledge_input_history_parents;
CREATE TEMP TABLE c1_backup_knowledge_input_actor_selectors AS SELECT * FROM knowledge_input_actor_selectors;
CREATE TEMP TABLE c1_backup_knowledge_producer_inputs AS SELECT * FROM knowledge_producer_inputs;
DROP TABLE knowledge_input_material_receipts;
DROP TABLE knowledge_input_history_parents;
DROP TABLE knowledge_input_actor_selectors;
DROP TABLE knowledge_producer_inputs;
DROP TABLE knowledge_input_admissions;
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
INSERT INTO knowledge_input_admissions SELECT * FROM c1_backup_knowledge_input_admissions;
DROP TABLE c1_backup_knowledge_input_admissions;
CREATE TABLE knowledge_input_material_receipts (
    row_kind TEXT NOT NULL CHECK (row_kind IN ('knowledge','chunk')),
    row_id TEXT NOT NULL CHECK (length(trim(row_id)) > 0),
    admission_id TEXT NOT NULL REFERENCES knowledge_input_admissions(admission_id),
    row_revision BIGINT NOT NULL CHECK (row_revision > 0),
    body_digest TEXT NOT NULL CHECK (length(body_digest) = 64 AND body_digest NOT GLOB '*[^0-9a-f]*'), graph_body_digest TEXT, graph_row_revision BIGINT,
    PRIMARY KEY (row_kind, row_id)
);
INSERT INTO knowledge_input_material_receipts SELECT * FROM c1_backup_knowledge_input_material_receipts;
DROP TABLE c1_backup_knowledge_input_material_receipts;
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
INSERT INTO knowledge_input_history_parents SELECT * FROM c1_backup_knowledge_input_history_parents;
DROP TABLE c1_backup_knowledge_input_history_parents;
CREATE TABLE knowledge_input_actor_selectors (
    admission_id TEXT PRIMARY KEY REFERENCES knowledge_input_admissions(admission_id),
    caller_tenant_id BIGINT NOT NULL CHECK(caller_tenant_id > 0),
    user_id TEXT NOT NULL DEFAULT '',
    principal_type TEXT NOT NULL CHECK(principal_type IN ('web_user','api_tenant','api_external_user')),
    principal_id TEXT NOT NULL CHECK(length(trim(principal_id)) > 0),
    api_key_id BIGINT,
    CHECK((principal_type='web_user' AND length(trim(user_id)) > 0 AND principal_id=user_id AND api_key_id IS NULL) OR (principal_type IN ('api_tenant','api_external_user') AND user_id='' AND api_key_id IS NOT NULL AND api_key_id > 0))
);
INSERT INTO knowledge_input_actor_selectors SELECT * FROM c1_backup_knowledge_input_actor_selectors;
DROP TABLE c1_backup_knowledge_input_actor_selectors;
CREATE TABLE knowledge_producer_inputs (
 id TEXT PRIMARY KEY CHECK(length(trim(id)) > 0),
 admission_id TEXT NOT NULL REFERENCES knowledge_input_admissions(admission_id),
 purpose TEXT NOT NULL CHECK(purpose IN ('summary','question','image')),
 input_digest TEXT NOT NULL CHECK(length(input_digest) = 64),
 original_proof TEXT NOT NULL,
 actor_selector TEXT NOT NULL,
 model_digest TEXT NOT NULL CHECK(length(model_digest) = 64)
);
INSERT INTO knowledge_producer_inputs SELECT * FROM c1_backup_knowledge_producer_inputs;
DROP TABLE c1_backup_knowledge_producer_inputs;
CREATE TRIGGER knowledge_input_admission_immutable_update BEFORE UPDATE ON knowledge_input_admissions BEGIN SELECT RAISE(ABORT, 'immutable knowledge admission'); END;
CREATE TRIGGER knowledge_input_admission_immutable_delete BEFORE DELETE ON knowledge_input_admissions BEGIN SELECT RAISE(ABORT, 'immutable knowledge admission'); END;
CREATE TRIGGER knowledge_input_history_parents_immutable_update BEFORE UPDATE ON knowledge_input_history_parents BEGIN SELECT RAISE(ABORT, 'immutable producer origin'); END;
CREATE TRIGGER knowledge_input_history_parents_immutable_delete BEFORE DELETE ON knowledge_input_history_parents BEGIN SELECT RAISE(ABORT, 'immutable producer origin'); END;
CREATE TRIGGER knowledge_input_actor_immutable_update BEFORE UPDATE ON knowledge_input_actor_selectors BEGIN SELECT RAISE(ABORT,'immutable original worker actor'); END;
CREATE TRIGGER knowledge_input_actor_immutable_delete BEFORE DELETE ON knowledge_input_actor_selectors BEGIN SELECT RAISE(ABORT,'immutable original worker actor'); END;
CREATE TRIGGER knowledge_producer_input_immutable_update BEFORE UPDATE ON knowledge_producer_inputs BEGIN SELECT RAISE(ABORT,'immutable actual producer input'); END;
CREATE TRIGGER knowledge_producer_input_immutable_delete BEFORE DELETE ON knowledge_producer_inputs BEGIN SELECT RAISE(ABORT,'immutable actual producer input'); END;
CREATE TRIGGER nextcloud_derived_origin_witness AFTER INSERT ON knowledge_input_admissions BEGIN INSERT INTO nextcloud_derived_origin_witnesses VALUES(NEW.admission_id,CAST((julianday('now')-2440587.5)*86400000 AS INTEGER)); END;
