CREATE TABLE dingtalk_document_publications (
 tenant_id BIGINT NOT NULL, knowledge_base_id TEXT NOT NULL, data_source_id TEXT NOT NULL, external_id TEXT NOT NULL,
 generation BIGINT NOT NULL CHECK(generation>0), knowledge_id TEXT NOT NULL UNIQUE, item_digest TEXT NOT NULL CHECK(length(item_digest)=64),
 PRIMARY KEY(tenant_id,knowledge_base_id,data_source_id,external_id));
CREATE TABLE dingtalk_work_intents (
 id TEXT PRIMARY KEY, tenant_id BIGINT NOT NULL, knowledge_base_id TEXT NOT NULL, data_source_id TEXT NOT NULL, external_id TEXT NOT NULL,
 knowledge_id TEXT NOT NULL, kind TEXT NOT NULL CHECK(kind IN ('process','cleanup')), generation BIGINT NOT NULL CHECK(generation>0),
 version BIGINT NOT NULL CHECK(version>0), state TEXT NOT NULL CHECK(state IN ('pending','retry','done')),
 UNIQUE(knowledge_id,kind));
CREATE INDEX dingtalk_work_source ON dingtalk_work_intents(tenant_id,knowledge_base_id,data_source_id,state);
