CREATE TABLE original_body_payloads (
 id TEXT PRIMARY KEY,tenant_id BIGINT NOT NULL,body_sha256 TEXT NOT NULL,
 payload TEXT,origin_domain_sha256 TEXT NOT NULL,body_bytes BIGINT NOT NULL,storage_kind TEXT NOT NULL,
 state TEXT NOT NULL CHECK(state IN ('live','pending','purged')),
 created_at_ms BIGINT NOT NULL,retired_at_ms BIGINT,body_deadline_ms BIGINT,
 metadata_deadline_ms BIGINT,purge_token TEXT NOT NULL DEFAULT '',purge_epoch BIGINT NOT NULL DEFAULT 0
);
CREATE TABLE original_body_field_heads (
 slot_id TEXT PRIMARY KEY,source_table TEXT NOT NULL,row_key TEXT NOT NULL,column_name TEXT NOT NULL,
 tenant_id BIGINT NOT NULL,knowledge_base_id TEXT NOT NULL,knowledge_id TEXT NOT NULL,
 owner_kind TEXT NOT NULL,owner_id TEXT NOT NULL,source_identity_sha256 TEXT NOT NULL,
 body_id TEXT NOT NULL REFERENCES original_body_payloads(id),revision BIGINT NOT NULL,
 admission_id TEXT NOT NULL,source_lineage TEXT NOT NULL,original_body_ref TEXT NOT NULL,
 UNIQUE(source_table,row_key,column_name)
);
CREATE INDEX original_body_heads_body ON original_body_field_heads(body_id);
CREATE TABLE original_body_rewrite_authorities (
 source_table TEXT NOT NULL,row_key TEXT NOT NULL,column_name TEXT NOT NULL,
 body_id TEXT NOT NULL REFERENCES original_body_payloads(id),old_value TEXT NOT NULL,
 new_value TEXT NOT NULL,PRIMARY KEY(source_table,row_key,column_name)
);
CREATE TABLE original_body_parent_refs (
 body_id TEXT NOT NULL REFERENCES original_body_payloads(id),tenant_id BIGINT NOT NULL,
 knowledge_base_id TEXT NOT NULL,knowledge_id TEXT NOT NULL,admission_id TEXT NOT NULL,
 PRIMARY KEY(body_id,tenant_id,knowledge_base_id,knowledge_id,admission_id)
);
CREATE TABLE original_body_leases (
 lease_id TEXT PRIMARY KEY,body_id TEXT NOT NULL REFERENCES original_body_payloads(id),
 kind TEXT NOT NULL CHECK(kind IN ('read','build')),owner_id TEXT NOT NULL,
 epoch BIGINT NOT NULL,expires_at_ms BIGINT NOT NULL,released_at_ms BIGINT
);
CREATE INDEX original_body_live_leases ON original_body_leases(body_id,expires_at_ms);
CREATE TABLE original_body_purge_receipts (
 body_id TEXT PRIMARY KEY,body_sha256 TEXT NOT NULL,scope_sha256 TEXT NOT NULL,
 before_bytes BIGINT NOT NULL,after_bytes BIGINT NOT NULL,cleared_fields BIGINT NOT NULL,
 journal_sequence BIGINT NOT NULL,journal_head_sha256 TEXT NOT NULL,
 purge_token TEXT NOT NULL,purge_epoch BIGINT NOT NULL,completed_at_ms BIGINT NOT NULL
);
CREATE TABLE original_body_journal_heads(id INTEGER PRIMARY KEY CHECK(id=1),sequence BIGINT NOT NULL,head_sha256 TEXT NOT NULL,journal_id TEXT NOT NULL DEFAULT '');
INSERT INTO original_body_journal_heads(id,sequence,head_sha256) VALUES(1,0,'');
CREATE FUNCTION original_body_receipt_immutable() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'immutable original body purge'; END $$;
CREATE TRIGGER original_body_receipt_immutable BEFORE UPDATE OR DELETE ON original_body_purge_receipts FOR EACH ROW EXECUTE FUNCTION original_body_receipt_immutable();

CREATE TABLE original_body_field_revisions (
 slot_id TEXT NOT NULL,revision BIGINT NOT NULL,source_table TEXT NOT NULL,row_key TEXT NOT NULL,column_name TEXT NOT NULL,
 tenant_id BIGINT NOT NULL,knowledge_base_id TEXT NOT NULL,knowledge_id TEXT NOT NULL,
 owner_kind TEXT NOT NULL,owner_id TEXT NOT NULL,source_identity_sha256 TEXT NOT NULL,
 body_id TEXT NOT NULL REFERENCES original_body_payloads(id),admission_id TEXT NOT NULL,source_lineage TEXT NOT NULL,original_body_ref TEXT NOT NULL,
 PRIMARY KEY(slot_id,revision)
);
CREATE INDEX original_body_revision_body ON original_body_field_revisions(body_id);
CREATE INDEX original_body_same_payload ON original_body_payloads(tenant_id,origin_domain_sha256,body_sha256,state);
