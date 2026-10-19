CREATE TABLE nextcloud_root_migrations (
 operation_id TEXT PRIMARY KEY,pair_operation_id TEXT NOT NULL,tenant_id BIGINT NOT NULL,
 knowledge_base_id TEXT NOT NULL,datasource_id TEXT NOT NULL,proof_json TEXT NOT NULL,proof_sha256 TEXT NOT NULL,
 state TEXT NOT NULL CHECK(state IN ('prepared','finalized')),sync_was_enabled BOOLEAN NOT NULL,
 operation_revision BIGINT NOT NULL,created_at DATETIME NOT NULL,updated_at DATETIME NOT NULL);
CREATE UNIQUE INDEX nextcloud_root_migration_live ON nextcloud_root_migrations(pair_operation_id) WHERE state='prepared';
CREATE TABLE nextcloud_target_migrations (
 operation_id TEXT PRIMARY KEY,old_operation_id TEXT NOT NULL UNIQUE,new_operation_id TEXT NOT NULL UNIQUE,
 old_tenant_id BIGINT NOT NULL,old_knowledge_base_id TEXT NOT NULL,old_data_source_id TEXT NOT NULL,
 new_tenant_id BIGINT NOT NULL,new_knowledge_base_id TEXT NOT NULL,new_binding_id TEXT NOT NULL,
 state TEXT NOT NULL CHECK(state IN ('building','active')),manifest_sha256 TEXT NOT NULL DEFAULT '',
 created_at DATETIME NOT NULL,updated_at DATETIME NOT NULL);
