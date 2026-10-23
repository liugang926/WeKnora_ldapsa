-- SQLite cannot drop the inline UNIQUE constraints. Rebuild both immutable
-- control tables while preserving every row and binding-scoped invariant.
CREATE TABLE nextcloud_source_pairings_v86 (
    operation_id TEXT PRIMARY KEY,
    tenant_id INTEGER NOT NULL,
    knowledge_base_id TEXT NOT NULL,
    datasource_id TEXT NOT NULL UNIQUE REFERENCES data_sources (id) ON DELETE RESTRICT,
    nextcloud_instance_id TEXT NOT NULL,
    binding_id TEXT NOT NULL,
    datasource_base_url TEXT NOT NULL,
    datasource_config_sha256 TEXT NOT NULL,
    publication_epoch INTEGER NOT NULL CHECK (publication_epoch >= 0),
    key_id TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('pending', 'active')),
    last_error_code TEXT NOT NULL DEFAULT '',
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (nextcloud_instance_id, binding_id)
);
INSERT INTO nextcloud_source_pairings_v86
    (operation_id, tenant_id, knowledge_base_id, datasource_id, nextcloud_instance_id,
     binding_id, datasource_base_url, datasource_config_sha256, publication_epoch,
     key_id, state, last_error_code, created_at, updated_at)
SELECT operation_id, tenant_id, knowledge_base_id, datasource_id, nextcloud_instance_id,
       binding_id, datasource_base_url, datasource_config_sha256, publication_epoch,
       key_id, state, last_error_code, created_at, updated_at
FROM nextcloud_source_pairings;

-- A knowledge base may now have one independently enforced onboarding
-- operation per Nextcloud binding. Operation IDs remain unique, and a binding
-- may still be claimed only once.
CREATE TABLE nextcloud_onboarding_operations_v86 (
    operation_id TEXT PRIMARY KEY,
    credential_id TEXT NOT NULL REFERENCES nextcloud_onboarding_credentials(id),
    tenant_id BIGINT NOT NULL,
    instance_id TEXT NOT NULL,
    binding_id TEXT NOT NULL,
    knowledge_base_id TEXT NOT NULL,
    actor_directory_id TEXT NOT NULL,
    actor_object_guid TEXT NOT NULL,
    actor_user_id TEXT NOT NULL,
    mode TEXT NOT NULL CHECK(mode IN ('create','existing')),
    name TEXT NOT NULL,
    processing_template_id TEXT NOT NULL,
    request_sha256 TEXT NOT NULL,
    state TEXT NOT NULL CHECK(state IN ('reserved','active','retired')),
    revision BIGINT NOT NULL DEFAULT 1,
    permission_version BIGINT NOT NULL DEFAULT 0,
    permission_sha256 TEXT NOT NULL DEFAULT '',
    permission_synced_at DATETIME,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(instance_id,binding_id)
);
INSERT INTO nextcloud_onboarding_operations_v86
    (operation_id, credential_id, tenant_id, instance_id, binding_id,
     knowledge_base_id, actor_directory_id, actor_object_guid, actor_user_id,
     mode, name, processing_template_id, request_sha256, state, revision,
     permission_version, permission_sha256, permission_synced_at, created_at,
     updated_at)
SELECT operation_id, credential_id, tenant_id, instance_id, binding_id,
       knowledge_base_id, actor_directory_id, actor_object_guid, actor_user_id,
       mode, name, processing_template_id, request_sha256, state, revision,
       permission_version, permission_sha256, permission_synced_at, created_at,
       updated_at
FROM nextcloud_onboarding_operations;

CREATE TABLE nextcloud_onboarding_target_closures_v86 (
    operation_id TEXT PRIMARY KEY REFERENCES nextcloud_onboarding_operations(operation_id),
    closure_id TEXT NOT NULL UNIQUE CHECK(length(closure_id)=64),
    tenant_id BIGINT NOT NULL CHECK(tenant_id>0),
    knowledge_base_id TEXT NOT NULL,
    instance_id TEXT NOT NULL,
    binding_id TEXT NOT NULL,
    operation_revision BIGINT NOT NULL CHECK(operation_revision>0),
    reason TEXT NOT NULL CHECK(reason IN ('tenant_deleted','knowledge_base_deleted')),
    deletion_time_ms BIGINT NOT NULL CHECK(deletion_time_ms>0),
    closed_at_ms BIGINT NOT NULL CHECK(closed_at_ms>=deletion_time_ms),
    recovery_plan_sha256 TEXT NOT NULL DEFAULT '',
    UNIQUE(instance_id,binding_id)
);
INSERT INTO nextcloud_onboarding_target_closures_v86
    (operation_id,closure_id,tenant_id,knowledge_base_id,instance_id,binding_id,
     operation_revision,reason,deletion_time_ms,closed_at_ms,recovery_plan_sha256)
SELECT operation_id,closure_id,tenant_id,knowledge_base_id,instance_id,binding_id,
       operation_revision,reason,deletion_time_ms,closed_at_ms,recovery_plan_sha256
FROM nextcloud_onboarding_target_closures;

CREATE TABLE nextcloud_source_decommissions_v86 (
    operation_id TEXT PRIMARY KEY,
    pair_operation_id TEXT NOT NULL UNIQUE
        REFERENCES nextcloud_source_pairings(operation_id) ON DELETE RESTRICT,
    tenant_id INTEGER NOT NULL,
    knowledge_base_id TEXT NOT NULL,
    datasource_id TEXT NOT NULL UNIQUE,
    nextcloud_instance_id TEXT NOT NULL,
    binding_id TEXT NOT NULL,
    publication_epoch INTEGER NOT NULL CHECK(publication_epoch>=0),
    key_id TEXT NOT NULL,
    state TEXT NOT NULL CHECK(state IN ('prepared','acknowledged')),
    inventory_sha256 TEXT NOT NULL DEFAULT '',
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(nextcloud_instance_id,binding_id)
);
INSERT INTO nextcloud_source_decommissions_v86
    (operation_id,pair_operation_id,tenant_id,knowledge_base_id,datasource_id,
     nextcloud_instance_id,binding_id,publication_epoch,key_id,state,
     inventory_sha256,created_at,updated_at)
SELECT operation_id,pair_operation_id,tenant_id,knowledge_base_id,datasource_id,
       nextcloud_instance_id,binding_id,publication_epoch,key_id,state,
       inventory_sha256,created_at,updated_at
FROM nextcloud_source_decommissions;

CREATE TABLE nextcloud_indexed_withdrawals_v86 (
    operation_id TEXT PRIMARY KEY,
    pair_operation_id TEXT NOT NULL UNIQUE
        REFERENCES nextcloud_source_pairings(operation_id) ON DELETE RESTRICT,
    tenant_id INTEGER NOT NULL,
    knowledge_base_id TEXT NOT NULL,
    datasource_id TEXT NOT NULL UNIQUE,
    nextcloud_instance_id TEXT NOT NULL,
    binding_id TEXT NOT NULL,
    publication_epoch INTEGER NOT NULL CHECK(publication_epoch>=0),
    key_id TEXT NOT NULL,
    state TEXT NOT NULL DEFAULT 'withdrawn_inventory_incomplete'
        CHECK(state='withdrawn_inventory_incomplete'),
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(nextcloud_instance_id,binding_id)
);
INSERT INTO nextcloud_indexed_withdrawals_v86
    (operation_id,pair_operation_id,tenant_id,knowledge_base_id,datasource_id,
     nextcloud_instance_id,binding_id,publication_epoch,key_id,state,created_at,updated_at)
SELECT operation_id,pair_operation_id,tenant_id,knowledge_base_id,datasource_id,
       nextcloud_instance_id,binding_id,publication_epoch,key_id,state,created_at,updated_at
FROM nextcloud_indexed_withdrawals;

DROP TRIGGER IF EXISTS nextcloud_paired_kb_hard_delete_guard;
DROP TRIGGER IF EXISTS nextcloud_paired_kb_delete_guard;
DROP TRIGGER IF EXISTS nextcloud_source_pairing_sync_update;
DROP TRIGGER IF EXISTS nextcloud_source_pairing_sync_insert;
DROP TRIGGER IF EXISTS nextcloud_source_pairing_immutable_update;
DROP TRIGGER IF EXISTS nextcloud_source_pairing_admit_update;
DROP TRIGGER IF EXISTS nextcloud_source_pairing_admit_insert;
DROP TRIGGER IF EXISTS nextcloud_onboarding_operation_activation_guard;
DROP TRIGGER IF EXISTS nextcloud_virgin_event_connection;
DROP TRIGGER IF EXISTS nextcloud_virgin_version;
DROP TRIGGER IF EXISTS nextcloud_virgin_chunk;
DROP TRIGGER IF EXISTS nextcloud_virgin_knowledge;
DROP TRIGGER IF EXISTS nextcloud_virgin_sync_log;
DROP TRIGGER IF EXISTS nextcloud_graph_recovery_controls_fence;
DROP TRIGGER IF EXISTS nextcloud_onboarding_operation_identity_guard;
DROP TRIGGER IF EXISTS nextcloud_onboarding_operation_generation_guard;
DROP TRIGGER IF EXISTS nextcloud_onboarding_guard_knowledges_insert;
DROP TRIGGER IF EXISTS nextcloud_onboarding_guard_knowledges_update;
DROP TRIGGER IF EXISTS nextcloud_onboarding_guard_data_sources_insert;
DROP TRIGGER IF EXISTS nextcloud_onboarding_guard_data_sources_update;
DROP TRIGGER IF EXISTS nextcloud_onboarding_guard_wiki_pages_insert;
DROP TRIGGER IF EXISTS nextcloud_onboarding_guard_wiki_pages_update;
DROP TRIGGER IF EXISTS nextcloud_onboarding_guard_chunks_insert;
DROP TRIGGER IF EXISTS nextcloud_onboarding_guard_chunks_update;
DROP TRIGGER IF EXISTS nextcloud_onboarding_profile_guard;
DROP TRIGGER IF EXISTS nextcloud_onboarding_grant_insert_guard;
DROP TRIGGER IF EXISTS nextcloud_onboarding_grant_update_guard;
DROP TRIGGER IF EXISTS nextcloud_tenant_deletion_insert_guard;
DROP TRIGGER IF EXISTS nextcloud_target_closure_immutable_update;
DROP TRIGGER IF EXISTS nextcloud_target_closure_immutable_delete;
DROP TRIGGER IF EXISTS nextcloud_target_closure_insert_guard;
DROP TRIGGER IF EXISTS nextcloud_target_operation_insert;
DROP TRIGGER IF EXISTS nextcloud_target_operation_update;
DROP TRIGGER IF EXISTS nextcloud_target_operation_delete;
DROP TRIGGER IF EXISTS nextcloud_target_nextcloud_source_grants_insert;
DROP TRIGGER IF EXISTS nextcloud_target_nextcloud_source_grants_update;
DROP TRIGGER IF EXISTS nextcloud_target_nextcloud_source_pairings_insert;
DROP TRIGGER IF EXISTS nextcloud_target_nextcloud_source_pairings_update;
DROP TRIGGER IF EXISTS nextcloud_target_nextcloud_source_pairings_delete;
DROP TRIGGER IF EXISTS nextcloud_target_nextcloud_event_connections_insert;
DROP TRIGGER IF EXISTS nextcloud_target_nextcloud_event_connections_update;
DROP TRIGGER IF EXISTS nextcloud_target_nextcloud_event_connections_delete;
DROP TRIGGER IF EXISTS nextcloud_target_data_sources_delete;
DROP TRIGGER IF EXISTS nextcloud_target_datasource_insert;
DROP TRIGGER IF EXISTS nextcloud_target_datasource_update;
DROP TRIGGER IF EXISTS nextcloud_target_knowledges_insert;
DROP TRIGGER IF EXISTS nextcloud_target_knowledges_update;
DROP TRIGGER IF EXISTS nextcloud_target_chunks_insert;
DROP TRIGGER IF EXISTS nextcloud_target_chunks_update;
DROP TRIGGER IF EXISTS nextcloud_target_wiki_pages_insert;
DROP TRIGGER IF EXISTS nextcloud_target_wiki_pages_update;
DROP TRIGGER IF EXISTS nextcloud_target_fence_insert;
DROP TRIGGER IF EXISTS nextcloud_target_fence_update;
DROP TRIGGER IF EXISTS nextcloud_target_lease_insert;
DROP TRIGGER IF EXISTS nextcloud_target_kb_insert;
DROP TRIGGER IF EXISTS nextcloud_target_kb_undelete;
DROP TRIGGER IF EXISTS nextcloud_decommission_resume_guard;
DROP TRIGGER IF EXISTS nextcloud_decommission_row_guard_update;
DROP TRIGGER IF EXISTS nextcloud_decommission_row_guard_delete;
DROP TRIGGER IF EXISTS nextcloud_source_tombstones_nextcloud_source_decommissions_insert;
DROP TRIGGER IF EXISTS nextcloud_indexed_withdrawal_immutable_update;
DROP TRIGGER IF EXISTS nextcloud_indexed_withdrawal_immutable_delete;
DROP TRIGGER IF EXISTS nextcloud_indexed_excludes_empty_ack;
DROP TRIGGER IF EXISTS nextcloud_indexed_withdrawal_resume_guard;
DROP TRIGGER IF EXISTS nextcloud_source_tombstones_nextcloud_indexed_withdrawals_insert;
DROP TABLE nextcloud_onboarding_target_closures;
ALTER TABLE nextcloud_onboarding_target_closures_v86
    RENAME TO nextcloud_onboarding_target_closures;
DROP TABLE nextcloud_source_decommissions;
ALTER TABLE nextcloud_source_decommissions_v86
    RENAME TO nextcloud_source_decommissions;
DROP TABLE nextcloud_indexed_withdrawals;
ALTER TABLE nextcloud_indexed_withdrawals_v86
    RENAME TO nextcloud_indexed_withdrawals;
DROP TABLE nextcloud_source_pairings;
ALTER TABLE nextcloud_source_pairings_v86 RENAME TO nextcloud_source_pairings;
DROP TABLE nextcloud_onboarding_operations;
ALTER TABLE nextcloud_onboarding_operations_v86 RENAME TO nextcloud_onboarding_operations;
CREATE INDEX nextcloud_pairing_kb_state
    ON nextcloud_source_pairings (tenant_id, knowledge_base_id, state);
CREATE INDEX nextcloud_onboarding_operation_kb_state
    ON nextcloud_onboarding_operations (tenant_id, knowledge_base_id, state);
CREATE INDEX nextcloud_target_closure_kb_operation
    ON nextcloud_onboarding_target_closures (tenant_id, knowledge_base_id, operation_id);
CREATE INDEX nextcloud_source_decommission_kb_state
    ON nextcloud_source_decommissions (tenant_id, knowledge_base_id, state);
CREATE INDEX nextcloud_indexed_withdrawal_kb_state
    ON nextcloud_indexed_withdrawals (tenant_id, knowledge_base_id, state);

CREATE TRIGGER nextcloud_onboarding_operation_identity_guard
BEFORE UPDATE ON nextcloud_onboarding_operations
WHEN NEW.operation_id IS NOT OLD.operation_id
  OR NEW.credential_id IS NOT OLD.credential_id
  OR NEW.tenant_id IS NOT OLD.tenant_id
  OR NEW.instance_id IS NOT OLD.instance_id
  OR NEW.binding_id IS NOT OLD.binding_id
  OR NEW.knowledge_base_id IS NOT OLD.knowledge_base_id
  OR NEW.actor_directory_id IS NOT OLD.actor_directory_id
  OR NEW.actor_object_guid IS NOT OLD.actor_object_guid
  OR NEW.actor_user_id IS NOT OLD.actor_user_id
  OR NEW.mode IS NOT OLD.mode
  OR NEW.name IS NOT OLD.name
  OR NEW.processing_template_id IS NOT OLD.processing_template_id
  OR NEW.request_sha256 IS NOT OLD.request_sha256
BEGIN SELECT RAISE(ABORT,'nextcloud_onboarding_identity_immutable'); END;

CREATE TRIGGER nextcloud_onboarding_operation_generation_guard
BEFORE UPDATE ON nextcloud_onboarding_operations
WHEN (OLD.state='retired' AND NEW.state<>'retired')
  OR (OLD.state='active' AND NEW.state='reserved')
  OR NEW.permission_version<OLD.permission_version
  OR NEW.revision<OLD.revision
BEGIN SELECT RAISE(ABORT,'nextcloud_onboarding_generation_regression'); END;

CREATE TRIGGER nextcloud_target_operation_insert
BEFORE INSERT ON nextcloud_onboarding_operations
WHEN EXISTS(SELECT 1 FROM nextcloud_onboarding_tenant_deletions td
            WHERE td.tenant_id=NEW.tenant_id)
  OR EXISTS(SELECT 1 FROM nextcloud_onboarding_target_closures tc
            WHERE tc.tenant_id=NEW.tenant_id
              AND tc.knowledge_base_id=NEW.knowledge_base_id)
  OR NOT EXISTS(SELECT 1 FROM tenants t
                WHERE t.id=NEW.tenant_id AND t.deleted_at IS NULL)
  OR NOT EXISTS(SELECT 1 FROM knowledge_bases k
                WHERE k.id=NEW.knowledge_base_id
                  AND k.tenant_id=NEW.tenant_id AND k.deleted_at IS NULL)
BEGIN SELECT RAISE(ABORT,'nextcloud_target_closed'); END;

CREATE TRIGGER nextcloud_target_operation_update
BEFORE UPDATE ON nextcloud_onboarding_operations
WHEN (EXISTS(SELECT 1 FROM nextcloud_onboarding_target_closures tc
             WHERE tc.operation_id=OLD.operation_id)
   OR EXISTS(SELECT 1 FROM nextcloud_onboarding_tenant_deletions td
             WHERE td.tenant_id=OLD.tenant_id))
 AND (NEW.state<>'retired'
   OR NEW.permission_synced_at IS NOT NULL
   OR NEW.operation_id IS NOT OLD.operation_id
   OR NEW.credential_id IS NOT OLD.credential_id
   OR NEW.tenant_id IS NOT OLD.tenant_id
   OR NEW.knowledge_base_id IS NOT OLD.knowledge_base_id
   OR NEW.instance_id IS NOT OLD.instance_id
   OR NEW.binding_id IS NOT OLD.binding_id
   OR NEW.actor_directory_id IS NOT OLD.actor_directory_id
   OR NEW.actor_object_guid IS NOT OLD.actor_object_guid
   OR NEW.actor_user_id IS NOT OLD.actor_user_id
   OR NEW.request_sha256 IS NOT OLD.request_sha256)
BEGIN SELECT RAISE(ABORT,'nextcloud_target_closed'); END;

CREATE TRIGGER nextcloud_target_operation_delete
BEFORE DELETE ON nextcloud_onboarding_operations
WHEN EXISTS(SELECT 1 FROM nextcloud_onboarding_tenant_deletions td
            WHERE td.tenant_id=OLD.tenant_id)
  OR EXISTS(SELECT 1 FROM nextcloud_onboarding_target_closures tc
            WHERE tc.tenant_id=OLD.tenant_id
              AND tc.knowledge_base_id=OLD.knowledge_base_id)
BEGIN SELECT RAISE(ABORT,'nextcloud_target_closed'); END;

CREATE TRIGGER nextcloud_target_closure_immutable_update
BEFORE UPDATE ON nextcloud_onboarding_target_closures
WHEN 1 BEGIN SELECT RAISE(ABORT,'nextcloud_target_closed'); END;
CREATE TRIGGER nextcloud_target_closure_immutable_delete
BEFORE DELETE ON nextcloud_onboarding_target_closures
WHEN 1 BEGIN SELECT RAISE(ABORT,'nextcloud_target_closed'); END;

CREATE TRIGGER nextcloud_target_nextcloud_source_grants_insert
BEFORE INSERT ON nextcloud_source_grants
WHEN EXISTS(SELECT 1 FROM nextcloud_onboarding_tenant_deletions td
            WHERE td.tenant_id=NEW.tenant_id)
  OR EXISTS(SELECT 1 FROM nextcloud_onboarding_target_closures tc
            WHERE tc.tenant_id=NEW.tenant_id
              AND tc.knowledge_base_id=NEW.knowledge_base_id)
BEGIN SELECT RAISE(ABORT,'nextcloud_target_closed'); END;
CREATE TRIGGER nextcloud_target_nextcloud_source_grants_update
BEFORE UPDATE ON nextcloud_source_grants
WHEN EXISTS(SELECT 1 FROM nextcloud_onboarding_tenant_deletions td
            WHERE td.tenant_id=OLD.tenant_id)
  OR EXISTS(SELECT 1 FROM nextcloud_onboarding_target_closures tc
            WHERE tc.tenant_id=OLD.tenant_id
              AND tc.knowledge_base_id=OLD.knowledge_base_id)
  OR EXISTS(SELECT 1 FROM nextcloud_onboarding_tenant_deletions td
            WHERE td.tenant_id=NEW.tenant_id)
  OR EXISTS(SELECT 1 FROM nextcloud_onboarding_target_closures tc
            WHERE tc.tenant_id=NEW.tenant_id
              AND tc.knowledge_base_id=NEW.knowledge_base_id)
BEGIN SELECT RAISE(ABORT,'nextcloud_target_closed'); END;
CREATE TRIGGER nextcloud_target_nextcloud_event_connections_insert
BEFORE INSERT ON nextcloud_event_connections
WHEN EXISTS(SELECT 1 FROM nextcloud_onboarding_tenant_deletions td
            WHERE td.tenant_id=NEW.tenant_id)
  OR EXISTS(SELECT 1 FROM nextcloud_onboarding_target_closures tc
            WHERE tc.tenant_id=NEW.tenant_id
              AND tc.knowledge_base_id=NEW.knowledge_base_id)
BEGIN SELECT RAISE(ABORT,'nextcloud_target_closed'); END;
CREATE TRIGGER nextcloud_target_nextcloud_event_connections_update
BEFORE UPDATE ON nextcloud_event_connections
WHEN (EXISTS(SELECT 1 FROM nextcloud_onboarding_tenant_deletions td
             WHERE td.tenant_id=OLD.tenant_id)
   OR EXISTS(SELECT 1 FROM nextcloud_onboarding_target_closures tc
             WHERE tc.tenant_id=OLD.tenant_id
               AND tc.knowledge_base_id=OLD.knowledge_base_id)
   OR EXISTS(SELECT 1 FROM nextcloud_onboarding_tenant_deletions td
             WHERE td.tenant_id=NEW.tenant_id)
   OR EXISTS(SELECT 1 FROM nextcloud_onboarding_target_closures tc
             WHERE tc.tenant_id=NEW.tenant_id
               AND tc.knowledge_base_id=NEW.knowledge_base_id))
 AND NEW.status<>'revoked'
BEGIN SELECT RAISE(ABORT,'nextcloud_target_closed'); END;
CREATE TRIGGER nextcloud_target_nextcloud_event_connections_delete
BEFORE DELETE ON nextcloud_event_connections
WHEN EXISTS(SELECT 1 FROM nextcloud_onboarding_tenant_deletions td
            WHERE td.tenant_id=OLD.tenant_id)
  OR EXISTS(SELECT 1 FROM nextcloud_onboarding_target_closures tc
            WHERE tc.tenant_id=OLD.tenant_id
              AND tc.knowledge_base_id=OLD.knowledge_base_id)
BEGIN SELECT RAISE(ABORT,'nextcloud_target_closed'); END;
CREATE TRIGGER nextcloud_target_data_sources_delete
BEFORE DELETE ON data_sources
WHEN EXISTS(SELECT 1 FROM nextcloud_onboarding_tenant_deletions td
            WHERE td.tenant_id=OLD.tenant_id)
  OR EXISTS(SELECT 1 FROM nextcloud_onboarding_target_closures tc
            WHERE tc.tenant_id=OLD.tenant_id
              AND tc.knowledge_base_id=OLD.knowledge_base_id)
BEGIN SELECT RAISE(ABORT,'nextcloud_target_closed'); END;
CREATE TRIGGER nextcloud_target_datasource_insert
BEFORE INSERT ON data_sources
WHEN EXISTS(SELECT 1 FROM nextcloud_onboarding_tenant_deletions td
            WHERE td.tenant_id=NEW.tenant_id)
  OR EXISTS(SELECT 1 FROM nextcloud_onboarding_target_closures tc
            WHERE tc.tenant_id=NEW.tenant_id
              AND tc.knowledge_base_id=NEW.knowledge_base_id)
BEGIN SELECT RAISE(ABORT,'nextcloud_target_closed'); END;
CREATE TRIGGER nextcloud_target_datasource_update
BEFORE UPDATE ON data_sources
WHEN (EXISTS(SELECT 1 FROM nextcloud_onboarding_tenant_deletions td
             WHERE td.tenant_id=OLD.tenant_id)
   OR EXISTS(SELECT 1 FROM nextcloud_onboarding_target_closures tc
             WHERE tc.tenant_id=OLD.tenant_id
               AND tc.knowledge_base_id=OLD.knowledge_base_id)
   OR EXISTS(SELECT 1 FROM nextcloud_onboarding_tenant_deletions td
             WHERE td.tenant_id=NEW.tenant_id)
   OR EXISTS(SELECT 1 FROM nextcloud_onboarding_target_closures tc
             WHERE tc.tenant_id=NEW.tenant_id
               AND tc.knowledge_base_id=NEW.knowledge_base_id))
 AND (NEW.status<>'paused' OR NEW.sync_enabled IS NOT 0
   OR NEW.id IS NOT OLD.id OR NEW.tenant_id IS NOT OLD.tenant_id
   OR NEW.knowledge_base_id IS NOT OLD.knowledge_base_id
   OR NEW.type IS NOT OLD.type OR NEW.config IS NOT OLD.config
   OR (OLD.deleted_at IS NOT NULL AND NEW.deleted_at IS NULL))
BEGIN SELECT RAISE(ABORT,'nextcloud_target_closed'); END;
CREATE TRIGGER nextcloud_target_knowledges_insert
BEFORE INSERT ON knowledges
WHEN EXISTS(SELECT 1 FROM nextcloud_onboarding_tenant_deletions td
            WHERE td.tenant_id=NEW.tenant_id)
  OR EXISTS(SELECT 1 FROM nextcloud_onboarding_target_closures tc
            WHERE tc.tenant_id=NEW.tenant_id
              AND tc.knowledge_base_id=NEW.knowledge_base_id)
BEGIN SELECT RAISE(ABORT,'nextcloud_target_closed'); END;
CREATE TRIGGER nextcloud_target_knowledges_update
BEFORE UPDATE ON knowledges
WHEN (EXISTS(SELECT 1 FROM nextcloud_onboarding_tenant_deletions td
             WHERE td.tenant_id=OLD.tenant_id)
   OR EXISTS(SELECT 1 FROM nextcloud_onboarding_target_closures tc
             WHERE tc.tenant_id=OLD.tenant_id
               AND tc.knowledge_base_id=OLD.knowledge_base_id)
   OR EXISTS(SELECT 1 FROM nextcloud_onboarding_tenant_deletions td
             WHERE td.tenant_id=NEW.tenant_id)
   OR EXISTS(SELECT 1 FROM nextcloud_onboarding_target_closures tc
             WHERE tc.tenant_id=NEW.tenant_id
               AND tc.knowledge_base_id=NEW.knowledge_base_id))
 AND (NEW.id IS NOT OLD.id OR NEW.tenant_id IS NOT OLD.tenant_id
   OR NEW.knowledge_base_id IS NOT OLD.knowledge_base_id
   OR NEW.type IS NOT OLD.type OR NEW.title IS NOT OLD.title
   OR NEW.description IS NOT OLD.description OR NEW.source IS NOT OLD.source
   OR NEW.channel IS NOT OLD.channel
   OR NEW.pending_subtasks_count IS NOT OLD.pending_subtasks_count
   OR NEW.summary_status IS NOT OLD.summary_status
   OR NEW.profile IS NOT OLD.profile
   OR NEW.embedding_model_id IS NOT OLD.embedding_model_id
   OR NEW.file_name IS NOT OLD.file_name OR NEW.folder_path IS NOT OLD.folder_path
   OR NEW.file_type IS NOT OLD.file_type OR NEW.file_size IS NOT OLD.file_size
   OR NEW.file_hash IS NOT OLD.file_hash OR NEW.file_path IS NOT OLD.file_path
   OR NEW.storage_size IS NOT OLD.storage_size
   OR NEW.custom_metadata IS NOT OLD.custom_metadata
   OR NEW.last_faq_import_result IS NOT OLD.last_faq_import_result
   OR NEW.created_at IS NOT OLD.created_at OR NEW.processed_at IS NOT OLD.processed_at
   OR NEW.error_message IS NOT OLD.error_message OR NEW.enable_status<>'disabled'
   OR (NEW.parse_status IS NOT OLD.parse_status
       AND NEW.parse_status NOT IN ('deleting','cancelled'))
   OR json_remove(COALESCE(NEW.metadata,'{}'),'$.nextcloud_etag')
      IS NOT json_remove(COALESCE(OLD.metadata,'{}'),'$.nextcloud_etag')
   OR COALESCE(json_extract(NEW.metadata,'$.nextcloud_etag'),'')<>''
   OR (OLD.deleted_at IS NOT NULL AND NEW.deleted_at IS NULL))
BEGIN SELECT RAISE(ABORT,'nextcloud_target_closed'); END;
CREATE TRIGGER nextcloud_target_chunks_insert
BEFORE INSERT ON chunks
WHEN EXISTS(SELECT 1 FROM nextcloud_onboarding_tenant_deletions td
            WHERE td.tenant_id=NEW.tenant_id)
  OR EXISTS(SELECT 1 FROM nextcloud_onboarding_target_closures tc
            WHERE tc.tenant_id=NEW.tenant_id
              AND tc.knowledge_base_id=NEW.knowledge_base_id)
BEGIN SELECT RAISE(ABORT,'nextcloud_target_closed'); END;
CREATE TRIGGER nextcloud_target_chunks_update
BEFORE UPDATE ON chunks
WHEN (EXISTS(SELECT 1 FROM nextcloud_onboarding_tenant_deletions td
             WHERE td.tenant_id=OLD.tenant_id)
   OR EXISTS(SELECT 1 FROM nextcloud_onboarding_target_closures tc
             WHERE tc.tenant_id=OLD.tenant_id
               AND tc.knowledge_base_id=OLD.knowledge_base_id)
   OR EXISTS(SELECT 1 FROM nextcloud_onboarding_tenant_deletions td
             WHERE td.tenant_id=NEW.tenant_id)
   OR EXISTS(SELECT 1 FROM nextcloud_onboarding_target_closures tc
             WHERE tc.tenant_id=NEW.tenant_id
               AND tc.knowledge_base_id=NEW.knowledge_base_id))
 AND (NEW.id IS NOT OLD.id OR NEW.seq_id IS NOT OLD.seq_id
   OR NEW.tenant_id IS NOT OLD.tenant_id OR NEW.knowledge_id IS NOT OLD.knowledge_id
   OR NEW.knowledge_base_id IS NOT OLD.knowledge_base_id
   OR NEW.tag_id IS NOT OLD.tag_id OR NEW.content IS NOT OLD.content
   OR NEW.source_content IS NOT OLD.source_content
   OR NEW.content_revision IS NOT OLD.content_revision
   OR NEW.index_status IS NOT OLD.index_status
   OR NEW.last_editor_id IS NOT OLD.last_editor_id
   OR NEW.chunk_index IS NOT OLD.chunk_index OR NEW.flags IS NOT OLD.flags
   OR NEW.status IS NOT OLD.status OR NEW.start_at IS NOT OLD.start_at
   OR NEW.end_at IS NOT OLD.end_at OR NEW.pre_chunk_id IS NOT OLD.pre_chunk_id
   OR NEW.next_chunk_id IS NOT OLD.next_chunk_id
   OR NEW.chunk_type IS NOT OLD.chunk_type
   OR NEW.parent_chunk_id IS NOT OLD.parent_chunk_id
   OR NEW.relation_chunks IS NOT OLD.relation_chunks
   OR NEW.indirect_relation_chunks IS NOT OLD.indirect_relation_chunks
   OR NEW.metadata IS NOT OLD.metadata
   OR NEW.content_hash IS NOT OLD.content_hash
   OR NEW.image_info IS NOT OLD.image_info OR NEW.created_at IS NOT OLD.created_at
   OR NEW.context_header IS NOT OLD.context_header OR NEW.is_enabled<>0
   OR (OLD.deleted_at IS NOT NULL AND NEW.deleted_at IS NULL))
BEGIN SELECT RAISE(ABORT,'nextcloud_target_closed'); END;
CREATE TRIGGER nextcloud_target_wiki_pages_insert
BEFORE INSERT ON wiki_pages
WHEN EXISTS(SELECT 1 FROM nextcloud_onboarding_tenant_deletions td
            WHERE td.tenant_id=NEW.tenant_id)
  OR EXISTS(SELECT 1 FROM nextcloud_onboarding_target_closures tc
            WHERE tc.tenant_id=NEW.tenant_id
              AND tc.knowledge_base_id=NEW.knowledge_base_id)
BEGIN SELECT RAISE(ABORT,'nextcloud_target_closed'); END;
CREATE TRIGGER nextcloud_target_wiki_pages_update
BEFORE UPDATE ON wiki_pages
WHEN EXISTS(SELECT 1 FROM nextcloud_onboarding_tenant_deletions td
            WHERE td.tenant_id=OLD.tenant_id)
  OR EXISTS(SELECT 1 FROM nextcloud_onboarding_target_closures tc
            WHERE tc.tenant_id=OLD.tenant_id
              AND tc.knowledge_base_id=OLD.knowledge_base_id)
  OR EXISTS(SELECT 1 FROM nextcloud_onboarding_tenant_deletions td
            WHERE td.tenant_id=NEW.tenant_id)
  OR EXISTS(SELECT 1 FROM nextcloud_onboarding_target_closures tc
            WHERE tc.tenant_id=NEW.tenant_id
              AND tc.knowledge_base_id=NEW.knowledge_base_id)
BEGIN SELECT RAISE(ABORT,'nextcloud_target_closed'); END;
CREATE TRIGGER nextcloud_target_fence_insert
BEFORE INSERT ON nextcloud_content_fences
WHEN (EXISTS(SELECT 1 FROM nextcloud_onboarding_tenant_deletions td
             WHERE td.tenant_id=NEW.tenant_id)
   OR EXISTS(SELECT 1 FROM nextcloud_onboarding_target_closures tc
             WHERE tc.tenant_id=NEW.tenant_id
               AND tc.knowledge_base_id=NEW.knowledge_base_id))
 AND NEW.state='open'
 AND NOT EXISTS(SELECT 1 FROM nextcloud_content_fences f
                WHERE f.tenant_id=NEW.tenant_id
                  AND f.knowledge_base_id=NEW.knowledge_base_id
                  AND f.knowledge_id=NEW.knowledge_id)
BEGIN SELECT RAISE(ABORT,'nextcloud_target_closed'); END;
CREATE TRIGGER nextcloud_target_fence_update
BEFORE UPDATE ON nextcloud_content_fences
WHEN (EXISTS(SELECT 1 FROM nextcloud_onboarding_tenant_deletions td
             WHERE td.tenant_id=OLD.tenant_id)
   OR EXISTS(SELECT 1 FROM nextcloud_onboarding_target_closures tc
             WHERE tc.tenant_id=OLD.tenant_id
               AND tc.knowledge_base_id=OLD.knowledge_base_id)
   OR EXISTS(SELECT 1 FROM nextcloud_onboarding_tenant_deletions td
             WHERE td.tenant_id=NEW.tenant_id)
   OR EXISTS(SELECT 1 FROM nextcloud_onboarding_target_closures tc
             WHERE tc.tenant_id=NEW.tenant_id
               AND tc.knowledge_base_id=NEW.knowledge_base_id))
 AND NEW.state='open'
BEGIN SELECT RAISE(ABORT,'nextcloud_target_closed'); END;
CREATE TRIGGER nextcloud_target_lease_insert
BEFORE INSERT ON nextcloud_content_leases
WHEN EXISTS(SELECT 1 FROM nextcloud_onboarding_tenant_deletions td
            WHERE td.tenant_id=NEW.tenant_id)
  OR EXISTS(SELECT 1 FROM nextcloud_onboarding_target_closures tc
            WHERE tc.tenant_id=NEW.tenant_id
              AND tc.knowledge_base_id=NEW.knowledge_base_id)
BEGIN SELECT RAISE(ABORT,'nextcloud_target_closed'); END;
CREATE TRIGGER nextcloud_target_kb_insert
BEFORE INSERT ON knowledge_bases
WHEN EXISTS(SELECT 1 FROM nextcloud_onboarding_tenant_deletions td
            WHERE td.tenant_id=NEW.tenant_id)
  OR EXISTS(SELECT 1 FROM nextcloud_onboarding_target_closures tc
            WHERE tc.tenant_id=NEW.tenant_id AND tc.knowledge_base_id=NEW.id)
BEGIN SELECT RAISE(ABORT,'nextcloud_target_closed'); END;
CREATE TRIGGER nextcloud_target_kb_undelete
BEFORE UPDATE OF deleted_at ON knowledge_bases
WHEN (EXISTS(SELECT 1 FROM nextcloud_onboarding_tenant_deletions td
             WHERE td.tenant_id=OLD.tenant_id)
   OR EXISTS(SELECT 1 FROM nextcloud_onboarding_target_closures tc
             WHERE tc.tenant_id=OLD.tenant_id AND tc.knowledge_base_id=OLD.id))
 AND OLD.deleted_at IS NOT NULL AND NEW.deleted_at IS NULL
BEGIN SELECT RAISE(ABORT,'nextcloud_target_closed'); END;

CREATE TRIGGER nextcloud_decommission_resume_guard
BEFORE UPDATE OF status ON data_sources
WHEN NEW.status<>'paused'
 AND EXISTS(SELECT 1 FROM nextcloud_source_decommissions
            WHERE datasource_id=NEW.id)
BEGIN SELECT RAISE(ABORT,'nextcloud_source_decommissioned'); END;
CREATE TRIGGER nextcloud_decommission_row_guard_update
BEFORE UPDATE ON nextcloud_source_decommissions
WHEN NEW.operation_id IS NOT OLD.operation_id
  OR NEW.pair_operation_id IS NOT OLD.pair_operation_id
  OR NEW.tenant_id IS NOT OLD.tenant_id
  OR NEW.knowledge_base_id IS NOT OLD.knowledge_base_id
  OR NEW.datasource_id IS NOT OLD.datasource_id
  OR NEW.nextcloud_instance_id IS NOT OLD.nextcloud_instance_id
  OR NEW.binding_id IS NOT OLD.binding_id
  OR NEW.publication_epoch IS NOT OLD.publication_epoch
  OR NEW.key_id IS NOT OLD.key_id
  OR (OLD.state<>'prepared' AND NEW.state IS NOT OLD.state)
  OR (NEW.state='prepared' AND NEW.inventory_sha256<>'')
  OR (NEW.state='acknowledged' AND NEW.inventory_sha256<>
      'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855')
  OR (OLD.state='acknowledged' AND NEW.inventory_sha256 IS NOT OLD.inventory_sha256)
BEGIN SELECT RAISE(ABORT,'nextcloud_source_decommission_immutable'); END;
CREATE TRIGGER nextcloud_decommission_row_guard_delete
BEFORE DELETE ON nextcloud_source_decommissions
BEGIN SELECT RAISE(ABORT,'nextcloud_source_decommission_immutable'); END;
CREATE TRIGGER nextcloud_source_tombstones_nextcloud_source_decommissions_insert
AFTER INSERT ON nextcloud_source_decommissions
BEGIN
    INSERT INTO nextcloud_source_tombstones(tenant_id,scope_type,scope_id)
    SELECT NEW.tenant_id,'tenant',CAST(NEW.tenant_id AS TEXT)
    WHERE NEW.tenant_id>0 ON CONFLICT DO NOTHING;
    INSERT INTO nextcloud_source_tombstones(tenant_id,scope_type,scope_id)
    SELECT NEW.tenant_id,'knowledge_base',NEW.knowledge_base_id
    WHERE NEW.tenant_id>0 AND length(trim(NEW.knowledge_base_id))>0 ON CONFLICT DO NOTHING;
    INSERT INTO nextcloud_source_tombstones(tenant_id,scope_type,scope_id)
    SELECT NEW.tenant_id,'datasource',NEW.datasource_id
    WHERE NEW.tenant_id>0 AND length(trim(NEW.datasource_id))>0 ON CONFLICT DO NOTHING;
    INSERT INTO nextcloud_source_tombstones(tenant_id,scope_type,scope_id)
    SELECT NEW.tenant_id,'pair',NEW.pair_operation_id
    WHERE NEW.tenant_id>0 AND length(trim(NEW.pair_operation_id))>0 ON CONFLICT DO NOTHING;
END;

CREATE TRIGGER nextcloud_indexed_withdrawal_immutable_update
BEFORE UPDATE ON nextcloud_indexed_withdrawals
BEGIN SELECT RAISE(ABORT,'nextcloud_indexed_withdrawal_immutable'); END;
CREATE TRIGGER nextcloud_indexed_withdrawal_immutable_delete
BEFORE DELETE ON nextcloud_indexed_withdrawals
BEGIN SELECT RAISE(ABORT,'nextcloud_indexed_withdrawal_immutable'); END;
CREATE TRIGGER nextcloud_indexed_excludes_empty_ack
BEFORE INSERT ON nextcloud_source_decommissions
WHEN EXISTS(SELECT 1 FROM nextcloud_indexed_withdrawals
            WHERE pair_operation_id=NEW.pair_operation_id)
BEGIN SELECT RAISE(ABORT,'nextcloud_indexed_source_cannot_ack_empty'); END;
CREATE TRIGGER nextcloud_indexed_withdrawal_resume_guard
BEFORE UPDATE OF status ON data_sources
WHEN NEW.status<>'paused'
 AND EXISTS(SELECT 1 FROM nextcloud_indexed_withdrawals
            WHERE datasource_id=NEW.id)
BEGIN SELECT RAISE(ABORT,'nextcloud_indexed_source_withdrawn'); END;
CREATE TRIGGER nextcloud_source_tombstones_nextcloud_indexed_withdrawals_insert
AFTER INSERT ON nextcloud_indexed_withdrawals
BEGIN
    INSERT INTO nextcloud_source_tombstones(tenant_id,scope_type,scope_id)
    SELECT NEW.tenant_id,'tenant',CAST(NEW.tenant_id AS TEXT)
    WHERE NEW.tenant_id>0 ON CONFLICT DO NOTHING;
    INSERT INTO nextcloud_source_tombstones(tenant_id,scope_type,scope_id)
    SELECT NEW.tenant_id,'knowledge_base',NEW.knowledge_base_id
    WHERE NEW.tenant_id>0 AND length(trim(NEW.knowledge_base_id))>0 ON CONFLICT DO NOTHING;
    INSERT INTO nextcloud_source_tombstones(tenant_id,scope_type,scope_id)
    SELECT NEW.tenant_id,'datasource',NEW.datasource_id
    WHERE NEW.tenant_id>0 AND length(trim(NEW.datasource_id))>0 ON CONFLICT DO NOTHING;
    INSERT INTO nextcloud_source_tombstones(tenant_id,scope_type,scope_id)
    SELECT NEW.tenant_id,'pair',NEW.pair_operation_id
    WHERE NEW.tenant_id>0 AND length(trim(NEW.pair_operation_id))>0 ON CONFLICT DO NOTHING;
END;

-- The pre-v3 guard required a data source's operation ID to differ from the
-- single operation for the KB. Match any operation owned by that KB instead.
CREATE TRIGGER nextcloud_onboarding_guard_data_sources_insert
BEFORE INSERT ON data_sources
WHEN EXISTS(SELECT 1 FROM nextcloud_onboarding_operations o
            WHERE o.tenant_id=NEW.tenant_id
              AND o.knowledge_base_id=NEW.knowledge_base_id)
 AND (NEW.type <> 'nextcloud'
   OR NOT EXISTS(SELECT 1 FROM nextcloud_onboarding_operations owned
                 WHERE owned.tenant_id=NEW.tenant_id
                   AND owned.knowledge_base_id=NEW.knowledge_base_id
                   AND owned.operation_id=json_extract(
                       NEW.config,'$.settings.onboarding_operation_id')))
BEGIN SELECT RAISE(ABORT,'nextcloud_kb_reserved'); END;

CREATE TRIGGER nextcloud_onboarding_guard_data_sources_update
BEFORE UPDATE OF tenant_id,knowledge_base_id ON data_sources
WHEN EXISTS(SELECT 1 FROM nextcloud_onboarding_operations o
            WHERE o.tenant_id=NEW.tenant_id
              AND o.knowledge_base_id=NEW.knowledge_base_id)
 AND (NEW.type <> 'nextcloud'
   OR NOT EXISTS(SELECT 1 FROM nextcloud_onboarding_operations owned
                 WHERE owned.tenant_id=NEW.tenant_id
                   AND owned.knowledge_base_id=NEW.knowledge_base_id
                   AND owned.operation_id=json_extract(
                       NEW.config,'$.settings.onboarding_operation_id')))
BEGIN SELECT RAISE(ABORT,'nextcloud_kb_reserved'); END;

CREATE TRIGGER nextcloud_onboarding_guard_knowledges_insert
BEFORE INSERT ON knowledges
WHEN EXISTS(
    SELECT 1 FROM nextcloud_onboarding_operations o
    WHERE o.tenant_id=NEW.tenant_id
      AND o.knowledge_base_id=NEW.knowledge_base_id
      AND (o.state <> 'active' OR NEW.channel <> 'nextcloud'))
BEGIN SELECT RAISE(ABORT,'nextcloud_kb_reserved'); END;
CREATE TRIGGER nextcloud_onboarding_guard_knowledges_update
BEFORE UPDATE OF tenant_id,knowledge_base_id ON knowledges
WHEN EXISTS(
    SELECT 1 FROM nextcloud_onboarding_operations o
    WHERE o.tenant_id=NEW.tenant_id
      AND o.knowledge_base_id=NEW.knowledge_base_id
      AND (o.state <> 'active' OR NEW.channel <> 'nextcloud'))
BEGIN SELECT RAISE(ABORT,'nextcloud_kb_reserved'); END;
CREATE TRIGGER nextcloud_onboarding_guard_wiki_pages_insert
BEFORE INSERT ON wiki_pages
WHEN EXISTS(
    SELECT 1 FROM nextcloud_onboarding_operations o
    WHERE o.tenant_id=NEW.tenant_id
      AND o.knowledge_base_id=NEW.knowledge_base_id)
BEGIN SELECT RAISE(ABORT,'nextcloud_kb_reserved'); END;
CREATE TRIGGER nextcloud_onboarding_guard_wiki_pages_update
BEFORE UPDATE OF tenant_id,knowledge_base_id ON wiki_pages
WHEN EXISTS(
    SELECT 1 FROM nextcloud_onboarding_operations o
    WHERE o.tenant_id=NEW.tenant_id
      AND o.knowledge_base_id=NEW.knowledge_base_id)
BEGIN SELECT RAISE(ABORT,'nextcloud_kb_reserved'); END;
CREATE TRIGGER nextcloud_onboarding_guard_chunks_insert
BEFORE INSERT ON chunks
WHEN EXISTS(
    SELECT 1 FROM nextcloud_onboarding_operations o
    WHERE o.tenant_id=NEW.tenant_id
      AND o.knowledge_base_id=NEW.knowledge_base_id
      AND o.state <> 'active')
BEGIN SELECT RAISE(ABORT,'nextcloud_kb_reserved'); END;
CREATE TRIGGER nextcloud_onboarding_guard_chunks_update
BEFORE UPDATE OF tenant_id,knowledge_base_id ON chunks
WHEN EXISTS(
    SELECT 1 FROM nextcloud_onboarding_operations o
    WHERE o.tenant_id=NEW.tenant_id
      AND o.knowledge_base_id=NEW.knowledge_base_id
      AND o.state <> 'active')
BEGIN SELECT RAISE(ABORT,'nextcloud_kb_reserved'); END;
CREATE TRIGGER nextcloud_onboarding_profile_guard
BEFORE UPDATE OF generated_profile ON knowledge_bases
WHEN NEW.generated_profile IS NOT NULL
 AND EXISTS(
    SELECT 1 FROM nextcloud_onboarding_operations o
    WHERE o.tenant_id=NEW.tenant_id AND o.knowledge_base_id=NEW.id)
BEGIN SELECT RAISE(ABORT,'nextcloud_kb_reserved'); END;
CREATE TRIGGER nextcloud_onboarding_grant_insert_guard
BEFORE INSERT ON nextcloud_source_grants
WHEN NOT EXISTS(
    SELECT 1 FROM nextcloud_onboarding_operations o
    WHERE o.operation_id=NEW.operation_id
      AND o.tenant_id=NEW.tenant_id
      AND o.knowledge_base_id=NEW.knowledge_base_id
      AND o.actor_directory_id=NEW.directory_id
      AND o.state='active')
BEGIN SELECT RAISE(ABORT,'nextcloud_grant_scope_mismatch'); END;
CREATE TRIGGER nextcloud_onboarding_grant_update_guard
BEFORE UPDATE ON nextcloud_source_grants
WHEN NOT EXISTS(
    SELECT 1 FROM nextcloud_onboarding_operations o
    WHERE o.operation_id=NEW.operation_id
      AND o.tenant_id=NEW.tenant_id
      AND o.knowledge_base_id=NEW.knowledge_base_id
      AND o.actor_directory_id=NEW.directory_id
      AND o.state='active')
BEGIN SELECT RAISE(ABORT,'nextcloud_grant_scope_mismatch'); END;

CREATE TRIGGER nextcloud_tenant_deletion_insert_guard
BEFORE INSERT ON nextcloud_onboarding_tenant_deletions
WHEN NOT ((NEW.recovery_plan_sha256='' AND EXISTS(
        SELECT 1 FROM tenants t
        WHERE t.id=NEW.tenant_id AND t.deleted_at IS NOT NULL))
    OR (NEW.recovery_plan_sha256<>'' AND EXISTS(
        SELECT 1 FROM nextcloud_publication_recovery_receipts r,
                      json_each(r.plan_json,'$.tenant_deletions') e
        JOIN nextcloud_onboarding_operations o
          ON o.operation_id=json_extract(e.value,'$.tenant.operation_id')
         AND o.instance_id=r.instance_id
         AND o.instance_id=json_extract(e.value,'$.tenant.instance_id')
         AND o.binding_id=json_extract(e.value,'$.tenant.binding_id')
         AND o.tenant_id=NEW.tenant_id
        WHERE r.plan_sha256=NEW.recovery_plan_sha256
          AND json_extract(e.value,'$.tenant.tenant_id')=CAST(NEW.tenant_id AS TEXT)
          AND json_extract(e.value,'$.tenant.deletion_id')=NEW.deletion_id
          AND json_extract(e.value,'$.tenant.deletion_time_ms')=CAST(NEW.deletion_time_ms AS TEXT)
          AND json_extract(e.value,'$.tenant.closed_at')=CAST(NEW.closed_at_ms/1000 AS TEXT))))
BEGIN SELECT RAISE(ABORT,'nextcloud_target_closed'); END;

CREATE TRIGGER nextcloud_target_closure_insert_guard
BEFORE INSERT ON nextcloud_onboarding_target_closures
WHEN NOT EXISTS(
        SELECT 1 FROM nextcloud_onboarding_operations o
        WHERE o.operation_id=NEW.operation_id
          AND o.tenant_id=NEW.tenant_id
          AND o.knowledge_base_id=NEW.knowledge_base_id
          AND o.instance_id=NEW.instance_id
          AND o.binding_id=NEW.binding_id
          AND o.revision>=NEW.operation_revision)
    OR NOT ((NEW.recovery_plan_sha256='' AND (
            (NEW.reason='tenant_deleted' AND EXISTS(
                SELECT 1 FROM tenants t
                WHERE t.id=NEW.tenant_id AND t.deleted_at IS NOT NULL))
            OR (NEW.reason='knowledge_base_deleted' AND EXISTS(
                SELECT 1 FROM knowledge_bases k
                WHERE k.id=NEW.knowledge_base_id
                  AND k.tenant_id=NEW.tenant_id
                  AND k.deleted_at IS NOT NULL))))
        OR (NEW.recovery_plan_sha256<>'' AND EXISTS(
            SELECT 1
            FROM nextcloud_publication_recovery_receipts r,
                 json_each(r.plan_json,'$.target_closures') e
            WHERE r.plan_sha256=NEW.recovery_plan_sha256
              AND r.instance_id=NEW.instance_id
              AND json_extract(e.value,'$.target.closure_id')=NEW.closure_id
              AND json_extract(e.value,'$.target.operation_id')=NEW.operation_id
              AND json_extract(e.value,'$.target.tenant_id')=CAST(NEW.tenant_id AS TEXT)
              AND json_extract(e.value,'$.target.knowledge_base_id')=NEW.knowledge_base_id
              AND json_extract(e.value,'$.target.instance_id')=NEW.instance_id
              AND json_extract(e.value,'$.target.binding_id')=NEW.binding_id
              AND json_extract(e.value,'$.target.operation_revision')=CAST(NEW.operation_revision AS TEXT)
              AND json_extract(e.value,'$.target.reason')=NEW.reason
              AND json_extract(e.value,'$.target.deletion_time_ms')=CAST(NEW.deletion_time_ms AS TEXT))))
BEGIN SELECT RAISE(ABORT,'nextcloud_target_closed'); END;

CREATE TRIGGER nextcloud_source_pairing_admit_insert
BEFORE INSERT ON data_sources
WHEN NEW.type = 'nextcloud' AND NEW.status <> 'paused' AND NOT EXISTS (
    SELECT 1 FROM nextcloud_source_pairings p
    WHERE p.datasource_id = NEW.id AND p.tenant_id = NEW.tenant_id
      AND p.knowledge_base_id = NEW.knowledge_base_id AND p.state = 'active')
BEGIN SELECT RAISE(ABORT, 'nextcloud_source_unpaired'); END;

CREATE TRIGGER nextcloud_source_pairing_admit_update
BEFORE UPDATE OF status, type, tenant_id, knowledge_base_id, config, deleted_at ON data_sources
WHEN NEW.type = 'nextcloud' AND NEW.status <> 'paused' AND NOT EXISTS (
    SELECT 1 FROM nextcloud_source_pairings p
    WHERE p.datasource_id = NEW.id AND p.tenant_id = NEW.tenant_id
      AND p.knowledge_base_id = NEW.knowledge_base_id AND p.state = 'active')
BEGIN SELECT RAISE(ABORT, 'nextcloud_source_unpaired'); END;

CREATE TRIGGER nextcloud_source_pairing_immutable_update
BEFORE UPDATE OF config, type, tenant_id, knowledge_base_id, deleted_at ON data_sources
WHEN EXISTS (SELECT 1 FROM nextcloud_source_pairings p WHERE p.datasource_id = OLD.id)
 AND (NEW.type IS NOT OLD.type OR NEW.tenant_id IS NOT OLD.tenant_id
  OR NEW.knowledge_base_id IS NOT OLD.knowledge_base_id OR NEW.deleted_at IS NOT OLD.deleted_at
  OR (NEW.config IS NOT OLD.config AND NOT EXISTS (
      SELECT 1 FROM nextcloud_source_rotations r
      WHERE r.datasource_id = OLD.id AND r.state = 'committed'
        AND r.old_config = OLD.config AND r.new_config = NEW.config
  )))
BEGIN SELECT RAISE(ABORT, 'nextcloud_paired_source_immutable'); END;

CREATE TRIGGER nextcloud_source_pairing_sync_insert
BEFORE INSERT ON sync_logs
WHEN NEW.status = 'running'
 AND EXISTS (SELECT 1 FROM data_sources WHERE id = NEW.data_source_id AND type = 'nextcloud')
 AND NOT EXISTS (SELECT 1 FROM nextcloud_source_pairings p
     WHERE p.datasource_id = NEW.data_source_id AND p.tenant_id = NEW.tenant_id
       AND p.state = 'active')
BEGIN SELECT RAISE(ABORT, 'nextcloud_source_unpaired'); END;

CREATE TRIGGER nextcloud_source_pairing_sync_update
BEFORE UPDATE OF status, data_source_id ON sync_logs
WHEN NEW.status = 'running'
 AND EXISTS (SELECT 1 FROM data_sources WHERE id = NEW.data_source_id AND type = 'nextcloud')
 AND NOT EXISTS (SELECT 1 FROM nextcloud_source_pairings p
     WHERE p.datasource_id = NEW.data_source_id AND p.tenant_id = NEW.tenant_id
       AND p.state = 'active')
BEGIN SELECT RAISE(ABORT, 'nextcloud_source_unpaired'); END;

-- The legacy soft-delete guard is intentionally not restored. Controlled KB
-- deletion soft-deletes first, then records one exact closure per binding.
-- A physical DELETE remains blocked while any pairing still names the KB.
CREATE TRIGGER nextcloud_paired_kb_hard_delete_guard
BEFORE DELETE ON knowledge_bases
WHEN EXISTS (SELECT 1 FROM nextcloud_source_pairings p WHERE p.knowledge_base_id = OLD.id)
BEGIN SELECT RAISE(ABORT, 'nextcloud_paired_kb_delete_forbidden'); END;

CREATE TRIGGER nextcloud_source_pairing_row_guard_update
BEFORE UPDATE ON nextcloud_source_pairings
WHEN NEW.operation_id IS NOT OLD.operation_id
  OR NEW.tenant_id IS NOT OLD.tenant_id
  OR NEW.knowledge_base_id IS NOT OLD.knowledge_base_id
  OR NEW.datasource_id IS NOT OLD.datasource_id
  OR NEW.nextcloud_instance_id IS NOT OLD.nextcloud_instance_id
  OR NEW.binding_id IS NOT OLD.binding_id
  OR NEW.datasource_base_url IS NOT OLD.datasource_base_url
  OR NEW.publication_epoch IS NOT OLD.publication_epoch
  OR (OLD.state = 'active' AND NEW.state IS NOT OLD.state)
  OR ((NEW.datasource_config_sha256 IS NOT OLD.datasource_config_sha256
    OR NEW.key_id IS NOT OLD.key_id) AND NOT EXISTS (
      SELECT 1 FROM nextcloud_source_rotations r
      WHERE r.pair_operation_id = OLD.operation_id AND r.state = 'committed'
        AND r.old_key_id = OLD.key_id AND r.new_key_id = NEW.key_id
        AND r.old_config_sha256 = OLD.datasource_config_sha256
        AND r.new_config_sha256 = NEW.datasource_config_sha256
  ))
BEGIN SELECT RAISE(ABORT, 'nextcloud_source_pairing_immutable'); END;

CREATE TRIGGER nextcloud_source_pairing_row_guard_delete
BEFORE DELETE ON nextcloud_source_pairings
WHEN OLD.state = 'active' OR EXISTS (
    SELECT 1 FROM data_sources d WHERE d.id = OLD.datasource_id AND d.status <> 'paused'
)
BEGIN SELECT RAISE(ABORT, 'nextcloud_source_pairing_delete_forbidden'); END;

CREATE TRIGGER nextcloud_pair_rebuild_remember
BEFORE DELETE ON nextcloud_source_pairings
BEGIN
    INSERT OR IGNORE INTO nextcloud_pair_rebuild_history(operation_id)
    VALUES (OLD.operation_id);
END;

CREATE TRIGGER nextcloud_virgin_pair_insert AFTER INSERT ON nextcloud_source_pairings
BEGIN
    INSERT INTO nextcloud_source_virgin(pair_operation_id, ever_touched)
    VALUES (NEW.operation_id, EXISTS (
        SELECT 1 FROM nextcloud_pair_rebuild_history
        WHERE operation_id = NEW.operation_id
    ));
END;

CREATE TRIGGER nextcloud_source_tombstones_nextcloud_source_pairings_insert
AFTER INSERT ON nextcloud_source_pairings
BEGIN
    INSERT INTO nextcloud_source_tombstones(tenant_id, scope_type, scope_id)
    SELECT NEW.tenant_id, 'tenant', CAST(NEW.tenant_id AS TEXT)
    WHERE NEW.tenant_id > 0 ON CONFLICT DO NOTHING;
    INSERT INTO nextcloud_source_tombstones(tenant_id, scope_type, scope_id)
    SELECT NEW.tenant_id, 'knowledge_base', NEW.knowledge_base_id
    WHERE NEW.tenant_id > 0 AND length(trim(NEW.knowledge_base_id)) > 0 ON CONFLICT DO NOTHING;
    INSERT INTO nextcloud_source_tombstones(tenant_id, scope_type, scope_id)
    SELECT NEW.tenant_id, 'datasource', NEW.datasource_id
    WHERE NEW.tenant_id > 0 AND length(trim(NEW.datasource_id)) > 0 ON CONFLICT DO NOTHING;
END;

CREATE TRIGGER nextcloud_upload_pair_insert AFTER INSERT ON nextcloud_source_pairings
BEGIN
    INSERT INTO nextcloud_upload_source_versions(datasource_id,revision)
    VALUES(NEW.datasource_id,1)
    ON CONFLICT(datasource_id) DO UPDATE SET revision=revision+1;
END;
CREATE TRIGGER nextcloud_upload_pair_update AFTER UPDATE ON nextcloud_source_pairings
WHEN OLD.tenant_id IS NOT NEW.tenant_id OR OLD.knowledge_base_id IS NOT NEW.knowledge_base_id
 OR OLD.datasource_id IS NOT NEW.datasource_id OR OLD.nextcloud_instance_id IS NOT NEW.nextcloud_instance_id
 OR OLD.binding_id IS NOT NEW.binding_id OR OLD.datasource_base_url IS NOT NEW.datasource_base_url
 OR OLD.datasource_config_sha256 IS NOT NEW.datasource_config_sha256
 OR OLD.publication_epoch IS NOT NEW.publication_epoch OR OLD.state IS NOT NEW.state
BEGIN
    INSERT INTO nextcloud_upload_source_versions(datasource_id,revision)
    VALUES(OLD.datasource_id,1)
    ON CONFLICT(datasource_id) DO UPDATE SET revision=revision+1;
    INSERT INTO nextcloud_upload_source_versions(datasource_id,revision)
    SELECT NEW.datasource_id,1 WHERE NEW.datasource_id IS NOT OLD.datasource_id
    ON CONFLICT(datasource_id) DO UPDATE SET revision=revision+1;
END;
CREATE TRIGGER nextcloud_upload_pair_delete AFTER DELETE ON nextcloud_source_pairings
BEGIN
    INSERT INTO nextcloud_upload_source_versions(datasource_id,revision)
    VALUES(OLD.datasource_id,1)
    ON CONFLICT(datasource_id) DO UPDATE SET revision=revision+1;
END;

CREATE TRIGGER nextcloud_target_nextcloud_source_pairings_insert
BEFORE INSERT ON nextcloud_source_pairings
WHEN (EXISTS(SELECT 1 FROM nextcloud_onboarding_tenant_deletions td WHERE td.tenant_id=NEW.tenant_id)
 OR EXISTS(SELECT 1 FROM nextcloud_onboarding_target_closures tc
    WHERE tc.tenant_id=NEW.tenant_id AND tc.knowledge_base_id=NEW.knowledge_base_id))
BEGIN SELECT RAISE(ABORT,'nextcloud_target_closed'); END;
CREATE TRIGGER nextcloud_target_nextcloud_source_pairings_update
BEFORE UPDATE ON nextcloud_source_pairings
WHEN ((EXISTS(SELECT 1 FROM nextcloud_onboarding_tenant_deletions td WHERE td.tenant_id=OLD.tenant_id)
 OR EXISTS(SELECT 1 FROM nextcloud_onboarding_target_closures tc
    WHERE tc.tenant_id=OLD.tenant_id AND tc.knowledge_base_id=OLD.knowledge_base_id))
 OR (EXISTS(SELECT 1 FROM nextcloud_onboarding_tenant_deletions td WHERE td.tenant_id=NEW.tenant_id)
 OR EXISTS(SELECT 1 FROM nextcloud_onboarding_target_closures tc
    WHERE tc.tenant_id=NEW.tenant_id AND tc.knowledge_base_id=NEW.knowledge_base_id)))
 AND NEW.state NOT IN ('retired','decommissioned','withdrawn')
BEGIN SELECT RAISE(ABORT,'nextcloud_target_closed'); END;
CREATE TRIGGER nextcloud_target_nextcloud_source_pairings_delete
BEFORE DELETE ON nextcloud_source_pairings
WHEN (EXISTS(SELECT 1 FROM nextcloud_onboarding_tenant_deletions td WHERE td.tenant_id=OLD.tenant_id)
 OR EXISTS(SELECT 1 FROM nextcloud_onboarding_target_closures tc
    WHERE tc.tenant_id=OLD.tenant_id AND tc.knowledge_base_id=OLD.knowledge_base_id))
BEGIN SELECT RAISE(ABORT,'nextcloud_target_closed'); END;

CREATE TRIGGER nextcloud_virgin_sync_log BEFORE INSERT ON sync_logs
BEGIN
    SELECT RAISE(ABORT, 'nextcloud_source_decommissioned')
    WHERE EXISTS (SELECT 1 FROM nextcloud_source_decommissions WHERE datasource_id = NEW.data_source_id);
    UPDATE nextcloud_source_virgin SET ever_touched = 1
    WHERE pair_operation_id = (SELECT operation_id FROM nextcloud_source_pairings
        WHERE datasource_id = NEW.data_source_id);
END;
CREATE TRIGGER nextcloud_virgin_knowledge BEFORE INSERT ON knowledges
BEGIN
    SELECT RAISE(ABORT, 'nextcloud_source_decommissioned')
    WHERE EXISTS (
        SELECT 1 FROM nextcloud_source_decommissions d
        WHERE d.knowledge_base_id=NEW.knowledge_base_id
          AND (
            COALESCE(CASE WHEN json_valid(NEW.metadata)
                THEN json_extract(NEW.metadata,'$.datasource_id') END,'')=''
            OR d.datasource_id=CASE WHEN json_valid(NEW.metadata)
                THEN json_extract(NEW.metadata,'$.datasource_id') END
          )
    );
    UPDATE nextcloud_source_virgin SET ever_touched = 1
    WHERE pair_operation_id IN (
        SELECT p.operation_id FROM nextcloud_source_pairings p
        WHERE p.knowledge_base_id=NEW.knowledge_base_id
          AND (
            COALESCE(CASE WHEN json_valid(NEW.metadata)
                THEN json_extract(NEW.metadata,'$.datasource_id') END,'')=''
            OR p.datasource_id=CASE WHEN json_valid(NEW.metadata)
                THEN json_extract(NEW.metadata,'$.datasource_id') END
          )
    );
END;
CREATE TRIGGER nextcloud_virgin_chunk BEFORE INSERT ON chunks
BEGIN
    SELECT RAISE(ABORT, 'nextcloud_source_decommissioned')
    WHERE EXISTS (
        SELECT 1
        FROM knowledges k
        JOIN nextcloud_source_decommissions d
          ON d.knowledge_base_id=k.knowledge_base_id
         AND (
           COALESCE(CASE WHEN json_valid(k.metadata)
               THEN json_extract(k.metadata,'$.datasource_id') END,'')=''
           OR d.datasource_id=CASE WHEN json_valid(k.metadata)
               THEN json_extract(k.metadata,'$.datasource_id') END
         )
        WHERE k.id=NEW.knowledge_id
          AND k.tenant_id=NEW.tenant_id
          AND k.knowledge_base_id=NEW.knowledge_base_id
    );
    UPDATE nextcloud_source_virgin SET ever_touched = 1
    WHERE pair_operation_id IN (
        SELECT p.operation_id
        FROM nextcloud_source_pairings p
        JOIN knowledges k ON k.id=NEW.knowledge_id
         AND k.tenant_id=NEW.tenant_id
         AND k.knowledge_base_id=NEW.knowledge_base_id
        WHERE p.knowledge_base_id=NEW.knowledge_base_id
          AND (
            COALESCE(CASE WHEN json_valid(k.metadata)
                THEN json_extract(k.metadata,'$.datasource_id') END,'')=''
            OR p.datasource_id=CASE WHEN json_valid(k.metadata)
                THEN json_extract(k.metadata,'$.datasource_id') END
          )
    );
END;
CREATE TRIGGER nextcloud_virgin_version BEFORE INSERT ON nextcloud_source_versions
BEGIN
    SELECT RAISE(ABORT, 'nextcloud_source_decommissioned')
    WHERE EXISTS (SELECT 1 FROM nextcloud_source_decommissions WHERE datasource_id = NEW.datasource_id);
    UPDATE nextcloud_source_virgin SET ever_touched = 1
    WHERE pair_operation_id = (SELECT operation_id FROM nextcloud_source_pairings
        WHERE datasource_id = NEW.datasource_id);
END;
CREATE TRIGGER nextcloud_virgin_event_connection BEFORE INSERT ON nextcloud_event_connections
BEGIN
    SELECT RAISE(ABORT, 'nextcloud_source_decommissioned')
    WHERE EXISTS (SELECT 1 FROM nextcloud_source_decommissions WHERE datasource_id = NEW.datasource_id);
    UPDATE nextcloud_source_virgin SET ever_touched = 1
    WHERE pair_operation_id = (SELECT operation_id FROM nextcloud_source_pairings
        WHERE datasource_id = NEW.datasource_id);
END;
CREATE TRIGGER nextcloud_onboarding_operation_activation_guard
BEFORE UPDATE OF state ON nextcloud_onboarding_operations
WHEN NEW.state='active' AND NOT EXISTS(
    SELECT 1 FROM nextcloud_source_pairings p
    WHERE p.operation_id=NEW.operation_id AND p.tenant_id=NEW.tenant_id
      AND p.knowledge_base_id=NEW.knowledge_base_id
      AND p.nextcloud_instance_id=NEW.instance_id AND p.binding_id=NEW.binding_id
      AND p.state='active'
)
BEGIN SELECT RAISE(ABORT,'nextcloud_onboarding_unpaired'); END;

CREATE TRIGGER nextcloud_graph_recovery_controls_fence
BEFORE INSERT ON nextcloud_graph_records
BEGIN
 SELECT CASE WHEN EXISTS(
   SELECT 1 FROM nextcloud_content_fences f
   JOIN nextcloud_source_pairings p
     ON p.tenant_id=f.tenant_id AND p.knowledge_base_id=f.knowledge_base_id
    AND p.datasource_id=f.datasource_id
   JOIN nextcloud_control_recovery_gates g
     ON g.instance_id=p.nextcloud_instance_id AND g.closed=TRUE
   WHERE f.tenant_id=NEW.tenant_id AND f.knowledge_base_id=NEW.knowledge_base_id
     AND f.knowledge_id=NEW.knowledge_id
 )
 AND NOT EXISTS(
   SELECT 1 FROM nextcloud_recovery_build_leases m
   JOIN nextcloud_publication_recovery_scopes s
     ON s.plan_sha256=m.plan_sha256 AND s.operation_id=m.operation_id
    AND s.tenant_id=m.tenant_id AND s.knowledge_base_id=m.knowledge_base_id
    AND s.datasource_id=m.datasource_id
   JOIN nextcloud_control_recovery_gates g
     ON g.instance_id=s.instance_id AND g.plan_sha256=s.plan_sha256 AND g.closed=TRUE
   JOIN nextcloud_content_fences f
     ON f.tenant_id=m.tenant_id AND f.knowledge_base_id=m.knowledge_base_id
    AND f.knowledge_id=m.knowledge_id
   JOIN nextcloud_content_fences kb
     ON kb.tenant_id=m.tenant_id AND kb.knowledge_base_id=m.knowledge_base_id
    AND kb.knowledge_id=''
   WHERE m.lease_id=NEW.build_lease_id AND m.tenant_id=NEW.tenant_id
     AND m.knowledge_base_id=NEW.knowledge_base_id AND m.knowledge_id=NEW.knowledge_id
     AND m.build_epoch=NEW.generation AND f.datasource_id=m.datasource_id
     AND f.epoch=m.build_epoch AND f.state='open' AND kb.state='open'
     AND kb.epoch=m.kb_epoch AND kb.epoch>s.closed_kb_epoch AND s.state='reconciled'
     AND s.reconciliation_sha256=m.manifest_sha256
 )
 THEN RAISE(ABORT,'nextcloud_graph_recovery_permit_required') END;
END;
