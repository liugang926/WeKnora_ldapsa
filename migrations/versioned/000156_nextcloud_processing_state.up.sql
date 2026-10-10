ALTER TABLE data_sources ADD COLUMN sync_enabled BOOLEAN NOT NULL DEFAULT TRUE;
CREATE TABLE nextcloud_file_states (
 connection_id TEXT NOT NULL, file_id TEXT NOT NULL, publication_epoch BIGINT NOT NULL,
 head_revision BIGINT NOT NULL CHECK(head_revision>0), status_revision BIGINT NOT NULL CHECK(status_revision>0),
 config_revision BIGINT NOT NULL, head_identity TEXT NOT NULL, config_digest TEXT NOT NULL,
 payload_json TEXT NOT NULL, body_digest TEXT NOT NULL,latest_event_id TEXT NOT NULL, last_verified_at BIGINT NOT NULL,
 occurred_at BIGINT NOT NULL, PRIMARY KEY(connection_id,file_id,publication_epoch)
);
CREATE TABLE nextcloud_status_outbox (
 event_id TEXT PRIMARY KEY, connection_id TEXT NOT NULL, file_id TEXT NOT NULL,
 publication_epoch BIGINT NOT NULL, head_revision BIGINT NOT NULL, status_revision BIGINT NOT NULL,
 payload_json TEXT NOT NULL, state TEXT NOT NULL DEFAULT 'pending' CHECK(state IN ('pending','delivered','superseded')),
 attempts INTEGER NOT NULL DEFAULT 0, priority INTEGER NOT NULL DEFAULT 1,next_attempt_at BIGINT NOT NULL DEFAULT 0,
 last_error_code TEXT NOT NULL DEFAULT '', created_at BIGINT NOT NULL, delivered_at BIGINT
);
CREATE INDEX idx_nextcloud_status_outbox_due ON nextcloud_status_outbox(state,priority,next_attempt_at);
CREATE INDEX idx_nextcloud_status_scope ON nextcloud_status_outbox(connection_id,file_id,publication_epoch);
CREATE TABLE nextcloud_graph_plans (
 tenant_id BIGINT NOT NULL,knowledge_id TEXT NOT NULL,attempt INTEGER NOT NULL,
 knowledge_base_id TEXT NOT NULL,expected INTEGER NOT NULL CHECK(expected>=0),
 enabled BOOLEAN NOT NULL,required BOOLEAN NOT NULL,config_digest TEXT NOT NULL,
 manifest_digest TEXT NOT NULL,created_at BIGINT NOT NULL,
 PRIMARY KEY(tenant_id,knowledge_id,attempt)
);
CREATE TABLE nextcloud_graph_tasks (
 operation_id TEXT PRIMARY KEY, tenant_id BIGINT NOT NULL, knowledge_base_id TEXT NOT NULL,
 knowledge_id TEXT NOT NULL, attempt INTEGER NOT NULL, chunk_id TEXT NOT NULL,
 build_epoch BIGINT NOT NULL, config_digest TEXT NOT NULL,config_revision BIGINT NOT NULL,
 payload_json TEXT NOT NULL DEFAULT '',queue_state TEXT NOT NULL DEFAULT 'unprepared' CHECK(queue_state IN ('unprepared','pending','queued')),queue_attempts INTEGER NOT NULL DEFAULT 0,queue_next_at BIGINT NOT NULL DEFAULT 0,
 state TEXT NOT NULL CHECK(state IN ('pending','running','succeeded','succeeded_empty','failed','skipped','cancelled','unverified')),
 persisted BOOLEAN NOT NULL DEFAULT FALSE, finalized BOOLEAN NOT NULL DEFAULT FALSE, error_code TEXT NOT NULL DEFAULT '',
 created_at BIGINT NOT NULL, completed_at BIGINT,
 UNIQUE(tenant_id,knowledge_id,attempt,chunk_id)
);
CREATE TABLE nextcloud_graph_records (
 operation_id TEXT PRIMARY KEY REFERENCES nextcloud_graph_tasks(operation_id) ON DELETE RESTRICT,
 tenant_id BIGINT NOT NULL, knowledge_base_id TEXT NOT NULL, knowledge_id TEXT NOT NULL,
 backend TEXT NOT NULL CHECK(backend='scoped_sql_v1'), generation BIGINT NOT NULL, build_lease_id TEXT NOT NULL,
 body_json TEXT NOT NULL, body_sha256 TEXT NOT NULL, node_count INTEGER NOT NULL,
 edge_count INTEGER NOT NULL, committed_at BIGINT NOT NULL
);
-- The target provider itself enforces generations, including direct repository
-- mutations. Checks share the durable KB/exact row locks with source retirement.
CREATE FUNCTION nextcloud_graph_record_fence() RETURNS trigger LANGUAGE plpgsql AS $guard$
BEGIN
 IF TG_OP='UPDATE' THEN RAISE EXCEPTION 'nextcloud_graph_record_immutable'; END IF;
 IF TG_OP='INSERT' THEN
  PERFORM 1 FROM nextcloud_content_fences WHERE tenant_id=NEW.tenant_id AND knowledge_base_id=NEW.knowledge_base_id AND knowledge_id='' FOR UPDATE;
  PERFORM 1 FROM nextcloud_content_fences WHERE tenant_id=NEW.tenant_id AND knowledge_base_id=NEW.knowledge_base_id AND knowledge_id=NEW.knowledge_id FOR UPDATE;
  IF NOT EXISTS(SELECT 1 FROM nextcloud_content_fences f JOIN nextcloud_content_fences kb ON kb.tenant_id=f.tenant_id AND kb.knowledge_base_id=f.knowledge_base_id AND kb.knowledge_id='' JOIN nextcloud_source_versions v ON v.tenant_id=f.tenant_id AND v.knowledge_base_id=f.knowledge_base_id AND v.datasource_id=f.datasource_id AND v.external_id=f.external_id JOIN nextcloud_graph_tasks t ON t.operation_id=NEW.operation_id WHERE f.tenant_id=NEW.tenant_id AND f.knowledge_base_id=NEW.knowledge_base_id AND f.knowledge_id=NEW.knowledge_id AND f.state='open' AND kb.state='open' AND f.epoch=NEW.generation AND t.build_epoch=f.epoch AND t.state='running' AND t.tenant_id=f.tenant_id AND t.knowledge_id=f.knowledge_id AND v.candidate_knowledge_id=f.knowledge_id AND v.state IN ('staging','published') AND t.config_revision=(SELECT revision FROM knowledge_configuration_control_versions WHERE knowledge_base_id=f.knowledge_base_id) AND t.attempt=(SELECT MAX(attempt) FROM knowledge_processing_spans WHERE knowledge_id=f.knowledge_id) AND EXISTS(SELECT 1 FROM nextcloud_content_leases l WHERE l.lease_id=NEW.build_lease_id AND l.tenant_id=f.tenant_id AND l.knowledge_base_id=f.knowledge_base_id AND l.knowledge_id=f.knowledge_id AND l.kind='build' AND l.epoch=f.epoch AND l.released_at_ms IS NULL AND l.expires_at_ms>floor(extract(epoch from clock_timestamp())*1000))) THEN RAISE EXCEPTION 'nextcloud_graph_generation_closed'; END IF;
  RETURN NEW;
 END IF;
 IF NOT EXISTS(SELECT 1 FROM nextcloud_content_fences f WHERE f.tenant_id=OLD.tenant_id AND f.knowledge_base_id=OLD.knowledge_base_id AND f.knowledge_id=OLD.knowledge_id AND f.state='deleting' AND f.claim_token<>'' AND f.claim_until_ms>floor(extract(epoch from clock_timestamp())*1000)) THEN RAISE EXCEPTION 'nextcloud_graph_delete_requires_gc_claim'; END IF;
 RETURN OLD;
END $guard$;
CREATE TRIGGER nextcloud_graph_record_fence BEFORE INSERT OR UPDATE OR DELETE ON nextcloud_graph_records FOR EACH ROW EXECUTE FUNCTION nextcloud_graph_record_fence();
CREATE TABLE nextcloud_source_status_epochs (
 connection_id TEXT PRIMARY KEY, publication_epoch BIGINT NOT NULL CHECK(publication_epoch>=0),
 pair_operation_id TEXT NOT NULL, verified_at BIGINT NOT NULL
);
CREATE FUNCTION nextcloud_state_epoch_monotonic() RETURNS trigger LANGUAGE plpgsql AS $guard$ BEGIN IF NEW.publication_epoch<OLD.publication_epoch OR NEW.pair_operation_id<>OLD.pair_operation_id THEN RAISE EXCEPTION 'nextcloud_state_epoch_regression'; END IF; RETURN NEW; END $guard$;
CREATE TRIGGER nextcloud_state_epoch_monotonic BEFORE UPDATE ON nextcloud_source_status_epochs FOR EACH ROW EXECUTE FUNCTION nextcloud_state_epoch_monotonic();
ALTER TABLE nextcloud_gc_items DROP CONSTRAINT IF EXISTS nextcloud_gc_items_kind_check;
ALTER TABLE nextcloud_gc_items ADD CONSTRAINT nextcloud_gc_items_kind_check CHECK(kind IN ('knowledge','source_file','extracted_image','derived_index','derived_chunk','postgres_embedding','sqlite_embedding','scoped_sql_graph'));

CREATE FUNCTION nextcloud_graph_plan_immutable() RETURNS trigger LANGUAGE plpgsql AS $guard$ BEGIN RAISE EXCEPTION 'nextcloud_graph_plan_immutable'; END $guard$;
CREATE TRIGGER nextcloud_graph_plan_immutable BEFORE UPDATE OR DELETE ON nextcloud_graph_plans FOR EACH ROW EXECUTE FUNCTION nextcloud_graph_plan_immutable();
