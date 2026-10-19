CREATE TABLE nextcloud_publication_recovery_receipts (
 plan_sha256 TEXT PRIMARY KEY CHECK (length(plan_sha256)=64),
 instance_id TEXT NOT NULL, stream_id TEXT NOT NULL,
 checkpoint_record_sha256 TEXT NOT NULL, recovery_record_sha256 TEXT NOT NULL,
 checkpoint_sequence BIGINT NOT NULL CHECK(checkpoint_sequence>=0),
 through_sequence BIGINT NOT NULL CHECK(through_sequence>=checkpoint_sequence),
 inventory_sha256 TEXT NOT NULL CHECK(length(inventory_sha256)=64),
 plan_json TEXT NOT NULL, pair_count INTEGER NOT NULL CHECK(pair_count>0),
 closed_at TIMESTAMPTZ NOT NULL,
 UNIQUE(instance_id,stream_id,checkpoint_record_sha256)
);
CREATE TABLE nextcloud_publication_recovery_scopes (
 plan_sha256 TEXT NOT NULL REFERENCES nextcloud_publication_recovery_receipts(plan_sha256) ON DELETE RESTRICT,
 operation_id TEXT NOT NULL, tenant_id BIGINT NOT NULL CHECK(tenant_id>0),
 knowledge_base_id TEXT NOT NULL, datasource_id TEXT NOT NULL,
 instance_id TEXT NOT NULL, binding_id TEXT NOT NULL,
 pair_sha256 TEXT NOT NULL CHECK(length(pair_sha256)=64),
 retired_generations_json TEXT NOT NULL CHECK(retired_generations_json<>''),
 closed_kb_epoch BIGINT NOT NULL CHECK(closed_kb_epoch>0),
 state TEXT NOT NULL CHECK(state IN ('closed','reconciled')),
 reconciliation_sha256 TEXT NOT NULL DEFAULT '', reconciliation_json TEXT NOT NULL DEFAULT '', reconciled_at TIMESTAMPTZ,
 PRIMARY KEY(plan_sha256,operation_id),
 CHECK((state='closed' AND reconciliation_sha256='' AND reconciliation_json='' AND reconciled_at IS NULL) OR
       (state='reconciled' AND length(reconciliation_sha256)=64 AND reconciliation_json<>'' AND reconciled_at IS NOT NULL))
);
CREATE INDEX idx_nextcloud_recovery_closed_scope ON nextcloud_publication_recovery_scopes(datasource_id,tenant_id,state);
CREATE FUNCTION nextcloud_recovery_admission() RETURNS trigger LANGUAGE plpgsql AS $guard$ BEGIN
 IF TG_TABLE_NAME='data_sources' THEN IF NEW.type='nextcloud' AND NEW.status<>'paused' AND EXISTS(SELECT 1 FROM nextcloud_publication_recovery_scopes s WHERE s.datasource_id=NEW.id AND s.tenant_id=NEW.tenant_id AND s.state='closed') THEN RAISE EXCEPTION 'nextcloud_recovery_closed'; END IF; ELSE IF NEW.state IN ('staging','published') AND (EXISTS(SELECT 1 FROM nextcloud_publication_recovery_scopes s WHERE s.datasource_id=NEW.datasource_id AND s.tenant_id=NEW.tenant_id AND s.knowledge_base_id=NEW.knowledge_base_id AND s.state='closed') OR EXISTS(SELECT 1 FROM nextcloud_content_fences f WHERE f.tenant_id=NEW.tenant_id AND f.knowledge_base_id=NEW.knowledge_base_id AND f.knowledge_id=NEW.candidate_knowledge_id AND f.state<>'open')) THEN RAISE EXCEPTION 'nextcloud_recovery_closed'; END IF; END IF; RETURN NEW; END $guard$;
CREATE TRIGGER nextcloud_recovery_data_sources BEFORE INSERT OR UPDATE ON data_sources FOR EACH ROW EXECUTE FUNCTION nextcloud_recovery_admission();
CREATE TRIGGER nextcloud_recovery_nextcloud_source_versions BEFORE INSERT OR UPDATE ON nextcloud_source_versions FOR EACH ROW EXECUTE FUNCTION nextcloud_recovery_admission();
CREATE FUNCTION nextcloud_recovery_receipt_immutable() RETURNS trigger LANGUAGE plpgsql AS $guard$ BEGIN RAISE EXCEPTION 'nextcloud_recovery_receipt_immutable'; END $guard$;
CREATE TRIGGER nextcloud_recovery_receipt_immutable BEFORE UPDATE OR DELETE ON nextcloud_publication_recovery_receipts FOR EACH ROW EXECUTE FUNCTION nextcloud_recovery_receipt_immutable();
CREATE TRIGGER nextcloud_recovery_receipt_no_truncate BEFORE TRUNCATE ON nextcloud_publication_recovery_receipts FOR EACH STATEMENT EXECUTE FUNCTION nextcloud_recovery_receipt_immutable();
CREATE FUNCTION nextcloud_recovery_scope_immutable() RETURNS trigger LANGUAGE plpgsql AS $guard$ BEGIN IF TG_OP='DELETE' OR (NEW.plan_sha256 IS DISTINCT FROM OLD.plan_sha256 OR NEW.operation_id IS DISTINCT FROM OLD.operation_id OR NEW.tenant_id IS DISTINCT FROM OLD.tenant_id OR NEW.knowledge_base_id IS DISTINCT FROM OLD.knowledge_base_id OR NEW.datasource_id IS DISTINCT FROM OLD.datasource_id OR NEW.instance_id IS DISTINCT FROM OLD.instance_id OR NEW.binding_id IS DISTINCT FROM OLD.binding_id OR NEW.pair_sha256 IS DISTINCT FROM OLD.pair_sha256 OR NEW.retired_generations_json IS DISTINCT FROM OLD.retired_generations_json OR NEW.closed_kb_epoch IS DISTINCT FROM OLD.closed_kb_epoch OR OLD.state<>'closed' OR NEW.state<>'reconciled') THEN RAISE EXCEPTION 'nextcloud_recovery_scope_immutable'; END IF; RETURN NEW; END $guard$;
CREATE TRIGGER nextcloud_recovery_scope_immutable BEFORE UPDATE OR DELETE ON nextcloud_publication_recovery_scopes FOR EACH ROW EXECUTE FUNCTION nextcloud_recovery_scope_immutable();
CREATE TRIGGER nextcloud_recovery_scope_no_truncate BEFORE TRUNCATE ON nextcloud_publication_recovery_scopes FOR EACH STATEMENT EXECUTE FUNCTION nextcloud_recovery_receipt_immutable();
CREATE FUNCTION nextcloud_recovery_source_identity() RETURNS trigger LANGUAGE plpgsql AS $guard$ BEGIN IF EXISTS(SELECT 1 FROM nextcloud_publication_recovery_scopes s WHERE s.datasource_id=OLD.id AND s.state='closed') AND (NEW.config IS DISTINCT FROM OLD.config OR NEW.type IS DISTINCT FROM OLD.type OR NEW.tenant_id IS DISTINCT FROM OLD.tenant_id OR NEW.knowledge_base_id IS DISTINCT FROM OLD.knowledge_base_id OR NEW.deleted_at IS DISTINCT FROM OLD.deleted_at) THEN RAISE EXCEPTION 'nextcloud_recovery_source_immutable'; END IF; RETURN NEW; END $guard$;
CREATE TRIGGER nextcloud_recovery_source_identity BEFORE UPDATE ON data_sources FOR EACH ROW EXECUTE FUNCTION nextcloud_recovery_source_identity();
