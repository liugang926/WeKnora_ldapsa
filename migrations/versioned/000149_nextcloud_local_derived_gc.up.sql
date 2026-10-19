CREATE TABLE nextcloud_derived_backend_plans (
 tenant_id BIGINT NOT NULL,knowledge_base_id TEXT NOT NULL,knowledge_id TEXT NOT NULL,
 datasource_id TEXT NOT NULL,external_id TEXT NOT NULL,origin_admission_id TEXT NOT NULL,
 backend_plan TEXT NOT NULL,plan_sha256 TEXT NOT NULL,created_at_ms BIGINT NOT NULL,
 PRIMARY KEY(tenant_id,knowledge_base_id,knowledge_id)
);
CREATE TABLE nextcloud_derived_gc_manifests (
 job_id TEXT PRIMARY KEY,plan_sha256 TEXT NOT NULL,inventory_json TEXT NOT NULL,inventory_sha256 TEXT NOT NULL,
 state TEXT NOT NULL CHECK(state IN ('sealed','collected')),created_at_ms BIGINT NOT NULL,completed_at_ms BIGINT,
 physical_items INTEGER NOT NULL,physical_collected INTEGER NOT NULL DEFAULT 0,protected_items INTEGER NOT NULL DEFAULT 0,retained_audit_items INTEGER NOT NULL,
 audit_retain_until_ms BIGINT NOT NULL
);
CREATE TABLE nextcloud_derived_gc_receipts (
 job_id TEXT NOT NULL,item_kind TEXT NOT NULL,object_ref TEXT NOT NULL,scope_sha256 TEXT NOT NULL,
 before_sha256 TEXT NOT NULL,deleted_rows BIGINT NOT NULL,confirmed_bytes BIGINT NOT NULL,
 claim_token TEXT NOT NULL,claim_epoch BIGINT NOT NULL,completed_at_ms BIGINT NOT NULL,
 PRIMARY KEY(job_id,item_kind,object_ref)
);
CREATE TABLE nextcloud_derived_protocol_boundary(id INTEGER PRIMARY KEY CHECK(id=1),installed_at_ms BIGINT NOT NULL);
INSERT INTO nextcloud_derived_protocol_boundary VALUES(1,(extract(epoch FROM clock_timestamp())*1000)::bigint);
CREATE FUNCTION nextcloud_derived_evidence_immutable() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'immutable derived storage evidence'; END $$;
CREATE TRIGGER nextcloud_derived_plan_immutable BEFORE UPDATE OR DELETE ON nextcloud_derived_backend_plans FOR EACH ROW EXECUTE FUNCTION nextcloud_derived_evidence_immutable();
CREATE TRIGGER nextcloud_derived_receipt_immutable BEFORE UPDATE OR DELETE ON nextcloud_derived_gc_receipts FOR EACH ROW EXECUTE FUNCTION nextcloud_derived_evidence_immutable();

CREATE TABLE nextcloud_derived_origin_witnesses(admission_id TEXT PRIMARY KEY,admitted_at_ms BIGINT NOT NULL);
CREATE FUNCTION nextcloud_derived_new_origin() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN INSERT INTO nextcloud_derived_origin_witnesses VALUES(NEW.admission_id,(extract(epoch FROM clock_timestamp())*1000)::bigint); RETURN NEW; END $$;
CREATE TRIGGER nextcloud_derived_origin_witness AFTER INSERT ON knowledge_input_admissions FOR EACH ROW EXECUTE FUNCTION nextcloud_derived_new_origin();

CREATE FUNCTION nextcloud_derived_manifest_fixed() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF TG_OP='DELETE' OR NEW.job_id IS DISTINCT FROM OLD.job_id OR NEW.plan_sha256 IS DISTINCT FROM OLD.plan_sha256 OR NEW.inventory_json IS DISTINCT FROM OLD.inventory_json OR NEW.inventory_sha256 IS DISTINCT FROM OLD.inventory_sha256 OR NEW.created_at_ms IS DISTINCT FROM OLD.created_at_ms OR NEW.physical_items IS DISTINCT FROM OLD.physical_items OR NEW.retained_audit_items IS DISTINCT FROM OLD.retained_audit_items OR NEW.audit_retain_until_ms IS DISTINCT FROM OLD.audit_retain_until_ms THEN RAISE EXCEPTION 'immutable derived inventory'; END IF; RETURN NEW; END $$;
CREATE TRIGGER nextcloud_derived_manifest_fixed BEFORE UPDATE OR DELETE ON nextcloud_derived_gc_manifests FOR EACH ROW EXECUTE FUNCTION nextcloud_derived_manifest_fixed();

CREATE TABLE nextcloud_derived_write_artifacts (
 tenant_id BIGINT NOT NULL,knowledge_base_id TEXT NOT NULL,knowledge_id TEXT NOT NULL,item_kind TEXT NOT NULL,object_ref TEXT NOT NULL,
 plan_sha256 TEXT NOT NULL,identity_sha256 TEXT NOT NULL,dimension BIGINT NOT NULL,written_at_ms BIGINT NOT NULL,
 PRIMARY KEY(tenant_id,knowledge_base_id,knowledge_id,item_kind,object_ref)
);
CREATE TRIGGER nextcloud_derived_artifact_immutable BEFORE UPDATE OR DELETE ON nextcloud_derived_write_artifacts FOR EACH ROW EXECUTE FUNCTION nextcloud_derived_evidence_immutable();
CREATE TABLE nextcloud_derived_new_knowledges(knowledge_id TEXT PRIMARY KEY,created_at_ms BIGINT NOT NULL);
CREATE FUNCTION nextcloud_derived_new_knowledge() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.channel='nextcloud' THEN INSERT INTO nextcloud_derived_new_knowledges VALUES(NEW.id,(extract(epoch FROM clock_timestamp())*1000)::bigint); END IF; RETURN NEW; END $$;
CREATE TRIGGER nextcloud_derived_knowledge_witness AFTER INSERT ON knowledges FOR EACH ROW EXECUTE FUNCTION nextcloud_derived_new_knowledge();
CREATE INDEX nextcloud_derived_artifact_ref ON nextcloud_derived_write_artifacts(item_kind,object_ref);
