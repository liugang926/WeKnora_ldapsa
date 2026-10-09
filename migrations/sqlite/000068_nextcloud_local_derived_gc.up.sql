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
INSERT INTO nextcloud_derived_protocol_boundary VALUES(1,CAST((julianday('now')-2440587.5)*86400000 AS INTEGER));
CREATE TRIGGER nextcloud_derived_plan_update BEFORE UPDATE ON nextcloud_derived_backend_plans BEGIN SELECT RAISE(ABORT,'immutable derived storage evidence'); END;
CREATE TRIGGER nextcloud_derived_plan_delete BEFORE DELETE ON nextcloud_derived_backend_plans BEGIN SELECT RAISE(ABORT,'immutable derived storage evidence'); END;
CREATE TRIGGER nextcloud_derived_receipt_update BEFORE UPDATE ON nextcloud_derived_gc_receipts BEGIN SELECT RAISE(ABORT,'immutable derived storage evidence'); END;
CREATE TRIGGER nextcloud_derived_receipt_delete BEFORE DELETE ON nextcloud_derived_gc_receipts BEGIN SELECT RAISE(ABORT,'immutable derived storage evidence'); END;

CREATE TABLE nextcloud_derived_origin_witnesses(admission_id TEXT PRIMARY KEY,admitted_at_ms BIGINT NOT NULL);
CREATE TRIGGER nextcloud_derived_origin_witness AFTER INSERT ON knowledge_input_admissions BEGIN INSERT INTO nextcloud_derived_origin_witnesses VALUES(NEW.admission_id,CAST((julianday('now')-2440587.5)*86400000 AS INTEGER)); END;

CREATE TRIGGER nextcloud_derived_manifest_fixed_update BEFORE UPDATE ON nextcloud_derived_gc_manifests WHEN NEW.job_id<>OLD.job_id OR NEW.plan_sha256<>OLD.plan_sha256 OR NEW.inventory_json<>OLD.inventory_json OR NEW.inventory_sha256<>OLD.inventory_sha256 OR NEW.created_at_ms<>OLD.created_at_ms OR NEW.physical_items<>OLD.physical_items OR NEW.retained_audit_items<>OLD.retained_audit_items OR NEW.audit_retain_until_ms<>OLD.audit_retain_until_ms BEGIN SELECT RAISE(ABORT,'immutable derived inventory'); END;
CREATE TRIGGER nextcloud_derived_manifest_fixed_delete BEFORE DELETE ON nextcloud_derived_gc_manifests BEGIN SELECT RAISE(ABORT,'immutable derived inventory'); END;

CREATE TABLE nextcloud_derived_write_artifacts (
 tenant_id BIGINT NOT NULL,knowledge_base_id TEXT NOT NULL,knowledge_id TEXT NOT NULL,item_kind TEXT NOT NULL,object_ref TEXT NOT NULL,
 plan_sha256 TEXT NOT NULL,identity_sha256 TEXT NOT NULL,dimension BIGINT NOT NULL,written_at_ms BIGINT NOT NULL,
 PRIMARY KEY(tenant_id,knowledge_base_id,knowledge_id,item_kind,object_ref)
);
CREATE TRIGGER nextcloud_derived_artifact_update BEFORE UPDATE ON nextcloud_derived_write_artifacts BEGIN SELECT RAISE(ABORT,'immutable derived writer evidence'); END;
CREATE TRIGGER nextcloud_derived_artifact_delete BEFORE DELETE ON nextcloud_derived_write_artifacts BEGIN SELECT RAISE(ABORT,'immutable derived writer evidence'); END;
CREATE TABLE nextcloud_derived_new_knowledges(knowledge_id TEXT PRIMARY KEY,created_at_ms BIGINT NOT NULL);
CREATE TRIGGER nextcloud_derived_knowledge_witness AFTER INSERT ON knowledges WHEN NEW.channel='nextcloud' BEGIN INSERT INTO nextcloud_derived_new_knowledges VALUES(NEW.id,CAST((julianday('now')-2440587.5)*86400000 AS INTEGER)); END;
CREATE INDEX nextcloud_derived_artifact_ref ON nextcloud_derived_write_artifacts(item_kind,object_ref);
