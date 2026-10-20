CREATE TABLE nextcloud_recovery_build_leases (
 lease_id TEXT PRIMARY KEY, plan_sha256 TEXT NOT NULL, operation_id TEXT NOT NULL,
 tenant_id BIGINT NOT NULL,knowledge_base_id TEXT NOT NULL,datasource_id TEXT NOT NULL,knowledge_id TEXT NOT NULL,
 build_epoch BIGINT NOT NULL,kb_epoch BIGINT NOT NULL,manifest_sha256 TEXT NOT NULL,verified_at_ms BIGINT NOT NULL
);
CREATE TRIGGER nextcloud_recovery_build_permit_insert BEFORE INSERT ON nextcloud_recovery_build_leases BEGIN
 SELECT CASE WHEN NOT EXISTS(SELECT 1 FROM nextcloud_content_leases l JOIN nextcloud_publication_recovery_scopes s ON s.plan_sha256=NEW.plan_sha256 AND s.operation_id=NEW.operation_id JOIN nextcloud_control_recovery_gates g ON g.instance_id=s.instance_id AND g.plan_sha256=s.plan_sha256 JOIN nextcloud_content_fences kb ON kb.tenant_id=s.tenant_id AND kb.knowledge_base_id=s.knowledge_base_id AND kb.knowledge_id='' WHERE l.lease_id=NEW.lease_id AND l.tenant_id=NEW.tenant_id AND l.knowledge_base_id=NEW.knowledge_base_id AND l.knowledge_id=NEW.knowledge_id AND l.kind='build' AND l.epoch=NEW.build_epoch AND l.released_at_ms IS NULL AND s.tenant_id=NEW.tenant_id AND s.knowledge_base_id=NEW.knowledge_base_id AND s.datasource_id=NEW.datasource_id AND s.state='reconciled' AND s.reconciliation_sha256=NEW.manifest_sha256 AND g.closed=TRUE AND kb.state='open' AND kb.epoch=NEW.kb_epoch AND kb.epoch>s.closed_kb_epoch) THEN RAISE(ABORT,'nextcloud_recovery_build_not_admitted') END;
END;
CREATE TRIGGER nextcloud_recovery_build_permit_immutable BEFORE UPDATE ON nextcloud_recovery_build_leases BEGIN SELECT RAISE(ABORT,'nextcloud_recovery_build_permit_immutable'); END;
CREATE TRIGGER nextcloud_recovery_build_permit_retained BEFORE DELETE ON nextcloud_recovery_build_leases BEGIN SELECT RAISE(ABORT,'nextcloud_recovery_build_permit_immutable'); END;
