-- Permanent exact-scope closure. This is never a physical-GC/withdrawal ACK.
CREATE TABLE nextcloud_onboarding_target_closures (
 operation_id TEXT PRIMARY KEY REFERENCES nextcloud_onboarding_operations(operation_id),
 closure_id TEXT NOT NULL UNIQUE CHECK(length(closure_id)=64),
 tenant_id BIGINT NOT NULL CHECK(tenant_id>0),knowledge_base_id TEXT NOT NULL,
 instance_id TEXT NOT NULL,binding_id TEXT NOT NULL,
 operation_revision BIGINT NOT NULL CHECK(operation_revision>0),
 reason TEXT NOT NULL CHECK(reason IN ('tenant_deleted','knowledge_base_deleted')),
 deletion_time_ms BIGINT NOT NULL CHECK(deletion_time_ms>0),
 closed_at_ms BIGINT NOT NULL CHECK(closed_at_ms>=deletion_time_ms),
 recovery_plan_sha256 TEXT NOT NULL DEFAULT '',
 UNIQUE(tenant_id,knowledge_base_id),UNIQUE(instance_id,binding_id));
CREATE TABLE nextcloud_onboarding_tenant_deletions (
 tenant_id BIGINT PRIMARY KEY CHECK(tenant_id>0),
 deletion_id TEXT NOT NULL UNIQUE CHECK(length(deletion_id)=64),
 deletion_time_ms BIGINT NOT NULL CHECK(deletion_time_ms>0),
 closed_at_ms BIGINT NOT NULL CHECK(closed_at_ms>=deletion_time_ms),
 recovery_plan_sha256 TEXT NOT NULL DEFAULT '');
-- Admissions and native SQL producers serialize against real owner deletion.
CREATE FUNCTION nextcloud_target_require_live(t BIGINT,k TEXT) RETURNS void LANGUAGE plpgsql AS $$ BEGIN
 IF EXISTS(SELECT 1 FROM nextcloud_onboarding_tenant_deletions td WHERE td.tenant_id=t) THEN RAISE EXCEPTION 'nextcloud_target_closed'; END IF;
 IF EXISTS(SELECT 1 FROM nextcloud_onboarding_operations o WHERE o.tenant_id=t AND o.knowledge_base_id=k) THEN
  PERFORM 1 FROM tenants WHERE id=t AND deleted_at IS NULL FOR UPDATE;
  IF NOT FOUND THEN RAISE EXCEPTION 'nextcloud_target_closed'; END IF;
  PERFORM 1 FROM knowledge_bases WHERE id=k AND tenant_id=t AND deleted_at IS NULL FOR UPDATE;
  IF NOT FOUND OR EXISTS(SELECT 1 FROM nextcloud_onboarding_target_closures c WHERE c.tenant_id=t AND c.knowledge_base_id=k) THEN RAISE EXCEPTION 'nextcloud_target_closed'; END IF;
 END IF;
END $$;
CREATE FUNCTION nextcloud_target_immutable() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
RAISE EXCEPTION 'nextcloud_target_closure_immutable';
END $$;
CREATE TRIGGER nextcloud_target_closure_immutable BEFORE UPDATE OR DELETE ON nextcloud_onboarding_target_closures FOR EACH ROW EXECUTE FUNCTION nextcloud_target_immutable();
CREATE TRIGGER nextcloud_tenant_deletion_immutable BEFORE UPDATE OR DELETE ON nextcloud_onboarding_tenant_deletions FOR EACH ROW EXECUTE FUNCTION nextcloud_target_immutable();
CREATE FUNCTION nextcloud_tenant_deletion_guard() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
IF NOT ((NEW.recovery_plan_sha256='' AND EXISTS(SELECT 1 FROM tenants t WHERE t.id=NEW.tenant_id AND t.deleted_at IS NOT NULL)) OR (NEW.recovery_plan_sha256<>'' AND EXISTS(SELECT 1 FROM nextcloud_publication_recovery_receipts r CROSS JOIN LATERAL jsonb_array_elements(r.plan_json::jsonb->'tenant_deletions') AS e(value) JOIN nextcloud_onboarding_operations o ON o.operation_id=e.value#>>'{tenant,operation_id}' AND o.instance_id=r.instance_id AND o.instance_id=e.value#>>'{tenant,instance_id}' AND o.binding_id=e.value#>>'{tenant,binding_id}' AND o.tenant_id=NEW.tenant_id WHERE r.plan_sha256=NEW.recovery_plan_sha256 AND e.value#>>'{tenant,tenant_id}'=CAST(NEW.tenant_id AS TEXT) AND e.value#>>'{tenant,deletion_id}'=NEW.deletion_id AND e.value#>>'{tenant,deletion_time_ms}'=CAST(NEW.deletion_time_ms AS TEXT) AND e.value#>>'{tenant,closed_at}'=CAST(NEW.closed_at_ms/1000 AS TEXT)))) THEN RAISE EXCEPTION 'nextcloud_target_unverified'; END IF; RETURN NEW;
END $$;
CREATE TRIGGER nextcloud_tenant_deletion_insert BEFORE INSERT ON nextcloud_onboarding_tenant_deletions FOR EACH ROW EXECUTE FUNCTION nextcloud_tenant_deletion_guard();
CREATE FUNCTION nextcloud_target_closure_guard() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
IF NOT EXISTS(SELECT 1 FROM nextcloud_onboarding_operations o WHERE o.operation_id=NEW.operation_id AND o.tenant_id=NEW.tenant_id AND o.knowledge_base_id=NEW.knowledge_base_id AND o.instance_id=NEW.instance_id AND o.binding_id=NEW.binding_id AND o.revision>=NEW.operation_revision) OR NOT ((NEW.recovery_plan_sha256='' AND ((NEW.reason='tenant_deleted' AND EXISTS(SELECT 1 FROM tenants t WHERE t.id=NEW.tenant_id AND t.deleted_at IS NOT NULL)) OR (NEW.reason='knowledge_base_deleted' AND EXISTS(SELECT 1 FROM knowledge_bases k WHERE k.id=NEW.knowledge_base_id AND k.tenant_id=NEW.tenant_id AND k.deleted_at IS NOT NULL)))) OR (NEW.recovery_plan_sha256<>'' AND EXISTS(SELECT 1 FROM nextcloud_publication_recovery_receipts r CROSS JOIN LATERAL jsonb_array_elements(r.plan_json::jsonb->'target_closures') AS e(value) WHERE r.plan_sha256=NEW.recovery_plan_sha256 AND r.instance_id=NEW.instance_id AND e.value#>>'{target,closure_id}'=NEW.closure_id AND e.value#>>'{target,operation_id}'=NEW.operation_id AND e.value#>>'{target,tenant_id}'=CAST(NEW.tenant_id AS TEXT) AND e.value#>>'{target,knowledge_base_id}'=NEW.knowledge_base_id AND e.value#>>'{target,instance_id}'=NEW.instance_id AND e.value#>>'{target,binding_id}'=NEW.binding_id AND e.value#>>'{target,operation_revision}'=CAST(NEW.operation_revision AS TEXT) AND e.value#>>'{target,reason}'=NEW.reason AND e.value#>>'{target,deletion_time_ms}'=CAST(NEW.deletion_time_ms AS TEXT)))) THEN RAISE EXCEPTION 'nextcloud_target_unverified'; END IF; RETURN NEW;
END $$;
CREATE TRIGGER nextcloud_target_closure_insert BEFORE INSERT ON nextcloud_onboarding_target_closures FOR EACH ROW EXECUTE FUNCTION nextcloud_target_closure_guard();
CREATE FUNCTION nextcloud_target_operation_guard() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
IF TG_OP='DELETE' THEN IF (EXISTS(SELECT 1 FROM nextcloud_onboarding_tenant_deletions td WHERE td.tenant_id=OLD.tenant_id) OR EXISTS(SELECT 1 FROM nextcloud_onboarding_target_closures tc WHERE tc.tenant_id=OLD.tenant_id AND tc.knowledge_base_id=OLD.knowledge_base_id)) THEN RAISE EXCEPTION 'nextcloud_target_closed'; END IF; RETURN OLD; END IF;
IF TG_OP='INSERT' THEN PERFORM 1 FROM tenants t WHERE t.id=NEW.tenant_id AND t.deleted_at IS NULL FOR UPDATE; IF NOT FOUND OR EXISTS(SELECT 1 FROM nextcloud_onboarding_tenant_deletions td WHERE td.tenant_id=NEW.tenant_id) OR (EXISTS(SELECT 1 FROM nextcloud_onboarding_tenant_deletions td WHERE td.tenant_id=NEW.tenant_id) OR EXISTS(SELECT 1 FROM nextcloud_onboarding_target_closures tc WHERE tc.tenant_id=NEW.tenant_id AND tc.knowledge_base_id=NEW.knowledge_base_id)) OR NOT EXISTS(SELECT 1 FROM knowledge_bases k WHERE k.id=NEW.knowledge_base_id AND k.tenant_id=NEW.tenant_id AND k.deleted_at IS NULL) THEN RAISE EXCEPTION 'nextcloud_target_closed'; END IF;
ELSIF (EXISTS(SELECT 1 FROM nextcloud_onboarding_target_closures tc WHERE tc.operation_id=OLD.operation_id) OR EXISTS(SELECT 1 FROM nextcloud_onboarding_tenant_deletions td WHERE td.tenant_id=OLD.tenant_id)) AND (NEW.state<>'retired' OR NEW.permission_synced_at IS DISTINCT FROM NULL OR NEW.operation_id IS DISTINCT FROM OLD.operation_id OR NEW.credential_id IS DISTINCT FROM OLD.credential_id OR NEW.tenant_id IS DISTINCT FROM OLD.tenant_id OR NEW.knowledge_base_id IS DISTINCT FROM OLD.knowledge_base_id OR NEW.instance_id IS DISTINCT FROM OLD.instance_id OR NEW.binding_id IS DISTINCT FROM OLD.binding_id OR NEW.actor_directory_id IS DISTINCT FROM OLD.actor_directory_id OR NEW.actor_object_guid IS DISTINCT FROM OLD.actor_object_guid OR NEW.actor_user_id IS DISTINCT FROM OLD.actor_user_id OR NEW.request_sha256 IS DISTINCT FROM OLD.request_sha256) THEN RAISE EXCEPTION 'nextcloud_target_closed'; END IF; RETURN NEW;
END $$;
CREATE TRIGGER aaa_nextcloud_target_operation BEFORE INSERT OR UPDATE OR DELETE ON nextcloud_onboarding_operations FOR EACH ROW EXECUTE FUNCTION nextcloud_target_operation_guard();
CREATE FUNCTION nextcloud_target_source_guard() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
IF TG_TABLE_NAME='nextcloud_source_pairings' THEN IF TG_OP='INSERT' OR NEW.state IN ('pending','active') THEN PERFORM nextcloud_target_require_live(NEW.tenant_id,NEW.knowledge_base_id); END IF; ELSIF TG_TABLE_NAME='nextcloud_event_connections' THEN IF TG_OP='INSERT' OR NEW.status='active' THEN PERFORM nextcloud_target_require_live(NEW.tenant_id,NEW.knowledge_base_id); END IF; ELSE PERFORM nextcloud_target_require_live(NEW.tenant_id,NEW.knowledge_base_id); END IF; IF (EXISTS(SELECT 1 FROM nextcloud_onboarding_tenant_deletions td WHERE td.tenant_id=NEW.tenant_id) OR EXISTS(SELECT 1 FROM nextcloud_onboarding_target_closures tc WHERE tc.tenant_id=NEW.tenant_id AND tc.knowledge_base_id=NEW.knowledge_base_id)) OR (TG_OP='UPDATE' AND (EXISTS(SELECT 1 FROM nextcloud_onboarding_tenant_deletions td WHERE td.tenant_id=OLD.tenant_id) OR EXISTS(SELECT 1 FROM nextcloud_onboarding_target_closures tc WHERE tc.tenant_id=OLD.tenant_id AND tc.knowledge_base_id=OLD.knowledge_base_id))) THEN
 IF TG_OP='INSERT' THEN RAISE EXCEPTION 'nextcloud_target_closed'; END IF;
 IF TG_TABLE_NAME='nextcloud_source_pairings' THEN IF NEW.state NOT IN ('retired','decommissioned','withdrawn') THEN RAISE EXCEPTION 'nextcloud_target_closed'; END IF;
 ELSIF TG_TABLE_NAME='nextcloud_event_connections' THEN IF NEW.status<>'revoked' THEN RAISE EXCEPTION 'nextcloud_target_closed'; END IF;
 ELSE RAISE EXCEPTION 'nextcloud_target_closed'; END IF; END IF; RETURN NEW;
END $$;
CREATE TRIGGER aaa_nextcloud_target_guard BEFORE INSERT OR UPDATE ON nextcloud_source_grants FOR EACH ROW EXECUTE FUNCTION nextcloud_target_source_guard();
CREATE TRIGGER aaa_nextcloud_target_guard BEFORE INSERT OR UPDATE ON nextcloud_source_pairings FOR EACH ROW EXECUTE FUNCTION nextcloud_target_source_guard();
CREATE TRIGGER aaa_nextcloud_target_guard BEFORE INSERT OR UPDATE ON nextcloud_event_connections FOR EACH ROW EXECUTE FUNCTION nextcloud_target_source_guard();
CREATE FUNCTION nextcloud_target_content_guard() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
IF TG_OP='UPDATE' THEN
 IF TG_TABLE_NAME='knowledges' THEN
  IF NEW.enable_status='disabled' AND (NEW.parse_status IS NOT DISTINCT FROM OLD.parse_status OR NEW.parse_status IN ('deleting','cancelled')) AND (to_jsonb(NEW)-'enable_status'-'parse_status'-'metadata'-'deleted_at'-'updated_at') IS NOT DISTINCT FROM (to_jsonb(OLD)-'enable_status'-'parse_status'-'metadata'-'deleted_at'-'updated_at') AND (COALESCE(NEW.metadata::jsonb,'{}')-'nextcloud_etag') IS NOT DISTINCT FROM (COALESCE(OLD.metadata::jsonb,'{}')-'nextcloud_etag') AND COALESCE(NEW.metadata::jsonb->>'nextcloud_etag','')='' AND NOT (OLD.deleted_at IS NOT NULL AND NEW.deleted_at IS NULL) THEN RETURN NEW; END IF;
 ELSIF TG_TABLE_NAME='chunks' THEN
  IF NEW.is_enabled=FALSE AND (to_jsonb(NEW)-'is_enabled'-'deleted_at'-'updated_at') IS NOT DISTINCT FROM (to_jsonb(OLD)-'is_enabled'-'deleted_at'-'updated_at') AND NOT (OLD.deleted_at IS NOT NULL AND NEW.deleted_at IS NULL) THEN RETURN NEW; END IF;
 END IF;
END IF; IF NOT (EXISTS(SELECT 1 FROM nextcloud_onboarding_tenant_deletions td WHERE td.tenant_id=NEW.tenant_id) OR EXISTS(SELECT 1 FROM nextcloud_onboarding_target_closures tc WHERE tc.tenant_id=NEW.tenant_id AND tc.knowledge_base_id=NEW.knowledge_base_id)) AND NOT (TG_OP='UPDATE' AND (EXISTS(SELECT 1 FROM nextcloud_onboarding_tenant_deletions td WHERE td.tenant_id=OLD.tenant_id) OR EXISTS(SELECT 1 FROM nextcloud_onboarding_target_closures tc WHERE tc.tenant_id=OLD.tenant_id AND tc.knowledge_base_id=OLD.knowledge_base_id))) THEN IF TG_TABLE_NAME='data_sources' THEN IF NEW.status<>'paused' OR NEW.sync_enabled IS DISTINCT FROM FALSE THEN PERFORM nextcloud_target_require_live(NEW.tenant_id,NEW.knowledge_base_id); END IF; ELSE PERFORM nextcloud_target_require_live(NEW.tenant_id,NEW.knowledge_base_id); END IF; RETURN NEW; END IF;
IF TG_OP='INSERT' THEN RAISE EXCEPTION 'nextcloud_target_closed'; END IF;
IF TG_TABLE_NAME='data_sources' THEN IF NEW.status<>'paused' OR NEW.sync_enabled IS DISTINCT FROM FALSE OR (to_jsonb(NEW)-'status'-'sync_enabled'-'deleted_at'-'updated_at') IS DISTINCT FROM (to_jsonb(OLD)-'status'-'sync_enabled'-'deleted_at'-'updated_at') OR OLD.deleted_at IS NOT NULL AND NEW.deleted_at IS NULL THEN RAISE EXCEPTION 'nextcloud_target_closed'; END IF;
ELSIF TG_TABLE_NAME='knowledges' THEN IF NEW.enable_status<>'disabled' OR (NEW.parse_status IS DISTINCT FROM OLD.parse_status AND NEW.parse_status NOT IN ('deleting','cancelled')) OR (to_jsonb(NEW)-'enable_status'-'parse_status'-'metadata'-'deleted_at'-'updated_at') IS DISTINCT FROM (to_jsonb(OLD)-'enable_status'-'parse_status'-'metadata'-'deleted_at'-'updated_at') OR (COALESCE(NEW.metadata::jsonb,'{}')-'nextcloud_etag') IS DISTINCT FROM (COALESCE(OLD.metadata::jsonb,'{}')-'nextcloud_etag') OR COALESCE(NEW.metadata::jsonb->>'nextcloud_etag','')<>'' OR OLD.deleted_at IS NOT NULL AND NEW.deleted_at IS NULL THEN RAISE EXCEPTION 'nextcloud_target_closed'; END IF;
ELSIF TG_TABLE_NAME='chunks' THEN IF NEW.is_enabled OR (to_jsonb(NEW)-'is_enabled'-'deleted_at'-'updated_at') IS DISTINCT FROM (to_jsonb(OLD)-'is_enabled'-'deleted_at'-'updated_at') OR OLD.deleted_at IS NOT NULL AND NEW.deleted_at IS NULL THEN RAISE EXCEPTION 'nextcloud_target_closed'; END IF;
ELSE RAISE EXCEPTION 'nextcloud_target_closed'; END IF; RETURN NEW;
END $$;
CREATE TRIGGER aaa_nextcloud_target_content BEFORE INSERT OR UPDATE ON knowledges FOR EACH ROW EXECUTE FUNCTION nextcloud_target_content_guard();
CREATE TRIGGER aaa_nextcloud_target_content BEFORE INSERT OR UPDATE ON chunks FOR EACH ROW EXECUTE FUNCTION nextcloud_target_content_guard();
CREATE TRIGGER aaa_nextcloud_target_content BEFORE INSERT OR UPDATE ON wiki_pages FOR EACH ROW EXECUTE FUNCTION nextcloud_target_content_guard();
CREATE TRIGGER aaa_nextcloud_target_content BEFORE INSERT OR UPDATE ON data_sources FOR EACH ROW EXECUTE FUNCTION nextcloud_target_content_guard();
CREATE FUNCTION nextcloud_target_retained_delete_guard() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
IF (EXISTS(SELECT 1 FROM nextcloud_onboarding_tenant_deletions td WHERE td.tenant_id=OLD.tenant_id) OR EXISTS(SELECT 1 FROM nextcloud_onboarding_target_closures tc WHERE tc.tenant_id=OLD.tenant_id AND tc.knowledge_base_id=OLD.knowledge_base_id)) THEN RAISE EXCEPTION 'nextcloud_target_closed'; END IF; RETURN OLD;
END $$;
CREATE TRIGGER aaa_nextcloud_target_retained_delete BEFORE DELETE ON nextcloud_source_pairings FOR EACH ROW EXECUTE FUNCTION nextcloud_target_retained_delete_guard();
CREATE TRIGGER aaa_nextcloud_target_retained_delete BEFORE DELETE ON nextcloud_event_connections FOR EACH ROW EXECUTE FUNCTION nextcloud_target_retained_delete_guard();
CREATE TRIGGER aaa_nextcloud_target_retained_delete BEFORE DELETE ON data_sources FOR EACH ROW EXECUTE FUNCTION nextcloud_target_retained_delete_guard();
CREATE FUNCTION nextcloud_target_fence_guard() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
IF NEW.state='open' AND (TG_OP='UPDATE' OR NOT EXISTS(SELECT 1 FROM nextcloud_content_fences f WHERE f.tenant_id=NEW.tenant_id AND f.knowledge_base_id=NEW.knowledge_base_id AND f.knowledge_id=NEW.knowledge_id)) THEN PERFORM nextcloud_target_require_live(NEW.tenant_id,NEW.knowledge_base_id); END IF; IF (EXISTS(SELECT 1 FROM nextcloud_onboarding_tenant_deletions td WHERE td.tenant_id=NEW.tenant_id) OR EXISTS(SELECT 1 FROM nextcloud_onboarding_target_closures tc WHERE tc.tenant_id=NEW.tenant_id AND tc.knowledge_base_id=NEW.knowledge_base_id)) AND NEW.state='open' AND (TG_OP='UPDATE' OR NOT EXISTS(SELECT 1 FROM nextcloud_content_fences f WHERE f.tenant_id=NEW.tenant_id AND f.knowledge_base_id=NEW.knowledge_base_id AND f.knowledge_id=NEW.knowledge_id)) THEN RAISE EXCEPTION 'nextcloud_target_closed'; END IF; RETURN NEW;
END $$;
CREATE TRIGGER aaa_nextcloud_target_fence BEFORE INSERT OR UPDATE ON nextcloud_content_fences FOR EACH ROW EXECUTE FUNCTION nextcloud_target_fence_guard();
CREATE FUNCTION nextcloud_target_lease_guard() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
PERFORM nextcloud_target_require_live(NEW.tenant_id,NEW.knowledge_base_id); IF (EXISTS(SELECT 1 FROM nextcloud_onboarding_tenant_deletions td WHERE td.tenant_id=NEW.tenant_id) OR EXISTS(SELECT 1 FROM nextcloud_onboarding_target_closures tc WHERE tc.tenant_id=NEW.tenant_id AND tc.knowledge_base_id=NEW.knowledge_base_id)) THEN RAISE EXCEPTION 'nextcloud_target_closed'; END IF; RETURN NEW;
END $$;
CREATE TRIGGER aaa_nextcloud_target_lease BEFORE INSERT ON nextcloud_content_leases FOR EACH ROW EXECUTE FUNCTION nextcloud_target_lease_guard();
CREATE FUNCTION nextcloud_target_owner_guard() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
IF TG_TABLE_NAME='knowledge_bases' THEN IF (EXISTS(SELECT 1 FROM nextcloud_onboarding_tenant_deletions td WHERE td.tenant_id=NEW.tenant_id) OR EXISTS(SELECT 1 FROM nextcloud_onboarding_target_closures tc WHERE tc.tenant_id=NEW.tenant_id AND tc.knowledge_base_id=NEW.id)) AND (TG_OP='INSERT' OR OLD.deleted_at IS NOT NULL AND NEW.deleted_at IS NULL) THEN RAISE EXCEPTION 'nextcloud_target_closed'; END IF; ELSE IF TG_OP='INSERT' THEN IF EXISTS(SELECT 1 FROM nextcloud_onboarding_tenant_deletions td WHERE td.tenant_id=NEW.id) THEN RAISE EXCEPTION 'nextcloud_target_closed'; END IF; RETURN NEW; END IF; IF OLD.deleted_at IS NOT NULL AND NEW.deleted_at IS NULL AND EXISTS(SELECT 1 FROM nextcloud_onboarding_tenant_deletions td WHERE td.tenant_id=OLD.id) THEN RAISE EXCEPTION 'nextcloud_target_closed'; END IF; END IF; RETURN NEW;
END $$;
CREATE TRIGGER aaa_nextcloud_target_kb BEFORE INSERT OR UPDATE OF deleted_at ON knowledge_bases FOR EACH ROW EXECUTE FUNCTION nextcloud_target_owner_guard();
CREATE TRIGGER aaa_nextcloud_target_tenant BEFORE INSERT OR UPDATE OF deleted_at ON tenants FOR EACH ROW EXECUTE FUNCTION nextcloud_target_owner_guard();
