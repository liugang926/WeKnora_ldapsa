CREATE TABLE nextcloud_control_recovery_gates (
 instance_id TEXT PRIMARY KEY,plan_sha256 TEXT NOT NULL,inventory_sha256 TEXT NOT NULL,
 closed BOOLEAN NOT NULL DEFAULT TRUE,closed_at TIMESTAMPTZ NOT NULL);
CREATE FUNCTION nextcloud_directory_history_guard() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF TG_OP='DELETE' THEN RAISE EXCEPTION 'nextcloud_directory_history_immutable'; END IF;
 IF TG_TABLE_NAME='nextcloud_root_migrations' THEN
  IF (to_jsonb(NEW)-'state'-'updated_at') IS DISTINCT FROM (to_jsonb(OLD)-'state'-'updated_at') OR (OLD.state='finalized' AND NEW.state<>'finalized') THEN RAISE EXCEPTION 'nextcloud_root_proof_immutable'; END IF;
 ELSE
  IF (to_jsonb(NEW)-'state'-'updated_at'-'manifest_sha256') IS DISTINCT FROM (to_jsonb(OLD)-'state'-'updated_at'-'manifest_sha256') OR (OLD.state='active' AND (NEW.state<>'active' OR NEW.manifest_sha256 IS DISTINCT FROM OLD.manifest_sha256)) THEN RAISE EXCEPTION 'nextcloud_target_history_immutable'; END IF;
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER nextcloud_root_proof_immutable BEFORE UPDATE OR DELETE ON nextcloud_root_migrations FOR EACH ROW EXECUTE FUNCTION nextcloud_directory_history_guard();
CREATE TRIGGER nextcloud_target_history_immutable BEFORE UPDATE OR DELETE ON nextcloud_target_migrations FOR EACH ROW EXECUTE FUNCTION nextcloud_directory_history_guard();
CREATE FUNCTION nextcloud_target_old_source_guard() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF NEW.status='active' AND EXISTS(SELECT 1 FROM nextcloud_target_migrations t WHERE t.old_tenant_id=NEW.tenant_id AND t.old_data_source_id=NEW.id) THEN RAISE EXCEPTION 'nextcloud_target_old_source_closed'; END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER nextcloud_target_old_source_closed BEFORE UPDATE ON data_sources FOR EACH ROW EXECUTE FUNCTION nextcloud_target_old_source_guard();
