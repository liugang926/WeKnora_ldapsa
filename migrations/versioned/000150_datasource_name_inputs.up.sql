-- Every data-source name is usable only through its actual new input receipt.
CREATE TABLE datasource_name_versions (datasource_id TEXT PRIMARY KEY, revision BIGINT NOT NULL CHECK(revision > 0));
CREATE TABLE datasource_name_inputs (id TEXT PRIMARY KEY, datasource_id TEXT NOT NULL, tenant_id BIGINT NOT NULL, knowledge_base_id TEXT NOT NULL, revision BIGINT NOT NULL CHECK(revision > 0), body_digest TEXT NOT NULL CHECK(length(body_digest)=64), origin_kind TEXT NOT NULL CHECK(origin_kind IN ('human','pairing_configuration')), actual_inputs TEXT NOT NULL, input_digest TEXT NOT NULL CHECK(length(input_digest)=64));
CREATE INDEX datasource_name_inputs_current ON datasource_name_inputs(datasource_id,revision);
INSERT INTO datasource_name_versions(datasource_id,revision) SELECT id,1 FROM data_sources;
CREATE TRIGGER datasource_name_input_immutable BEFORE UPDATE OR DELETE ON datasource_name_inputs FOR EACH ROW EXECUTE FUNCTION knowledge_producer_input_immutable();
CREATE FUNCTION datasource_name_input_version_bump() RETURNS trigger LANGUAGE plpgsql AS $$ DECLARE rowid text; BEGIN
 IF TG_OP='UPDATE' AND ROW(OLD.tenant_id,OLD.knowledge_base_id,OLD.name,OLD.type,OLD.deleted_at) IS NOT DISTINCT FROM ROW(NEW.tenant_id,NEW.knowledge_base_id,NEW.name,NEW.type,NEW.deleted_at) THEN RETURN NEW; END IF;
 IF TG_OP='DELETE' THEN rowid:=OLD.id; ELSE rowid:=NEW.id; END IF;
 INSERT INTO datasource_name_versions(datasource_id,revision) VALUES(rowid,1) ON CONFLICT(datasource_id) DO UPDATE SET revision=datasource_name_versions.revision+1;
 IF TG_OP='DELETE' THEN RETURN OLD; ELSE RETURN NEW; END IF;
END $$;
CREATE TRIGGER datasource_name_input_version AFTER INSERT OR UPDATE OR DELETE ON data_sources FOR EACH ROW EXECUTE FUNCTION datasource_name_input_version_bump();

CREATE TABLE nextcloud_upload_source_versions(datasource_id TEXT PRIMARY KEY, revision BIGINT NOT NULL CHECK(revision > 0));
INSERT INTO nextcloud_upload_source_versions(datasource_id,revision) SELECT id,1 FROM data_sources WHERE type='nextcloud';
CREATE TABLE nextcloud_original_upload_inputs(knowledge_id TEXT PRIMARY KEY, tenant_id BIGINT NOT NULL, knowledge_base_id TEXT NOT NULL, datasource_id TEXT NOT NULL, source_revision BIGINT NOT NULL CHECK(source_revision > 0), config_revision BIGINT NOT NULL CHECK(config_revision > 0), config_digest TEXT NOT NULL CHECK(length(config_digest)=64), root_digest TEXT NOT NULL CHECK(length(root_digest)=64), root_revision BIGINT NOT NULL CHECK(root_revision > 0), routing_revision BIGINT NOT NULL CHECK(routing_revision > 0), routing_digest TEXT NOT NULL CHECK(length(routing_digest)=64), tenant_revision BIGINT NOT NULL CHECK(tenant_revision > 0), tenant_digest TEXT NOT NULL CHECK(length(tenant_digest)=64), artifact_digest TEXT NOT NULL CHECK(length(artifact_digest)=64), actual_inputs TEXT NOT NULL, input_digest TEXT NOT NULL CHECK(length(input_digest)=64));
CREATE TRIGGER nextcloud_original_upload_immutable BEFORE UPDATE OR DELETE ON nextcloud_original_upload_inputs FOR EACH ROW EXECUTE FUNCTION knowledge_producer_input_immutable();
CREATE FUNCTION nextcloud_upload_ds_version_bump() RETURNS trigger LANGUAGE plpgsql AS $$ DECLARE rowid text; BEGIN
 IF TG_OP='UPDATE' AND ROW(OLD.tenant_id,OLD.knowledge_base_id,OLD.type,OLD.config,OLD.status,OLD.deleted_at) IS NOT DISTINCT FROM ROW(NEW.tenant_id,NEW.knowledge_base_id,NEW.type,NEW.config,NEW.status,NEW.deleted_at) THEN RETURN NEW; END IF;
 IF TG_OP='DELETE' THEN rowid:=OLD.id; IF OLD.type<>'nextcloud' THEN RETURN OLD; END IF; ELSE rowid:=NEW.id; IF NEW.type<>'nextcloud' AND (TG_OP='INSERT' OR OLD.type<>'nextcloud') THEN RETURN NEW; END IF; END IF;
 INSERT INTO nextcloud_upload_source_versions(datasource_id,revision) VALUES(rowid,1) ON CONFLICT(datasource_id) DO UPDATE SET revision=nextcloud_upload_source_versions.revision+1;
 IF TG_OP='DELETE' THEN RETURN OLD; ELSE RETURN NEW; END IF;
END $$;
CREATE TRIGGER nextcloud_upload_ds_version AFTER INSERT OR UPDATE OR DELETE ON data_sources FOR EACH ROW EXECUTE FUNCTION nextcloud_upload_ds_version_bump();
CREATE FUNCTION nextcloud_upload_pair_version_bump() RETURNS trigger LANGUAGE plpgsql AS $$ DECLARE rowid text; BEGIN
 IF TG_OP='UPDATE' AND ROW(OLD.tenant_id,OLD.knowledge_base_id,OLD.datasource_id,OLD.nextcloud_instance_id,OLD.binding_id,OLD.datasource_base_url,OLD.datasource_config_sha256,OLD.publication_epoch,OLD.state) IS NOT DISTINCT FROM ROW(NEW.tenant_id,NEW.knowledge_base_id,NEW.datasource_id,NEW.nextcloud_instance_id,NEW.binding_id,NEW.datasource_base_url,NEW.datasource_config_sha256,NEW.publication_epoch,NEW.state) THEN RETURN NEW; END IF;
 IF TG_OP='DELETE' THEN rowid:=OLD.datasource_id; ELSE rowid:=NEW.datasource_id; END IF;
 INSERT INTO nextcloud_upload_source_versions(datasource_id,revision) VALUES(rowid,1) ON CONFLICT(datasource_id) DO UPDATE SET revision=nextcloud_upload_source_versions.revision+1;
 IF TG_OP='UPDATE' AND OLD.datasource_id IS DISTINCT FROM NEW.datasource_id THEN INSERT INTO nextcloud_upload_source_versions(datasource_id,revision) VALUES(OLD.datasource_id,1) ON CONFLICT(datasource_id) DO UPDATE SET revision=nextcloud_upload_source_versions.revision+1; END IF;
 IF TG_OP='DELETE' THEN RETURN OLD; ELSE RETURN NEW; END IF;
END $$;
CREATE TRIGGER nextcloud_upload_pair_version AFTER INSERT OR UPDATE OR DELETE ON nextcloud_source_pairings FOR EACH ROW EXECUTE FUNCTION nextcloud_upload_pair_version_bump();

-- Exact private transition of one question producer's own metadata output.
CREATE TABLE knowledge_question_metadata_outputs(chunk_id TEXT NOT NULL,admission_id TEXT NOT NULL,producer_input_id TEXT NOT NULL,previous_revision BIGINT NOT NULL,previous_metadata_digest TEXT NOT NULL,previous_body_digest TEXT NOT NULL,result_revision BIGINT NOT NULL,result_metadata_digest TEXT NOT NULL,result_body_digest TEXT NOT NULL,PRIMARY KEY(chunk_id,result_revision));
CREATE TRIGGER knowledge_question_metadata_output_immutable BEFORE UPDATE OR DELETE ON knowledge_question_metadata_outputs FOR EACH ROW EXECUTE FUNCTION knowledge_producer_input_immutable();
