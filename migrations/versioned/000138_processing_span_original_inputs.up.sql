CREATE TABLE knowledge_span_start_manifests (
 run_token TEXT PRIMARY KEY, knowledge_id VARCHAR(64) NOT NULL REFERENCES knowledges(id) ON DELETE CASCADE,
 attempt INTEGER NOT NULL CHECK(attempt > 0), span_id VARCHAR(64) NOT NULL,
 parent_span_id VARCHAR(64) NOT NULL, name TEXT NOT NULL, kind VARCHAR(16) NOT NULL,
 original_input TEXT NOT NULL, actual_inputs TEXT NOT NULL, inputs_digest TEXT NOT NULL
);
CREATE INDEX knowledge_span_start_manifest_scope ON knowledge_span_start_manifests(knowledge_id,attempt,span_id);
CREATE TABLE knowledge_span_row_versions (span_row_id BIGINT PRIMARY KEY, revision BIGINT NOT NULL CHECK(revision > 0));
INSERT INTO knowledge_span_row_versions SELECT id,1 FROM knowledge_processing_spans;
CREATE TABLE knowledge_span_body_receipts (
 knowledge_id VARCHAR(64) NOT NULL, attempt INTEGER NOT NULL, span_id VARCHAR(64) NOT NULL,
 run_token TEXT NOT NULL REFERENCES knowledge_span_start_manifests(run_token) ON DELETE CASCADE, row_revision BIGINT NOT NULL CHECK(row_revision > 0), body_digest TEXT NOT NULL,
 PRIMARY KEY(knowledge_id,attempt,span_id),
 FOREIGN KEY(knowledge_id,attempt,span_id) REFERENCES knowledge_processing_spans(knowledge_id,attempt,span_id) ON DELETE CASCADE
);
CREATE FUNCTION knowledge_span_manifest_immutable() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN RAISE EXCEPTION 'original span manifest is immutable'; END; $$;
CREATE TRIGGER knowledge_span_manifest_immutable BEFORE UPDATE ON knowledge_span_start_manifests FOR EACH ROW EXECUTE FUNCTION knowledge_span_manifest_immutable();
CREATE FUNCTION knowledge_span_version_change() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF TG_OP='INSERT' THEN
  INSERT INTO knowledge_span_row_versions(span_row_id,revision) VALUES(NEW.id,1);
  RETURN NEW;
 ELSIF TG_OP='UPDATE' THEN
  UPDATE knowledge_span_row_versions SET revision=revision+1 WHERE span_row_id=OLD.id;
  DELETE FROM knowledge_span_body_receipts WHERE knowledge_id=OLD.knowledge_id AND attempt=OLD.attempt AND span_id=OLD.span_id;
  RETURN NEW;
 ELSE
  DELETE FROM knowledge_span_row_versions WHERE span_row_id=OLD.id;
  RETURN OLD;
 END IF;
END; $$;
CREATE TRIGGER knowledge_span_version_change AFTER INSERT OR UPDATE OR DELETE ON knowledge_processing_spans FOR EACH ROW EXECUTE FUNCTION knowledge_span_version_change();
CREATE FUNCTION knowledge_span_knowledge_delete() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 DELETE FROM knowledge_span_body_receipts WHERE knowledge_id=OLD.id;
 DELETE FROM knowledge_processing_spans WHERE knowledge_id=OLD.id;
 DELETE FROM knowledge_span_start_manifests WHERE knowledge_id=OLD.id;
 RETURN OLD;
END; $$;
CREATE TRIGGER knowledge_span_knowledge_delete AFTER DELETE ON knowledges FOR EACH ROW EXECUTE FUNCTION knowledge_span_knowledge_delete();
