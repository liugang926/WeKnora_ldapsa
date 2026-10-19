CREATE TABLE knowledge_span_start_manifests (
 run_token TEXT PRIMARY KEY, knowledge_id TEXT NOT NULL REFERENCES knowledges(id) ON DELETE CASCADE,
 attempt INTEGER NOT NULL CHECK(attempt > 0), span_id TEXT NOT NULL,
 parent_span_id TEXT NOT NULL, name TEXT NOT NULL, kind TEXT NOT NULL,
 original_input TEXT NOT NULL, actual_inputs TEXT NOT NULL, inputs_digest TEXT NOT NULL
);
CREATE INDEX knowledge_span_start_manifest_scope ON knowledge_span_start_manifests(knowledge_id, attempt, span_id);
CREATE TABLE knowledge_span_row_versions (span_row_id INTEGER PRIMARY KEY, revision INTEGER NOT NULL CHECK(revision > 0));
INSERT INTO knowledge_span_row_versions SELECT id,1 FROM knowledge_processing_spans;
CREATE TABLE knowledge_span_body_receipts (
 knowledge_id TEXT NOT NULL, attempt INTEGER NOT NULL, span_id TEXT NOT NULL,
 run_token TEXT NOT NULL REFERENCES knowledge_span_start_manifests(run_token) ON DELETE CASCADE, row_revision INTEGER NOT NULL CHECK(row_revision > 0), body_digest TEXT NOT NULL,
 PRIMARY KEY(knowledge_id,attempt,span_id),
 FOREIGN KEY(knowledge_id,attempt,span_id) REFERENCES knowledge_processing_spans(knowledge_id,attempt,span_id) ON DELETE CASCADE
);
CREATE TRIGGER knowledge_span_manifest_immutable BEFORE UPDATE ON knowledge_span_start_manifests
BEGIN SELECT RAISE(ABORT,'original span manifest is immutable'); END;
CREATE TRIGGER knowledge_span_version_insert AFTER INSERT ON knowledge_processing_spans
BEGIN INSERT INTO knowledge_span_row_versions(span_row_id,revision) VALUES(NEW.id,1); END;
CREATE TRIGGER knowledge_span_version_update AFTER UPDATE ON knowledge_processing_spans
BEGIN
 UPDATE knowledge_span_row_versions SET revision=revision+1 WHERE span_row_id=OLD.id;
 DELETE FROM knowledge_span_body_receipts WHERE knowledge_id=OLD.knowledge_id AND attempt=OLD.attempt AND span_id=OLD.span_id;
END;
CREATE TRIGGER knowledge_span_version_delete AFTER DELETE ON knowledge_processing_spans
BEGIN DELETE FROM knowledge_span_row_versions WHERE span_row_id=OLD.id; END;
CREATE TRIGGER knowledge_span_knowledge_delete AFTER DELETE ON knowledges
BEGIN
 DELETE FROM knowledge_span_body_receipts WHERE knowledge_id=OLD.id;
 DELETE FROM knowledge_processing_spans WHERE knowledge_id=OLD.id;
 DELETE FROM knowledge_span_start_manifests WHERE knowledge_id=OLD.id;
END;
