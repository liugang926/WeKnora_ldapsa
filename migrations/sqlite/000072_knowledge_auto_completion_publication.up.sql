-- Exact Auto completion/publication revisions. Existing rows receive counters, never transition receipts.
CREATE TABLE knowledge_auto_publication_versions (
 knowledge_id TEXT PRIMARY KEY,
 revision BIGINT NOT NULL CHECK (revision > 0)
);
CREATE TABLE knowledge_auto_publication_transitions (
 transition_id TEXT PRIMARY KEY,
 snapshot_id TEXT NOT NULL,
 tenant_id BIGINT NOT NULL,
 knowledge_base_id TEXT NOT NULL,
 knowledge_id TEXT NOT NULL,
 admission_id TEXT NOT NULL,
 before_revision BIGINT NOT NULL CHECK (before_revision > 0),
 after_revision BIGINT NOT NULL CHECK (after_revision > before_revision),
 before_digest TEXT NOT NULL,
 after_digest TEXT NOT NULL
);
CREATE UNIQUE INDEX knowledge_auto_publication_transition_step ON knowledge_auto_publication_transitions(snapshot_id,before_revision);
CREATE INDEX knowledge_auto_publication_transition_knowledge ON knowledge_auto_publication_transitions(tenant_id,knowledge_base_id,knowledge_id);
INSERT INTO knowledge_auto_publication_versions(knowledge_id,revision) SELECT id,1 FROM knowledges WHERE channel='nextcloud';
CREATE TRIGGER knowledge_auto_publication_insert AFTER INSERT ON knowledges WHEN NEW.channel='nextcloud' BEGIN
 INSERT INTO knowledge_auto_publication_versions(knowledge_id,revision) VALUES(NEW.id,1) ON CONFLICT(knowledge_id) DO UPDATE SET revision=revision+1;
END;
CREATE TRIGGER knowledge_auto_publication_update AFTER UPDATE ON knowledges WHEN (OLD.channel='nextcloud' OR NEW.channel='nextcloud') AND (OLD.metadata IS NOT NEW.metadata OR OLD.updated_at IS NOT NEW.updated_at OR OLD.parse_status IS NOT NEW.parse_status OR OLD.enable_status IS NOT NEW.enable_status OR OLD.summary_status IS NOT NEW.summary_status OR OLD.processed_at IS NOT NEW.processed_at OR OLD.pending_subtasks_count IS NOT NEW.pending_subtasks_count) BEGIN
 INSERT INTO knowledge_auto_publication_versions(knowledge_id,revision) VALUES(NEW.id,1) ON CONFLICT(knowledge_id) DO UPDATE SET revision=revision+1;
END;
CREATE TRIGGER knowledge_auto_publication_delete AFTER DELETE ON knowledges WHEN OLD.channel='nextcloud' BEGIN
 INSERT INTO knowledge_auto_publication_versions(knowledge_id,revision) VALUES(OLD.id,1) ON CONFLICT(knowledge_id) DO UPDATE SET revision=revision+1;
END;
CREATE TRIGGER knowledge_auto_publication_transition_no_update BEFORE UPDATE ON knowledge_auto_publication_transitions BEGIN SELECT RAISE(ABORT,'immutable auto publication transition'); END;
CREATE TRIGGER knowledge_auto_publication_transition_no_delete BEFORE DELETE ON knowledge_auto_publication_transitions BEGIN SELECT RAISE(ABORT,'immutable auto publication transition'); END;
