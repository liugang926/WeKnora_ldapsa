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
CREATE FUNCTION knowledge_auto_publication_version_bump() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF TG_OP='INSERT' AND NEW.channel<>'nextcloud' THEN RETURN NEW; END IF;
 IF TG_OP='DELETE' AND OLD.channel<>'nextcloud' THEN RETURN OLD; END IF;
 IF TG_OP='UPDATE' THEN
  IF OLD.channel<>'nextcloud' AND NEW.channel<>'nextcloud' THEN RETURN NEW; END IF;
  IF NOT (OLD.metadata::jsonb IS DISTINCT FROM NEW.metadata::jsonb OR OLD.updated_at IS DISTINCT FROM NEW.updated_at OR OLD.parse_status IS DISTINCT FROM NEW.parse_status OR OLD.enable_status IS DISTINCT FROM NEW.enable_status OR OLD.summary_status IS DISTINCT FROM NEW.summary_status OR OLD.processed_at IS DISTINCT FROM NEW.processed_at OR OLD.pending_subtasks_count IS DISTINCT FROM NEW.pending_subtasks_count) THEN RETURN NEW; END IF;
 END IF;
 INSERT INTO knowledge_auto_publication_versions(knowledge_id,revision) VALUES(CASE WHEN TG_OP='DELETE' THEN OLD.id ELSE NEW.id END,1) ON CONFLICT(knowledge_id) DO UPDATE SET revision=knowledge_auto_publication_versions.revision+1;
 IF TG_OP='DELETE' THEN RETURN OLD; ELSE RETURN NEW; END IF;
END; $$;
CREATE TRIGGER knowledge_auto_publication_version AFTER INSERT OR UPDATE OR DELETE ON knowledges FOR EACH ROW EXECUTE FUNCTION knowledge_auto_publication_version_bump();
CREATE FUNCTION knowledge_auto_publication_transition_immutable() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'immutable auto publication transition'; END; $$;
CREATE TRIGGER knowledge_auto_publication_transition_immutable BEFORE UPDATE OR DELETE ON knowledge_auto_publication_transitions FOR EACH ROW EXECUTE FUNCTION knowledge_auto_publication_transition_immutable();
