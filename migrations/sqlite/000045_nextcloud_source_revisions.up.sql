-- Local source mutation observations only; legacy rows have unknown history.
CREATE TABLE nextcloud_source_revisions (
    tenant_id INTEGER NOT NULL,
    knowledge_base_id TEXT NOT NULL,
    datasource_id TEXT NOT NULL,
    external_id TEXT NOT NULL,
    revision INTEGER NOT NULL CHECK (revision > 0),
    desired_etag TEXT NOT NULL,
    candidate_knowledge_id TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('staging', 'published', 'tombstone')),
    source_updated_at DATETIME NOT NULL,
    recorded_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    legacy_history_unknown INTEGER NOT NULL CHECK (legacy_history_unknown IN (0, 1)),
    PRIMARY KEY (tenant_id, knowledge_base_id, datasource_id, external_id, revision)
);

INSERT INTO nextcloud_source_revisions (
    tenant_id, knowledge_base_id, datasource_id, external_id, revision,
    desired_etag, candidate_knowledge_id, state, source_updated_at,
    legacy_history_unknown
)
SELECT tenant_id, knowledge_base_id, datasource_id, external_id, 1,
       desired_etag, candidate_knowledge_id, state, updated_at, 1
FROM nextcloud_source_versions;

CREATE TRIGGER nextcloud_source_revision_immutable_update
BEFORE UPDATE ON nextcloud_source_revisions
BEGIN SELECT RAISE(ABORT, 'nextcloud_source_revision_immutable'); END;
CREATE TRIGGER nextcloud_source_revision_immutable_delete
BEFORE DELETE ON nextcloud_source_revisions
BEGIN SELECT RAISE(ABORT, 'nextcloud_source_revision_immutable'); END;

CREATE TRIGGER nextcloud_source_version_identity_update
BEFORE UPDATE ON nextcloud_source_versions
WHEN OLD.tenant_id IS NOT NEW.tenant_id
  OR OLD.knowledge_base_id IS NOT NEW.knowledge_base_id
  OR OLD.datasource_id IS NOT NEW.datasource_id
  OR OLD.external_id IS NOT NEW.external_id
BEGIN SELECT RAISE(ABORT, 'nextcloud_source_version_identity_immutable'); END;
CREATE TRIGGER nextcloud_source_version_identity_delete
BEFORE DELETE ON nextcloud_source_versions
BEGIN SELECT RAISE(ABORT, 'nextcloud_source_version_identity_immutable'); END;
CREATE TRIGGER nextcloud_source_version_history_missing
BEFORE UPDATE ON nextcloud_source_versions
WHEN NOT EXISTS (
    SELECT 1 FROM nextcloud_source_revisions AS r
    WHERE r.tenant_id = OLD.tenant_id AND r.knowledge_base_id = OLD.knowledge_base_id
      AND r.datasource_id = OLD.datasource_id AND r.external_id = OLD.external_id
)
BEGIN SELECT RAISE(ABORT, 'nextcloud_source_revision_history_missing'); END;

-- SQLite serializes writers, including conflicting UPSERTs. AFTER triggers
-- observe the actual winning INSERT or UPDATE in the same transaction.
CREATE TRIGGER nextcloud_source_revision_append_insert
AFTER INSERT ON nextcloud_source_versions
BEGIN
    SELECT CASE WHEN EXISTS (
        SELECT 1 FROM nextcloud_source_revisions AS r
        WHERE r.tenant_id = NEW.tenant_id AND r.knowledge_base_id = NEW.knowledge_base_id
          AND r.datasource_id = NEW.datasource_id AND r.external_id = NEW.external_id
    ) THEN RAISE(ABORT, 'nextcloud_source_revision_reinsert') END;
    INSERT INTO nextcloud_source_revisions (
        tenant_id, knowledge_base_id, datasource_id, external_id, revision,
        desired_etag, candidate_knowledge_id, state, source_updated_at,
        legacy_history_unknown
    ) VALUES (NEW.tenant_id, NEW.knowledge_base_id, NEW.datasource_id, NEW.external_id,
        1, NEW.desired_etag, NEW.candidate_knowledge_id, NEW.state, NEW.updated_at, 0);
END;
CREATE TRIGGER nextcloud_source_revision_append_update
AFTER UPDATE ON nextcloud_source_versions
BEGIN
    INSERT INTO nextcloud_source_revisions (
        tenant_id, knowledge_base_id, datasource_id, external_id, revision,
        desired_etag, candidate_knowledge_id, state, source_updated_at,
        legacy_history_unknown
    ) SELECT NEW.tenant_id, NEW.knowledge_base_id, NEW.datasource_id, NEW.external_id,
        r.revision + 1, NEW.desired_etag, NEW.candidate_knowledge_id, NEW.state,
        NEW.updated_at, r.legacy_history_unknown
    FROM nextcloud_source_revisions AS r
    WHERE r.tenant_id = NEW.tenant_id AND r.knowledge_base_id = NEW.knowledge_base_id
      AND r.datasource_id = NEW.datasource_id AND r.external_id = NEW.external_id
    ORDER BY r.revision DESC LIMIT 1;
END;
