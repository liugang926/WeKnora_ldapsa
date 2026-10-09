CREATE TABLE nextcloud_source_versions (
    tenant_id BIGINT NOT NULL,
    knowledge_base_id TEXT NOT NULL,
    datasource_id TEXT NOT NULL,
    external_id TEXT NOT NULL,
    desired_etag TEXT NOT NULL DEFAULT '',
    candidate_knowledge_id TEXT NOT NULL DEFAULT '',
    state TEXT NOT NULL CHECK (state IN ('staging', 'published', 'tombstone')),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (tenant_id, knowledge_base_id, datasource_id, external_id)
);

CREATE UNIQUE INDEX idx_nextcloud_source_versions_candidate
    ON nextcloud_source_versions (candidate_knowledge_id)
    WHERE candidate_knowledge_id <> '';
