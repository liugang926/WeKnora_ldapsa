-- A durable, monotone fence for one Nextcloud knowledge generation. The
-- knowledge_id='' row is the containing KB fence, acquired before exact rows.
-- The tables are deliberately inert until every reader and builder is wired.
CREATE TABLE nextcloud_content_fences (
    tenant_id BIGINT NOT NULL CHECK (tenant_id > 0),
    knowledge_base_id TEXT NOT NULL CHECK (knowledge_base_id <> ''),
    knowledge_id TEXT NOT NULL,
    datasource_id TEXT NOT NULL DEFAULT '',
    external_id TEXT NOT NULL DEFAULT '',
    epoch BIGINT NOT NULL DEFAULT 1 CHECK (epoch > 0),
    state TEXT NOT NULL CHECK (state IN ('open', 'retired', 'deleting', 'deleted')),
    retired_at_ms BIGINT,
    deleted_at_ms BIGINT,
    claim_token TEXT NOT NULL DEFAULT '',
    claim_until_ms BIGINT,
    created_at_ms BIGINT NOT NULL,
    updated_at_ms BIGINT NOT NULL,
    PRIMARY KEY (tenant_id, knowledge_base_id, knowledge_id),
    CHECK ((knowledge_id = '' AND datasource_id = '' AND external_id = '') OR
           (knowledge_id <> '' AND datasource_id <> '' AND external_id <> ''))
);
CREATE INDEX idx_nextcloud_content_deleting
    ON nextcloud_content_fences (tenant_id, knowledge_base_id, claim_until_ms)
    WHERE state = 'deleting' AND knowledge_id <> '';

CREATE TABLE nextcloud_content_leases (
    lease_id TEXT PRIMARY KEY,
    tenant_id BIGINT NOT NULL,
    knowledge_base_id TEXT NOT NULL,
    knowledge_id TEXT NOT NULL,
    kind TEXT NOT NULL CHECK (kind IN ('read', 'build')),
    epoch BIGINT NOT NULL CHECK (epoch > 0),
    owner_id TEXT NOT NULL CHECK (owner_id <> ''),
    expires_at_ms BIGINT NOT NULL,
    released_at_ms BIGINT,
    created_at_ms BIGINT NOT NULL,
    updated_at_ms BIGINT NOT NULL,
    FOREIGN KEY (tenant_id, knowledge_base_id, knowledge_id)
        REFERENCES nextcloud_content_fences (tenant_id, knowledge_base_id, knowledge_id)
        ON DELETE RESTRICT,
    CHECK (knowledge_id <> '' OR kind = 'read')
);
CREATE INDEX idx_nextcloud_content_leases_live
    ON nextcloud_content_leases (tenant_id, knowledge_base_id, knowledge_id,
        kind, expires_at_ms) WHERE released_at_ms IS NULL;
CREATE INDEX idx_nextcloud_content_leases_prune_released
    ON nextcloud_content_leases (released_at_ms, lease_id)
    WHERE released_at_ms IS NOT NULL;
CREATE INDEX idx_nextcloud_content_leases_prune_expired
    ON nextcloud_content_leases (expires_at_ms, lease_id)
    WHERE released_at_ms IS NULL;

-- An absent row blocks GC. It must only be inserted by a future rollout after
-- all read/build paths are covered and pre-rollout workers/streams are drained.
CREATE TABLE nextcloud_content_lease_coverage (
    tenant_id BIGINT NOT NULL,
    knowledge_base_id TEXT NOT NULL,
    activated_at_ms BIGINT NOT NULL,
    legacy_drained_at_ms BIGINT NOT NULL,
    reader_revision TEXT NOT NULL CHECK (reader_revision <> ''),
    builder_revision TEXT NOT NULL CHECK (builder_revision <> ''),
    PRIMARY KEY (tenant_id, knowledge_base_id)
);
