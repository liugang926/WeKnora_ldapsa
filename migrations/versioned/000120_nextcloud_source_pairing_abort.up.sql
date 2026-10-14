-- An aborted one-time operation remains non-reusable after its empty paused
-- data source is removed, while live pair uniqueness is released.
CREATE TABLE nextcloud_source_pairing_aborts (
    operation_id TEXT PRIMARY KEY,
    tenant_id BIGINT NOT NULL,
    knowledge_base_id TEXT NOT NULL,
    datasource_id TEXT NOT NULL UNIQUE,
    nextcloud_instance_id TEXT NOT NULL,
    binding_id TEXT NOT NULL,
    datasource_base_url TEXT NOT NULL,
    datasource_config_sha256 TEXT NOT NULL,
    publication_epoch BIGINT NOT NULL,
    key_id TEXT NOT NULL,
    state TEXT NOT NULL DEFAULT 'aborted' CHECK (state = 'aborted'),
    last_error_code TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
