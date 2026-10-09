CREATE TABLE nextcloud_source_pairing_aborts (
    operation_id TEXT PRIMARY KEY,
    tenant_id INTEGER NOT NULL,
    knowledge_base_id TEXT NOT NULL,
    datasource_id TEXT NOT NULL UNIQUE,
    nextcloud_instance_id TEXT NOT NULL,
    binding_id TEXT NOT NULL,
    datasource_base_url TEXT NOT NULL,
    datasource_config_sha256 TEXT NOT NULL,
    publication_epoch INTEGER NOT NULL,
    key_id TEXT NOT NULL,
    state TEXT NOT NULL DEFAULT 'aborted' CHECK (state = 'aborted'),
    last_error_code TEXT NOT NULL DEFAULT '',
    created_at DATETIME NOT NULL,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);
