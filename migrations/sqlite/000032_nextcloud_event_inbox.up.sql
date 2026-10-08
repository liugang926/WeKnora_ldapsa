CREATE TABLE nextcloud_event_connections (
    connection_id TEXT PRIMARY KEY,
    tenant_id INTEGER NOT NULL,
    knowledge_base_id TEXT NOT NULL,
    datasource_id TEXT NOT NULL,
    nextcloud_instance_id TEXT NOT NULL,
    binding_id TEXT NOT NULL,
    datasource_base_url TEXT NOT NULL,
    datasource_config_sha256 TEXT NOT NULL
        CHECK (LENGTH(datasource_config_sha256) = 64),
    status TEXT NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'active', 'revoked')),
    current_key_id TEXT NOT NULL,
    current_secret_ciphertext TEXT NOT NULL
        CHECK (SUBSTR(current_secret_ciphertext, 1, LENGTH('enc:v1:')) = 'enc:v1:'
               AND LENGTH(current_secret_ciphertext) > LENGTH('enc:v1:')),
    previous_key_id TEXT,
    previous_secret_ciphertext TEXT,
    previous_valid_until DATETIME,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CHECK (connection_id <> '' AND knowledge_base_id <> '' AND datasource_id <> ''
           AND nextcloud_instance_id <> '' AND binding_id <> ''
           AND datasource_base_url <> '' AND current_key_id <> ''),
    CHECK ((previous_key_id IS NULL AND previous_secret_ciphertext IS NULL
            AND previous_valid_until IS NULL)
           OR (previous_key_id IS NOT NULL AND previous_key_id <> ''
               AND previous_secret_ciphertext IS NOT NULL
               AND SUBSTR(previous_secret_ciphertext, 1, LENGTH('enc:v1:')) = 'enc:v1:'
               AND LENGTH(previous_secret_ciphertext) > LENGTH('enc:v1:')
               AND previous_valid_until IS NOT NULL))
);

CREATE UNIQUE INDEX uq_nextcloud_event_connections_active_datasource
    ON nextcloud_event_connections (datasource_id) WHERE status = 'active';

CREATE UNIQUE INDEX uq_nextcloud_event_connections_active_binding
    ON nextcloud_event_connections (nextcloud_instance_id, binding_id)
    WHERE status = 'active';

CREATE TABLE nextcloud_event_nonces (
    connection_id TEXT NOT NULL REFERENCES nextcloud_event_connections (connection_id) ON DELETE RESTRICT,
    key_id TEXT NOT NULL,
    nonce TEXT NOT NULL,
    expires_at DATETIME NOT NULL,
    PRIMARY KEY (connection_id, key_id, nonce)
);

CREATE INDEX idx_nextcloud_event_nonces_expires
    ON nextcloud_event_nonces (expires_at);

CREATE TABLE nextcloud_event_inbox (
    connection_id TEXT NOT NULL REFERENCES nextcloud_event_connections (connection_id) ON DELETE RESTRICT,
    event_id INTEGER NOT NULL CHECK (event_id > 0),
    payload_sha256 TEXT NOT NULL,
    event_type TEXT NOT NULL,
    file_id INTEGER,
    received_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    state TEXT NOT NULL DEFAULT 'pending',
    PRIMARY KEY (connection_id, event_id)
);

CREATE INDEX idx_nextcloud_event_inbox_state
    ON nextcloud_event_inbox (connection_id, state, event_id);

CREATE TABLE nextcloud_event_checkpoint (
    connection_id TEXT PRIMARY KEY REFERENCES nextcloud_event_connections (connection_id) ON DELETE RESTRICT,
    received_id INTEGER NOT NULL DEFAULT 0 CHECK (received_id >= 0)
);
