-- LDAP/AD directory identities, complete membership snapshots and group-based
-- workspace/resource authorization. All resource rows default to inheritance,
-- so installing this migration does not change existing access behaviour.

CREATE TABLE IF NOT EXISTS directories (
    id                         VARCHAR(36) PRIMARY KEY DEFAULT uuid_generate_v4(),
    name                       VARCHAR(128) NOT NULL,
    protocol                   VARCHAR(32) NOT NULL,
    enabled                    BOOLEAN NOT NULL DEFAULT FALSE,
    config_source              VARCHAR(16) NOT NULL DEFAULT 'database',
    tls_mode                   VARCHAR(16) NOT NULL,
    server_urls                JSONB NOT NULL DEFAULT '[]'::jsonb,
    server_names               JSONB NOT NULL DEFAULT '[]'::jsonb,
    base_dn                    TEXT NOT NULL,
    user_base_dn               TEXT NOT NULL,
    group_base_dn              TEXT NOT NULL,
    user_filter                TEXT NOT NULL,
    group_filter               TEXT NOT NULL,
    allowed_login_filter       TEXT NOT NULL DEFAULT '',
    service_account_dn         TEXT NOT NULL,
    password_ciphertext        TEXT NOT NULL,
    enterprise_ca_pem          TEXT,
    security_config_fingerprint VARCHAR(64) NOT NULL DEFAULT '',
    connect_timeout_seconds    INTEGER NOT NULL DEFAULT 5,
    query_timeout_seconds      INTEGER NOT NULL DEFAULT 10,
    page_size                  INTEGER NOT NULL DEFAULT 500,
    result_limit               INTEGER NOT NULL DEFAULT 10000,
    sync_interval_seconds      INTEGER NOT NULL DEFAULT 300,
    stale_after_seconds        INTEGER NOT NULL DEFAULT 900,
    config_version             BIGINT NOT NULL DEFAULT 1,
    snapshot_version           BIGINT NOT NULL DEFAULT 0,
    last_successful_sync_at    TIMESTAMP WITH TIME ZONE,
    last_sync_attempt_at       TIMESTAMP WITH TIME ZONE,
    last_sync_error            TEXT NOT NULL DEFAULT '',
    sync_lease_owner           VARCHAR(64) NOT NULL DEFAULT '',
    sync_lease_expires_at      TIMESTAMP WITH TIME ZONE,
    created_at                 TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at                 TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT chk_directories_protocol CHECK (protocol IN ('ldap', 'active_directory')),
    CONSTRAINT chk_directories_tls CHECK (tls_mode IN ('ldaps', 'starttls')),
    CONSTRAINT chk_directories_source CHECK (config_source IN ('database', 'file'))
);

CREATE INDEX IF NOT EXISTS idx_directories_enabled ON directories(enabled);

CREATE TABLE IF NOT EXISTS directory_identities (
    id                 VARCHAR(36) PRIMARY KEY DEFAULT uuid_generate_v4(),
    directory_id       VARCHAR(36) NOT NULL REFERENCES directories(id) ON DELETE CASCADE,
    object_guid        VARCHAR(128) NOT NULL,
    object_sid         VARCHAR(256),
    dn                 TEXT NOT NULL,
    sam_account_name   VARCHAR(256),
    upn                VARCHAR(320),
    display_name       VARCHAR(512),
    email              VARCHAR(320),
    primary_group_sid  VARCHAR(256),
    user_id            VARCHAR(36),
    status             VARCHAR(24) NOT NULL DEFAULT 'active',
    disabled_reason    TEXT NOT NULL DEFAULT '',
    snapshot_version   BIGINT NOT NULL DEFAULT 0,
    last_seen_at       TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_at         TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at         TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_directory_identities_object
    ON directory_identities(directory_id, object_guid);
CREATE UNIQUE INDEX IF NOT EXISTS uq_directory_identities_linked_user
    ON directory_identities(directory_id, user_id) WHERE user_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_directory_identities_user ON directory_identities(user_id);
CREATE INDEX IF NOT EXISTS idx_directory_identities_sam ON directory_identities(directory_id, sam_account_name);
CREATE INDEX IF NOT EXISTS idx_directory_identities_upn ON directory_identities(directory_id, upn);

CREATE TABLE IF NOT EXISTS directory_groups (
    id                 VARCHAR(36) PRIMARY KEY DEFAULT uuid_generate_v4(),
    directory_id       VARCHAR(36) NOT NULL REFERENCES directories(id) ON DELETE CASCADE,
    object_guid        VARCHAR(128) NOT NULL,
    object_sid         VARCHAR(256),
    dn                 TEXT NOT NULL,
    sam_account_name   VARCHAR(256),
    display_name       VARCHAR(512) NOT NULL,
    email              VARCHAR(320),
    status             VARCHAR(24) NOT NULL DEFAULT 'active',
    snapshot_version   BIGINT NOT NULL DEFAULT 0,
    last_seen_at       TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_at         TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at         TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_directory_groups_object
    ON directory_groups(directory_id, object_guid);
CREATE INDEX IF NOT EXISTS idx_directory_groups_sid ON directory_groups(directory_id, object_sid);
CREATE INDEX IF NOT EXISTS idx_directory_groups_sam ON directory_groups(directory_id, sam_account_name);
CREATE INDEX IF NOT EXISTS idx_directory_groups_name ON directory_groups(directory_id, display_name);

CREATE TABLE IF NOT EXISTS directory_group_edges (
    id                 BIGSERIAL PRIMARY KEY,
    directory_id       VARCHAR(36) NOT NULL REFERENCES directories(id) ON DELETE CASCADE,
    parent_group_id    VARCHAR(36) NOT NULL REFERENCES directory_groups(id) ON DELETE CASCADE,
    child_group_id     VARCHAR(36) NOT NULL REFERENCES directory_groups(id) ON DELETE CASCADE,
    snapshot_version   BIGINT NOT NULL,
    created_at         TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_directory_group_edge UNIQUE(directory_id, parent_group_id, child_group_id),
    CONSTRAINT chk_directory_group_edge_not_self CHECK (parent_group_id <> child_group_id)
);

CREATE INDEX IF NOT EXISTS idx_directory_group_edges_child ON directory_group_edges(child_group_id);

CREATE TABLE IF NOT EXISTS directory_group_memberships (
    id                 BIGSERIAL PRIMARY KEY,
    directory_id       VARCHAR(36) NOT NULL REFERENCES directories(id) ON DELETE CASCADE,
    group_id           VARCHAR(36) NOT NULL REFERENCES directory_groups(id) ON DELETE CASCADE,
    identity_id        VARCHAR(36) NOT NULL REFERENCES directory_identities(id) ON DELETE CASCADE,
    direct             BOOLEAN NOT NULL DEFAULT FALSE,
    "primary"          BOOLEAN NOT NULL DEFAULT FALSE,
    depth              INTEGER NOT NULL DEFAULT 0,
    source             VARCHAR(24) NOT NULL,
    snapshot_version   BIGINT NOT NULL,
    created_at         TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_directory_group_membership UNIQUE(directory_id, group_id, identity_id)
);

CREATE INDEX IF NOT EXISTS idx_directory_memberships_identity ON directory_group_memberships(identity_id);
CREATE INDEX IF NOT EXISTS idx_directory_memberships_group ON directory_group_memberships(group_id);

CREATE TABLE IF NOT EXISTS directory_sync_runs (
    id                 VARCHAR(36) PRIMARY KEY DEFAULT uuid_generate_v4(),
    directory_id       VARCHAR(36) NOT NULL,
    status             VARCHAR(16) NOT NULL,
    trigger            VARCHAR(16) NOT NULL DEFAULT 'manual',
    snapshot_version   BIGINT NOT NULL DEFAULT 0,
    user_count         INTEGER NOT NULL DEFAULT 0,
    group_count        INTEGER NOT NULL DEFAULT 0,
    membership_count   INTEGER NOT NULL DEFAULT 0,
    error_code         VARCHAR(64),
    error_message      TEXT NOT NULL DEFAULT '',
    started_at         TIMESTAMP WITH TIME ZONE NOT NULL,
    completed_at       TIMESTAMP WITH TIME ZONE,
    created_at         TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_directory_sync_runs_directory_started
    ON directory_sync_runs(directory_id, started_at DESC);

CREATE TABLE IF NOT EXISTS tenant_group_role_grants (
    id                  VARCHAR(36) PRIMARY KEY DEFAULT uuid_generate_v4(),
    tenant_id           BIGINT NOT NULL,
    directory_group_id  VARCHAR(36) NOT NULL REFERENCES directory_groups(id) ON DELETE CASCADE,
    role                VARCHAR(20) NOT NULL,
    origin              VARCHAR(16) NOT NULL DEFAULT 'manual',
    created_by          VARCHAR(36) NOT NULL DEFAULT '',
    created_at          TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_tenant_group_role_origin UNIQUE(tenant_id, directory_group_id, origin),
    CONSTRAINT chk_tenant_group_role CHECK (role IN ('viewer', 'contributor', 'admin')),
    CONSTRAINT chk_tenant_group_origin CHECK (origin IN ('manual', 'directory'))
);

CREATE INDEX IF NOT EXISTS idx_tenant_group_role_grants_tenant ON tenant_group_role_grants(tenant_id);
CREATE INDEX IF NOT EXISTS idx_tenant_group_role_grants_group ON tenant_group_role_grants(directory_group_id);

CREATE TABLE IF NOT EXISTS resource_access_policies (
    id             VARCHAR(36) PRIMARY KEY DEFAULT uuid_generate_v4(),
    tenant_id      BIGINT NOT NULL,
    resource_type  VARCHAR(32) NOT NULL,
    resource_id    VARCHAR(64) NOT NULL,
    mode           VARCHAR(16) NOT NULL DEFAULT 'inherit',
    updated_by     VARCHAR(36) NOT NULL DEFAULT '',
    created_at     TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at     TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_resource_access_policy UNIQUE(tenant_id, resource_type, resource_id),
    CONSTRAINT chk_resource_access_policy_type CHECK (resource_type IN ('knowledge_base', 'agent')),
    CONSTRAINT chk_resource_access_policy_mode CHECK (mode IN ('inherit', 'restricted'))
);

CREATE INDEX IF NOT EXISTS idx_resource_access_policies_resource
    ON resource_access_policies(resource_type, resource_id);

CREATE TABLE IF NOT EXISTS resource_group_grants (
    id                  VARCHAR(36) PRIMARY KEY DEFAULT uuid_generate_v4(),
    tenant_id           BIGINT NOT NULL,
    resource_type       VARCHAR(32) NOT NULL,
    resource_id         VARCHAR(64) NOT NULL,
    directory_group_id  VARCHAR(36) NOT NULL REFERENCES directory_groups(id) ON DELETE CASCADE,
    permission          VARCHAR(16) NOT NULL,
    origin              VARCHAR(16) NOT NULL DEFAULT 'manual',
    created_by          VARCHAR(36) NOT NULL DEFAULT '',
    created_at          TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_resource_group_grant UNIQUE(tenant_id, resource_type, resource_id, directory_group_id, permission, origin),
    CONSTRAINT chk_resource_group_grant_type CHECK (resource_type IN ('knowledge_base', 'agent')),
    CONSTRAINT chk_resource_group_origin CHECK (origin IN ('manual', 'directory')),
    CONSTRAINT chk_resource_group_permission CHECK (
        (resource_type = 'knowledge_base' AND permission IN ('read', 'edit')) OR
        (resource_type = 'agent' AND permission IN ('use', 'edit'))
    )
);

CREATE INDEX IF NOT EXISTS idx_resource_group_grants_resource
    ON resource_group_grants(tenant_id, resource_type, resource_id);
CREATE INDEX IF NOT EXISTS idx_resource_group_grants_group ON resource_group_grants(directory_group_id);

CREATE TABLE IF NOT EXISTS directory_permission_versions (
    tenant_id   BIGINT PRIMARY KEY,
    version     BIGINT NOT NULL DEFAULT 0,
    updated_at  TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);
