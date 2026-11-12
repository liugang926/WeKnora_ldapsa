-- DingTalk SSO admin configuration: the single-row settings table backing
-- config_source=database, plus the audit-trail widening for the admin events.
-- The row is intentionally NOT seeded: absence is the "use env baseline"
-- state, so creating the table changes nothing for existing deployments.

CREATE TABLE IF NOT EXISTS dingtalk_auth_settings (
    id                     BIGINT PRIMARY KEY,
    enabled                BOOLEAN NOT NULL DEFAULT FALSE,
    corp_id                VARCHAR(64) NOT NULL DEFAULT '',
    app_key                VARCHAR(64) NOT NULL DEFAULT '',
    agent_id               VARCHAR(64) NOT NULL DEFAULT '',
    display_name           VARCHAR(128) NOT NULL DEFAULT '',
    redirect_url           VARCHAR(512) NOT NULL DEFAULT '',
    app_secret_ciphertext  TEXT NOT NULL DEFAULT '',
    config_version         BIGINT NOT NULL DEFAULT 1,
    updated_by             VARCHAR(36) NOT NULL DEFAULT '',
    created_at             TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at             TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- config_update / config_test are administrator events, not login events,
-- but they belong to the same append-only audit trail. PostgreSQL swaps the
-- CHECK constraint in place (same drop+add pattern as 000122).
ALTER TABLE dingtalk_auth_events DROP CONSTRAINT chk_dingtalk_auth_events_type;
ALTER TABLE dingtalk_auth_events ADD CONSTRAINT chk_dingtalk_auth_events_type CHECK (
    event_type IN (
        'bind', 'unbind', 'login_success', 'login_rejected', 'snapshot_update',
        'config_update', 'config_test'
    )
);
