-- DingTalk SSO admin configuration (SQLite twin of versioned/000171).
-- SQLite cannot alter a CHECK constraint in place, so dingtalk_auth_events is
-- rebuilt with the widened event-type list while preserving every audit row
-- (same rebuild pattern as 000036_nextcloud_source_pairing_guards).

CREATE TABLE IF NOT EXISTS dingtalk_auth_settings (
    id                    INTEGER PRIMARY KEY,
    enabled               INTEGER NOT NULL DEFAULT 0,
    corp_id               VARCHAR(64) NOT NULL DEFAULT '',
    app_key               VARCHAR(64) NOT NULL DEFAULT '',
    agent_id              VARCHAR(64) NOT NULL DEFAULT '',
    display_name          VARCHAR(128) NOT NULL DEFAULT '',
    redirect_url          VARCHAR(512) NOT NULL DEFAULT '',
    app_secret_ciphertext TEXT NOT NULL DEFAULT '',
    config_version        INTEGER NOT NULL DEFAULT 1,
    updated_by            VARCHAR(36) NOT NULL DEFAULT '',
    created_at            DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at            DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE dingtalk_auth_events_v88 (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_type VARCHAR(32) NOT NULL CHECK(event_type IN (
        'bind', 'unbind', 'login_success', 'login_rejected', 'snapshot_update',
        'config_update', 'config_test'
    )),
    corp_id VARCHAR(64) NOT NULL DEFAULT '',
    dingtalk_user_id VARCHAR(64) NOT NULL DEFAULT '',
    job_number VARCHAR(128) NOT NULL DEFAULT '',
    user_id VARCHAR(36) NOT NULL DEFAULT '',
    reason VARCHAR(64) NOT NULL DEFAULT '',
    detail VARCHAR(512) NOT NULL DEFAULT '',
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);
INSERT INTO dingtalk_auth_events_v88
    (id, event_type, corp_id, dingtalk_user_id, job_number, user_id, reason, detail, created_at)
SELECT id, event_type, corp_id, dingtalk_user_id, job_number, user_id, reason, detail, created_at
FROM dingtalk_auth_events;
DROP TABLE dingtalk_auth_events;
ALTER TABLE dingtalk_auth_events_v88 RENAME TO dingtalk_auth_events;
CREATE INDEX IF NOT EXISTS idx_dingtalk_auth_events_user
    ON dingtalk_auth_events(user_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_dingtalk_auth_events_dingtalk
    ON dingtalk_auth_events(corp_id, dingtalk_user_id, created_at DESC);
