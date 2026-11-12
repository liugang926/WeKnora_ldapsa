-- DingTalk enterprise SSO: identity bindings and the append-only audit trail.
-- SQLite (>= 3.8) supports partial indexes, so the "one active binding per
-- user" guard uses the same partial unique index as PostgreSQL instead of a
-- trigger; concurrent writers are additionally serialized by the
-- repository's insert-in-transaction pattern and the engine's single-writer
-- lock.

CREATE TABLE IF NOT EXISTS dingtalk_identities (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    corp_id VARCHAR(64) NOT NULL,
    dingtalk_user_id VARCHAR(64) NOT NULL,
    union_id VARCHAR(64) NOT NULL DEFAULT '',
    job_number VARCHAR(128) NOT NULL DEFAULT '',
    display_name VARCHAR(256) NOT NULL DEFAULT '',
    user_id VARCHAR(36),
    status VARCHAR(16) NOT NULL DEFAULT 'active' CHECK(status IN ('active', 'unlinked')),
    bound_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    unbound_at DATETIME,
    last_login_at DATETIME,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(corp_id, dingtalk_user_id)
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_dingtalk_identities_user
    ON dingtalk_identities(user_id) WHERE user_id IS NOT NULL AND status = 'active';
CREATE INDEX IF NOT EXISTS idx_dingtalk_identities_job_number
    ON dingtalk_identities(corp_id, job_number);

CREATE TABLE IF NOT EXISTS dingtalk_auth_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_type VARCHAR(32) NOT NULL CHECK(event_type IN ('bind', 'unbind', 'login_success', 'login_rejected', 'snapshot_update')),
    corp_id VARCHAR(64) NOT NULL DEFAULT '',
    dingtalk_user_id VARCHAR(64) NOT NULL DEFAULT '',
    job_number VARCHAR(128) NOT NULL DEFAULT '',
    user_id VARCHAR(36) NOT NULL DEFAULT '',
    reason VARCHAR(64) NOT NULL DEFAULT '',
    detail VARCHAR(512) NOT NULL DEFAULT '',
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_dingtalk_auth_events_user
    ON dingtalk_auth_events(user_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_dingtalk_auth_events_dingtalk
    ON dingtalk_auth_events(corp_id, dingtalk_user_id, created_at DESC);
