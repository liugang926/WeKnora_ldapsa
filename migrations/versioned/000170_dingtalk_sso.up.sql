-- DingTalk enterprise SSO: identity bindings and the append-only audit trail.
-- Bindings are keyed by the stable (corp_id, dingtalk_user_id) pair; job_number
-- is a bind-time snapshot matched exactly (leading zeros preserved) against
-- directory_identities.sam_account_name. No accounts are auto-created: a
-- binding only links an existing WeKnora user found via the AD job number.

CREATE TABLE IF NOT EXISTS dingtalk_identities (
    id                BIGSERIAL PRIMARY KEY,
    corp_id           VARCHAR(64) NOT NULL,
    dingtalk_user_id  VARCHAR(64) NOT NULL,
    union_id          VARCHAR(64) NOT NULL DEFAULT '',
    job_number        VARCHAR(128) NOT NULL DEFAULT '',
    display_name      VARCHAR(256) NOT NULL DEFAULT '',
    user_id           VARCHAR(36),
    status            VARCHAR(16) NOT NULL DEFAULT 'active',
    bound_at          TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    unbound_at        TIMESTAMP WITH TIME ZONE,
    last_login_at     TIMESTAMP WITH TIME ZONE,
    created_at        TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at        TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_dingtalk_identities_dingtalk UNIQUE(corp_id, dingtalk_user_id),
    CONSTRAINT chk_dingtalk_identities_status CHECK (status IN ('active', 'unlinked'))
);

-- At most one active binding per WeKnora user; unlinked history rows keep
-- their user_id cleared by MarkUnlinked so the partial predicate excludes them.
CREATE UNIQUE INDEX IF NOT EXISTS uq_dingtalk_identities_user
    ON dingtalk_identities(user_id) WHERE user_id IS NOT NULL AND status = 'active';
CREATE INDEX IF NOT EXISTS idx_dingtalk_identities_job_number
    ON dingtalk_identities(corp_id, job_number);

CREATE TABLE IF NOT EXISTS dingtalk_auth_events (
    id                BIGSERIAL PRIMARY KEY,
    event_type        VARCHAR(32) NOT NULL,
    corp_id           VARCHAR(64) NOT NULL DEFAULT '',
    dingtalk_user_id  VARCHAR(64) NOT NULL DEFAULT '',
    job_number        VARCHAR(128) NOT NULL DEFAULT '',
    user_id           VARCHAR(36) NOT NULL DEFAULT '',
    reason            VARCHAR(64) NOT NULL DEFAULT '',
    detail            VARCHAR(512) NOT NULL DEFAULT '',
    created_at        TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT chk_dingtalk_auth_events_type CHECK (
        event_type IN ('bind', 'unbind', 'login_success', 'login_rejected', 'snapshot_update')
    )
);

CREATE INDEX IF NOT EXISTS idx_dingtalk_auth_events_user
    ON dingtalk_auth_events(user_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_dingtalk_auth_events_dingtalk
    ON dingtalk_auth_events(corp_id, dingtalk_user_id, created_at DESC);
