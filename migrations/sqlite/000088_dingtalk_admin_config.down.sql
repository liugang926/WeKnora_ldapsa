-- Reverse 000088: rebuild dingtalk_auth_events with the original five
-- login-only event types (the rebuild mirrors the up direction) and drop the
-- admin configuration table. Admin event rows are removed first because the
-- narrowed CHECK would reject them.

CREATE TABLE dingtalk_auth_events_v88_down (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_type VARCHAR(32) NOT NULL CHECK(event_type IN (
        'bind', 'unbind', 'login_success', 'login_rejected', 'snapshot_update'
    )),
    corp_id VARCHAR(64) NOT NULL DEFAULT '',
    dingtalk_user_id VARCHAR(64) NOT NULL DEFAULT '',
    job_number VARCHAR(128) NOT NULL DEFAULT '',
    user_id VARCHAR(36) NOT NULL DEFAULT '',
    reason VARCHAR(64) NOT NULL DEFAULT '',
    detail VARCHAR(512) NOT NULL DEFAULT '',
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);
INSERT INTO dingtalk_auth_events_v88_down
    (id, event_type, corp_id, dingtalk_user_id, job_number, user_id, reason, detail, created_at)
SELECT id, event_type, corp_id, dingtalk_user_id, job_number, user_id, reason, detail, created_at
FROM dingtalk_auth_events
WHERE event_type NOT IN ('config_update', 'config_test');
DROP TABLE dingtalk_auth_events;
ALTER TABLE dingtalk_auth_events_v88_down RENAME TO dingtalk_auth_events;
CREATE INDEX IF NOT EXISTS idx_dingtalk_auth_events_user
    ON dingtalk_auth_events(user_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_dingtalk_auth_events_dingtalk
    ON dingtalk_auth_events(corp_id, dingtalk_user_id, created_at DESC);

DROP TABLE IF EXISTS dingtalk_auth_settings;
