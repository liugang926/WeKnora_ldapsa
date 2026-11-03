-- Reverse 000171: restore the five login-only event types and drop the
-- admin configuration table. Rows recorded for the two admin event types are
-- deleted first so the narrowed CHECK accepts the remaining history.

DELETE FROM dingtalk_auth_events
    WHERE event_type IN ('config_update', 'config_test');
ALTER TABLE dingtalk_auth_events DROP CONSTRAINT chk_dingtalk_auth_events_type;
ALTER TABLE dingtalk_auth_events ADD CONSTRAINT chk_dingtalk_auth_events_type CHECK (
    event_type IN ('bind', 'unbind', 'login_success', 'login_rejected', 'snapshot_update')
);

DROP TABLE IF EXISTS dingtalk_auth_settings;
