ALTER TABLE messages ADD COLUMN usage_source VARCHAR(20);
ALTER TABLE messages ADD COLUMN usage_actor_user_id VARCHAR(36);
ALTER TABLE messages ADD COLUMN usage_event_key VARCHAR(64);

CREATE TABLE im_usage_submissions (
    event_key VARCHAR(64) PRIMARY KEY NOT NULL,
    source VARCHAR(20) NOT NULL,
    session_id VARCHAR(36) NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
    actor_user_id VARCHAR(36) NOT NULL DEFAULT '',
    created_at DATETIME NOT NULL
);
CREATE INDEX idx_im_usage_submissions_session ON im_usage_submissions(session_id);
