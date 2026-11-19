DROP TABLE im_usage_submissions;
ALTER TABLE messages DROP COLUMN usage_event_key;
ALTER TABLE messages DROP COLUMN usage_actor_user_id;
ALTER TABLE messages DROP COLUMN usage_source;
