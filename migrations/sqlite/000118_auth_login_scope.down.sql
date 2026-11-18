DROP INDEX IF EXISTS idx_auth_tokens_session_id;
ALTER TABLE auth_tokens DROP COLUMN session_id;
