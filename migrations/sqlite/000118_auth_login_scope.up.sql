ALTER TABLE auth_tokens ADD COLUMN session_id VARCHAR(36);
-- Previous issuers persisted the two rows with the same timestamp. Only
-- associate unambiguous pairs; never infer a session from approximate times.
WITH pairs AS (
    SELECT user_id, created_at, MIN(id) AS family FROM auth_tokens
    GROUP BY user_id, created_at HAVING COUNT(*) = 2
    AND COUNT(DISTINCT token_type) = 2
)
UPDATE auth_tokens AS a SET session_id = p.family FROM pairs AS p
WHERE a.user_id = p.user_id AND a.created_at = p.created_at;
CREATE INDEX idx_auth_tokens_session_id ON auth_tokens(session_id);
