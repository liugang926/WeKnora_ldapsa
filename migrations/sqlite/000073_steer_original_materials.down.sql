-- Authored, discarded and consumed inputs and exact completion parents must
-- survive recovery. Rollback is an explicit refusal, not a proof deletion.
SELECT steer_original_schema_rollback_forbidden();
