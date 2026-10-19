-- Durable head/outbox and graph operation IDs must survive backup replay.
DO $$ BEGIN RAISE EXCEPTION 'nextcloud_processing_state_rollback_requires_audit_export'; END $$;
