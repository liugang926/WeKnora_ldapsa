DO $$ BEGIN RAISE EXCEPTION 'nextcloud_content_revision_rollback_requires_audit_export'; END $$;
