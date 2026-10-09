-- Automated rollback would erase an append-only provenance record. Export,
-- reconcile, and approve the retained history before any manual schema change.
DO $$ BEGIN
    RAISE EXCEPTION 'nextcloud_source_revision_rollback_requires_audit_export'
        USING ERRCODE = '23514';
END $$;
