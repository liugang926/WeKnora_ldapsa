-- Export and reconcile provenance before a manual rollback. An automatic
-- downgrade must not erase original answer lineage or independent tombstones.
DO $$ BEGIN
    RAISE EXCEPTION 'message_source_lineage_rollback_requires_audit_export' USING ERRCODE = '23514';
END $$;
