DO $$ BEGIN
    RAISE EXCEPTION 'session_text_lineage_rollback_requires_audit_export' USING ERRCODE = '23514';
END $$;
