CREATE TEMP TABLE session_text_lineage_rollback_requires_audit_export (stop INTEGER NOT NULL CONSTRAINT session_text_lineage_rollback_requires_audit_export CHECK (stop = 0));
INSERT INTO session_text_lineage_rollback_requires_audit_export (stop) VALUES (1);
