-- A deliberate failure preserves lineage/tombstones on attempted downgrade.
SELECT * FROM message_source_lineage_rollback_requires_audit_export;
