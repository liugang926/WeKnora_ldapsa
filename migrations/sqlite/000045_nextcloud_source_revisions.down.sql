-- Automated rollback would erase an append-only provenance record. This
-- deliberately fails until an operator exports and reconciles the history.
SELECT * FROM nextcloud_source_revision_rollback_requires_audit_export;
