-- This security ledger is intentionally monotonic; destructive downgrade is unsupported.
CREATE TEMP TABLE nextcloud_target_closure_downgrade_denied(v INTEGER CHECK(v=1)); INSERT INTO nextcloud_target_closure_downgrade_denied VALUES(0);
