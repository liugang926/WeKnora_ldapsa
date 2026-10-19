-- This security ledger is intentionally monotonic; destructive downgrade is unsupported.
DO $$ BEGIN RAISE EXCEPTION 'nextcloud_target_closure_downgrade_denied'; END $$;
