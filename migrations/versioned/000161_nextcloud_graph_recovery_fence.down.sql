DO $$ BEGIN RAISE EXCEPTION 'Retain recovery graph fences; audited export is required before rollback'; END $$;
