-- Producer receipts and monotonic message versions cannot safely be dropped.
DO $$ BEGIN RAISE EXCEPTION 'knowledge provenance rollback prohibited'; END $$;
