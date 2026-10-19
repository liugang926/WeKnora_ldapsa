-- Original control generation counters cannot safely be rolled back.
DO $$ BEGIN RAISE EXCEPTION 'knowledge provenance rollback prohibited'; END $$;
