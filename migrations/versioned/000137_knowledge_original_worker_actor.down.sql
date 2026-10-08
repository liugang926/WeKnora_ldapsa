-- Original worker actor selectors cannot safely be dropped.
DO $$ BEGIN RAISE EXCEPTION 'knowledge provenance rollback prohibited'; END $$;
