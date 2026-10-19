DO $$ BEGIN RAISE EXCEPTION 'derived storage evidence cannot be rolled back'; END $$;
