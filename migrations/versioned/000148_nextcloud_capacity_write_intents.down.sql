DO $$ BEGIN RAISE EXCEPTION 'capacity write intent evidence cannot be rolled back'; END $$;
