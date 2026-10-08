DO $$ BEGIN RAISE EXCEPTION 'capacity admission rollback requires draining reservations and a coordinated restore'; END $$;
