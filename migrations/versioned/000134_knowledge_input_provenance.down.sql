-- Provenance rollback is deliberately blocked: dropping receipts could permit unsafe reclassification.
DO $$ BEGIN RAISE EXCEPTION 'knowledge provenance rollback prohibited'; END $$;
