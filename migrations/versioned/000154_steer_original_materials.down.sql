-- Authored, discarded and consumed input identities cannot be erased to
-- revive a previous Redis input or remove an assistant's exact parent proof.
DO $$ BEGIN RAISE EXCEPTION 'original steer schema rollback forbidden'; END $$;
