-- Immutable storage ancestry cannot be removed by downgrade.
DO $$ BEGIN RAISE EXCEPTION 'config_body_downgrade_denied'; END $$;
