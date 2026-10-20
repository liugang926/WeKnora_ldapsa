-- Recovery receipts and retired epochs are safety evidence; rollback is deliberately refused.
DO $guard$ BEGIN RAISE EXCEPTION 'nextcloud_recovery_rollback_forbidden'; END $guard$;
