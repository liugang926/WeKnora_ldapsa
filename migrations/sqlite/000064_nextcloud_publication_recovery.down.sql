-- Recovery receipts and retired epochs are safety evidence; rollback is deliberately refused.
SELECT RAISE(ABORT,'nextcloud_recovery_rollback_forbidden');
