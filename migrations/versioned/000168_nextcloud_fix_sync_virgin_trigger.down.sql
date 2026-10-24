-- This migration repairs a broken admission function. Restoring the previous
-- definition would knowingly re-enable sync_log insert failures, so downgrade
-- intentionally leaves the corrected function in place.
SELECT 1;
