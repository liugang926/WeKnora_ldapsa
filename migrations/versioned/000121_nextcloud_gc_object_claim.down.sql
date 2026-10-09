-- Refuse to discard durable in-flight claims. A deleting resource must be
-- recovered or explicitly reconciled before rolling this protocol back.
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM nextcloud_gc_items WHERE state = 'deleting') THEN
        RAISE EXCEPTION 'Nextcloud GC has in-flight object deletions';
    END IF;
END $$;

ALTER TABLE nextcloud_gc_items
    DROP CONSTRAINT nextcloud_gc_items_state_check;
ALTER TABLE nextcloud_gc_items
    ADD CONSTRAINT nextcloud_gc_items_state_check
    CHECK (state IN ('pending', 'blocked', 'collected'));
ALTER TABLE nextcloud_gc_items
    DROP COLUMN lease_token,
    DROP COLUMN lease_until;
