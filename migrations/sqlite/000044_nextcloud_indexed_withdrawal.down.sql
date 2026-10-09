-- Do not delete a committed safety tombstone during downgrade.
CREATE TABLE nextcloud_indexed_withdrawal_down_guard (id INTEGER CHECK (id = 0));
INSERT INTO nextcloud_indexed_withdrawal_down_guard
SELECT 1 WHERE EXISTS (SELECT 1 FROM nextcloud_indexed_withdrawals);
DROP TABLE nextcloud_indexed_withdrawal_down_guard;
DROP TRIGGER nextcloud_indexed_withdrawal_resume_guard;
DROP TRIGGER nextcloud_indexed_excludes_empty_ack;
DROP TRIGGER nextcloud_indexed_withdrawal_item_immutable_delete;
DROP TRIGGER nextcloud_indexed_withdrawal_item_immutable_update;
DROP TRIGGER nextcloud_indexed_withdrawal_immutable_delete;
DROP TRIGGER nextcloud_indexed_withdrawal_immutable_update;
DROP TABLE nextcloud_indexed_withdrawal_items;
DROP TABLE nextcloud_indexed_withdrawals;
