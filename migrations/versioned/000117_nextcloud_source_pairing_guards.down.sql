-- Epoch-zero rows cannot be represented by the previous schema. The CHECK
-- deliberately rejects rollback while such a source exists.
ALTER TABLE nextcloud_source_pairings
    DROP CONSTRAINT nextcloud_source_pairings_publication_epoch_check,
    ADD CONSTRAINT nextcloud_source_pairings_publication_epoch_check
    CHECK (publication_epoch > 0);

DROP TRIGGER IF EXISTS nextcloud_source_pairing_row_guard_delete ON nextcloud_source_pairings;
DROP TRIGGER IF EXISTS nextcloud_source_pairing_row_guard_update ON nextcloud_source_pairings;
DROP FUNCTION IF EXISTS nextcloud_source_pairing_row_guard();
DROP TRIGGER IF EXISTS nextcloud_paired_kb_hard_delete_guard ON knowledge_bases;
DROP FUNCTION IF EXISTS nextcloud_paired_kb_hard_delete_guard();
