-- Restore the provenance-immutability guard (migration 000111 semantics).
CREATE OR REPLACE FUNCTION preserve_kb_nextcloud_provenance() RETURNS trigger AS $$
BEGIN
    IF OLD.ever_had_nextcloud_source AND NOT NEW.ever_had_nextcloud_source THEN
        RAISE EXCEPTION 'Nextcloud knowledge-base provenance cannot be cleared';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_preserve_kb_nextcloud_provenance ON knowledge_bases;
CREATE TRIGGER trg_preserve_kb_nextcloud_provenance
    BEFORE UPDATE OF ever_had_nextcloud_source ON knowledge_bases
    FOR EACH ROW EXECUTE FUNCTION preserve_kb_nextcloud_provenance();
