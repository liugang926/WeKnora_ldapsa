ALTER TABLE sessions ADD COLUMN IF NOT EXISTS title_source_lineage JSONB;
ALTER TABLE sessions ADD COLUMN IF NOT EXISTS description_source_lineage JSONB;
ALTER TABLE sessions ADD COLUMN IF NOT EXISTS title_revision BIGINT NOT NULL DEFAULT 0 CHECK (title_revision >= 0);
CREATE OR REPLACE FUNCTION nextcloud_bump_session_title_revision() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF OLD.title_revision = 9223372036854775807 THEN RAISE EXCEPTION 'session_title_revision_overflow'; END IF;
 NEW.title_revision := OLD.title_revision + 1;
 RETURN NEW;
END;
$$;
CREATE TRIGGER nextcloud_session_title_revision BEFORE UPDATE OF title, title_source_lineage, title_revision ON sessions
FOR EACH ROW EXECUTE FUNCTION nextcloud_bump_session_title_revision();
COMMENT ON COLUMN sessions.title_source_lineage IS 'Private original title input lineage; NULL is unknown and must not be backfilled from missing source evidence';
COMMENT ON COLUMN sessions.description_source_lineage IS 'Private original description input lineage; NULL is unknown';
