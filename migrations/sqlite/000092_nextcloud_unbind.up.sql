-- SQLite twin of PG 000175: the provenance trigger never existed here (SQLite
-- cannot host PL/pgSQL guards), so this is a version-parity marker plus a
-- defensive trigger drop in case a future build adds one.
DROP TRIGGER IF EXISTS trg_preserve_kb_nextcloud_provenance;
