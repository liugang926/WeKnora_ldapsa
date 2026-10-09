-- Additive foundation only: no legacy answer is certified complete.
-- golang-migrate runs this file atomically. Fence evidence writes across
-- backfill and trigger installation; tombstones have no business-row FK.
LOCK TABLE knowledge_bases, data_sources, knowledges, nextcloud_source_versions,
    nextcloud_source_revisions, nextcloud_source_pairings,
    nextcloud_source_rotations, nextcloud_source_pairing_aborts,
    nextcloud_source_decommissions, nextcloud_indexed_withdrawals,
    nextcloud_event_connections, nextcloud_gc_jobs IN SHARE ROW EXCLUSIVE MODE;

ALTER TABLE messages ADD COLUMN source_lineage JSONB;
ALTER TABLE messages ADD CONSTRAINT messages_source_lineage_envelope CHECK (
    source_lineage IS NULL OR COALESCE(
        jsonb_typeof(source_lineage) = 'object'
        AND source_lineage->'version' = '1'::jsonb
        AND source_lineage->>'state' IN ('complete', 'unknown')
        AND jsonb_typeof(source_lineage->'sources') = 'array'
        AND octet_length(source_lineage::text) <= 262144, FALSE));

-- This table holds only positive ever-source facts. Absence is unknown,
-- including scopes whose pre-upgrade business rows were already removed.
CREATE TABLE nextcloud_source_tombstones (
    tenant_id BIGINT NOT NULL CHECK (tenant_id > 0),
    scope_type TEXT NOT NULL CHECK (scope_type IN ('tenant', 'knowledge_base', 'datasource', 'pair')),
    scope_id TEXT NOT NULL CHECK (length(trim(scope_id)) > 0),
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tenant_id, scope_type, scope_id),
    CHECK (scope_type <> 'tenant' OR scope_id = tenant_id::text)
);

CREATE FUNCTION nextcloud_source_tombstones_record(owner BIGINT, kb TEXT, ds TEXT, pair_id TEXT)
RETURNS void LANGUAGE plpgsql AS $record$
BEGIN
    INSERT INTO nextcloud_source_tombstones(tenant_id, scope_type, scope_id)
    SELECT owner, scope_type, scope_id FROM (VALUES
        ('tenant', owner::text), ('knowledge_base', kb), ('datasource', ds), ('pair', pair_id)
    ) AS scopes(scope_type, scope_id)
    WHERE owner > 0 AND length(trim(scope_id)) > 0
    ON CONFLICT DO NOTHING;
END
$record$;

CREATE FUNCTION nextcloud_source_tombstones_capture_row(row_data JSONB, origin TEXT)
RETURNS void LANGUAGE plpgsql AS $capture_row$
DECLARE
    marked BOOLEAN := TRUE;
    ds TEXT := row_data->>'datasource_id';
    kb TEXT := row_data->>'knowledge_base_id';
    pair_id TEXT := row_data->>'pair_operation_id';
BEGIN
    IF row_data IS NULL THEN RETURN; END IF;
    IF origin = 'data_sources' THEN
        marked := row_data->>'type' = 'nextcloud';
        ds := row_data->>'id';
    ELSIF origin = 'knowledge_bases' THEN
        marked := row_data->>'ever_had_nextcloud_source' = 'true';
        kb := row_data->>'id';
    ELSIF origin = 'knowledges' THEN
        marked := row_data->>'channel' = 'nextcloud'
            OR (row_data->'metadata') ?| ARRAY['nextcloud_instance_id', 'nextcloud_binding_id', 'nextcloud_file_id'];
        ds := row_data->'metadata'->>'datasource_id';
    ELSIF origin IN ('nextcloud_source_pairings', 'nextcloud_source_pairing_aborts') THEN
        pair_id := row_data->>'operation_id';
    END IF;
    IF marked THEN
        PERFORM nextcloud_source_tombstones_record((row_data->>'tenant_id')::bigint, kb, ds, pair_id);
    END IF;
END
$capture_row$;

CREATE FUNCTION nextcloud_source_tombstones_capture() RETURNS trigger
LANGUAGE plpgsql AS $capture$
BEGIN
    IF TG_OP <> 'INSERT' THEN
        PERFORM nextcloud_source_tombstones_capture_row(to_jsonb(OLD), TG_TABLE_NAME);
    END IF;
    IF TG_OP <> 'DELETE' THEN
        PERFORM nextcloud_source_tombstones_capture_row(to_jsonb(NEW), TG_TABLE_NAME);
        RETURN NEW;
    END IF;
    RETURN OLD;
END
$capture$;

SELECT nextcloud_source_tombstones_capture_row(to_jsonb(evidence), 'knowledge_bases') FROM knowledge_bases AS evidence;
SELECT nextcloud_source_tombstones_capture_row(to_jsonb(evidence), 'data_sources') FROM data_sources AS evidence;
SELECT nextcloud_source_tombstones_capture_row(to_jsonb(evidence), 'knowledges') FROM knowledges AS evidence;
SELECT nextcloud_source_tombstones_capture_row(to_jsonb(evidence), 'nextcloud_source_versions') FROM nextcloud_source_versions AS evidence;
SELECT nextcloud_source_tombstones_capture_row(to_jsonb(evidence), 'nextcloud_source_revisions') FROM nextcloud_source_revisions AS evidence;
SELECT nextcloud_source_tombstones_capture_row(to_jsonb(evidence), 'nextcloud_source_pairings') FROM nextcloud_source_pairings AS evidence;
SELECT nextcloud_source_tombstones_capture_row(to_jsonb(evidence), 'nextcloud_source_rotations') FROM nextcloud_source_rotations AS evidence;
SELECT nextcloud_source_tombstones_capture_row(to_jsonb(evidence), 'nextcloud_source_pairing_aborts') FROM nextcloud_source_pairing_aborts AS evidence;
SELECT nextcloud_source_tombstones_capture_row(to_jsonb(evidence), 'nextcloud_source_decommissions') FROM nextcloud_source_decommissions AS evidence;
SELECT nextcloud_source_tombstones_capture_row(to_jsonb(evidence), 'nextcloud_indexed_withdrawals') FROM nextcloud_indexed_withdrawals AS evidence;
SELECT nextcloud_source_tombstones_capture_row(to_jsonb(evidence), 'nextcloud_event_connections') FROM nextcloud_event_connections AS evidence;
SELECT nextcloud_source_tombstones_capture_row(to_jsonb(evidence), 'nextcloud_gc_jobs') FROM nextcloud_gc_jobs AS evidence;

CREATE TRIGGER nextcloud_source_tombstones_capture
AFTER INSERT OR UPDATE OR DELETE ON knowledge_bases FOR EACH ROW
EXECUTE FUNCTION nextcloud_source_tombstones_capture();
CREATE TRIGGER nextcloud_source_tombstones_capture
AFTER INSERT OR UPDATE OR DELETE ON data_sources FOR EACH ROW
EXECUTE FUNCTION nextcloud_source_tombstones_capture();
CREATE TRIGGER nextcloud_source_tombstones_capture
AFTER INSERT OR UPDATE OR DELETE ON knowledges FOR EACH ROW
EXECUTE FUNCTION nextcloud_source_tombstones_capture();
CREATE TRIGGER nextcloud_source_tombstones_capture
AFTER INSERT OR UPDATE OR DELETE ON nextcloud_source_versions FOR EACH ROW
EXECUTE FUNCTION nextcloud_source_tombstones_capture();
CREATE TRIGGER nextcloud_source_tombstones_capture
AFTER INSERT OR UPDATE OR DELETE ON nextcloud_source_revisions FOR EACH ROW
EXECUTE FUNCTION nextcloud_source_tombstones_capture();
CREATE TRIGGER nextcloud_source_tombstones_capture
AFTER INSERT OR UPDATE OR DELETE ON nextcloud_source_pairings FOR EACH ROW
EXECUTE FUNCTION nextcloud_source_tombstones_capture();
CREATE TRIGGER nextcloud_source_tombstones_capture
AFTER INSERT OR UPDATE OR DELETE ON nextcloud_source_rotations FOR EACH ROW
EXECUTE FUNCTION nextcloud_source_tombstones_capture();
CREATE TRIGGER nextcloud_source_tombstones_capture
AFTER INSERT OR UPDATE OR DELETE ON nextcloud_source_pairing_aborts FOR EACH ROW
EXECUTE FUNCTION nextcloud_source_tombstones_capture();
CREATE TRIGGER nextcloud_source_tombstones_capture
AFTER INSERT OR UPDATE OR DELETE ON nextcloud_source_decommissions FOR EACH ROW
EXECUTE FUNCTION nextcloud_source_tombstones_capture();
CREATE TRIGGER nextcloud_source_tombstones_capture
AFTER INSERT OR UPDATE OR DELETE ON nextcloud_indexed_withdrawals FOR EACH ROW
EXECUTE FUNCTION nextcloud_source_tombstones_capture();
CREATE TRIGGER nextcloud_source_tombstones_capture
AFTER INSERT OR UPDATE OR DELETE ON nextcloud_event_connections FOR EACH ROW
EXECUTE FUNCTION nextcloud_source_tombstones_capture();
CREATE TRIGGER nextcloud_source_tombstones_capture
AFTER INSERT OR UPDATE OR DELETE ON nextcloud_gc_jobs FOR EACH ROW
EXECUTE FUNCTION nextcloud_source_tombstones_capture();

CREATE FUNCTION nextcloud_source_tombstones_immutable() RETURNS trigger
LANGUAGE plpgsql AS $immutable$
BEGIN
    RAISE EXCEPTION 'nextcloud_source_tombstones_immutable' USING ERRCODE = '23514';
END
$immutable$;
CREATE TRIGGER nextcloud_source_tombstones_immutable
BEFORE UPDATE OR DELETE ON nextcloud_source_tombstones FOR EACH ROW
EXECUTE FUNCTION nextcloud_source_tombstones_immutable();
CREATE TRIGGER nextcloud_source_tombstones_no_truncate
BEFORE TRUNCATE ON nextcloud_source_tombstones FOR EACH STATEMENT
EXECUTE FUNCTION nextcloud_source_tombstones_immutable();
