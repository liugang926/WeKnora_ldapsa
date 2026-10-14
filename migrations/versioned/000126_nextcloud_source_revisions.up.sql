-- A local mutation ledger for source publication intent. This is not an
-- inventory of copies in vector stores, object storage, graph, Wiki, or backups.
-- golang-migrate sends this file as one PostgreSQL query/transaction. Hold a
-- write-blocking table lock from before backfill through trigger installation:
-- earlier writers finish first, and later writers see the append trigger.
LOCK TABLE nextcloud_source_versions IN SHARE ROW EXCLUSIVE MODE;

CREATE TABLE nextcloud_source_revisions (
    tenant_id BIGINT NOT NULL,
    knowledge_base_id TEXT NOT NULL,
    datasource_id TEXT NOT NULL,
    external_id TEXT NOT NULL,
    revision BIGINT NOT NULL CHECK (revision > 0),
    desired_etag TEXT NOT NULL,
    candidate_knowledge_id TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('staging', 'published', 'tombstone')),
    source_updated_at TIMESTAMPTZ NOT NULL,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    legacy_history_unknown BOOLEAN NOT NULL,
    PRIMARY KEY (tenant_id, knowledge_base_id, datasource_id, external_id, revision)
);

-- The old mutable row contains only the latest known state. No earlier
-- candidate, ETag, publication, or tombstone can be reconstructed from it.
INSERT INTO nextcloud_source_revisions (
    tenant_id, knowledge_base_id, datasource_id, external_id, revision,
    desired_etag, candidate_knowledge_id, state, source_updated_at,
    legacy_history_unknown
)
SELECT tenant_id, knowledge_base_id, datasource_id, external_id, 1,
       desired_etag, candidate_knowledge_id, state, updated_at, TRUE
FROM nextcloud_source_versions;

CREATE FUNCTION nextcloud_source_revision_immutable() RETURNS trigger
LANGUAGE plpgsql AS $guard$
BEGIN
    RAISE EXCEPTION 'nextcloud_source_revision_immutable' USING ERRCODE = '23514';
END
$guard$;
CREATE TRIGGER nextcloud_source_revision_immutable
BEFORE UPDATE OR DELETE ON nextcloud_source_revisions FOR EACH ROW
EXECUTE FUNCTION nextcloud_source_revision_immutable();
CREATE TRIGGER nextcloud_source_revision_no_truncate
BEFORE TRUNCATE ON nextcloud_source_revisions FOR EACH STATEMENT
EXECUTE FUNCTION nextcloud_source_revision_immutable();

-- A source key may never move to another lineage or be deleted behind the
-- append-only record. Tombstone state is used for source deletion instead.
CREATE FUNCTION nextcloud_source_version_identity_guard() RETURNS trigger
LANGUAGE plpgsql AS $guard$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'nextcloud_source_version_identity_immutable' USING ERRCODE = '23514';
    END IF;
    IF NEW.tenant_id IS DISTINCT FROM OLD.tenant_id
       OR NEW.knowledge_base_id IS DISTINCT FROM OLD.knowledge_base_id
       OR NEW.datasource_id IS DISTINCT FROM OLD.datasource_id
       OR NEW.external_id IS DISTINCT FROM OLD.external_id THEN
        RAISE EXCEPTION 'nextcloud_source_version_identity_immutable' USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END
$guard$;
CREATE TRIGGER nextcloud_source_version_identity_guard
BEFORE UPDATE OR DELETE ON nextcloud_source_versions FOR EACH ROW
EXECUTE FUNCTION nextcloud_source_version_identity_guard();
CREATE TRIGGER nextcloud_source_version_no_truncate
BEFORE TRUNCATE ON nextcloud_source_versions FOR EACH STATEMENT
EXECUTE FUNCTION nextcloud_source_revision_immutable();

-- PostgreSQL serializes UPDATE and conflicting UPSERTs on the current row.
-- AFTER triggers run only for the actual INSERT or UPDATE chosen by UPSERT.
-- The next number is chosen while that row lock is held by this transaction.
CREATE FUNCTION nextcloud_source_revision_append() RETURNS trigger
LANGUAGE plpgsql AS $append$
DECLARE
    prior_revision BIGINT;
    prior_unknown BOOLEAN;
BEGIN
    SELECT revision, legacy_history_unknown
      INTO prior_revision, prior_unknown
      FROM nextcloud_source_revisions
     WHERE tenant_id = NEW.tenant_id
       AND knowledge_base_id = NEW.knowledge_base_id
       AND datasource_id = NEW.datasource_id
       AND external_id = NEW.external_id
     ORDER BY revision DESC LIMIT 1;
    IF TG_OP = 'INSERT' AND prior_revision IS NOT NULL THEN
        RAISE EXCEPTION 'nextcloud_source_revision_reinsert' USING ERRCODE = '23514';
    ELSIF TG_OP = 'UPDATE' AND prior_revision IS NULL THEN
        RAISE EXCEPTION 'nextcloud_source_revision_history_missing' USING ERRCODE = '23514';
    END IF;
    INSERT INTO nextcloud_source_revisions (
        tenant_id, knowledge_base_id, datasource_id, external_id, revision,
        desired_etag, candidate_knowledge_id, state, source_updated_at,
        legacy_history_unknown
    ) VALUES (
        NEW.tenant_id, NEW.knowledge_base_id, NEW.datasource_id, NEW.external_id,
        COALESCE(prior_revision, 0) + 1, NEW.desired_etag,
        NEW.candidate_knowledge_id, NEW.state, NEW.updated_at,
        COALESCE(prior_unknown, FALSE)
    );
    RETURN NEW;
END
$append$;
CREATE TRIGGER nextcloud_source_revision_append
AFTER INSERT OR UPDATE ON nextcloud_source_versions FOR EACH ROW
EXECUTE FUNCTION nextcloud_source_revision_append();
