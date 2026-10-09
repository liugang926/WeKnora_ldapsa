package database

import (
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/golang-migrate/migrate/v4"
	"github.com/stretchr/testify/require"
)

func TestSQLiteSourceLineageUpgradeFrom50PreservesLegacyAndTombstones(t *testing.T) {
	root := sqliteRepoRoot(t)
	legacy := copySQLiteMigrationsThrough(t, root, 50)
	chdirAndRestore(t, legacy)
	dbPath := filepath.Join(t.TempDir(), "lineage-upgrade.db")
	require.NoError(t, RunMigrationsWithOptions("sqlite3://unused", MigrationOptions{SQLiteDBPath: dbPath}))
	db := openSQLiteDB(t, dbPath)
	seedSourceLineageLegacy(t, db, "sqlite")
	chdirAndRestore(t, root)
	require.NoError(t, RunMigrationsWithOptions("sqlite3://unused", MigrationOptions{SQLiteDBPath: dbPath}))
	version, dirty := sqliteMigrationState(t, db)
	require.Equal(t, 51, version)
	require.False(t, dirty)
	requireSourceLineageMigrationBehavior(t, db, "sqlite")
	// A downgrade cannot silently discard the durable facts or answer lineage.
	down, err := os.ReadFile(filepath.Join(root, "migrations/sqlite/000051_message_source_lineage.down.sql"))
	require.NoError(t, err)
	_, err = db.Exec(string(down))
	require.ErrorContains(t, err, "message_source_lineage_rollback_requires_audit_export")
	requireSourceTombstone(t, db, "sqlite", 7, "tenant", "7", 1)
}

func TestPostgresSourceLineageUpgradeFrom131PreservesLegacyAndTombstones(t *testing.T) {
	dsn := os.Getenv("WEKNORA_MIGRATION_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("set WEKNORA_MIGRATION_TEST_POSTGRES_DSN to a disposable database")
	}
	t.Run("ConfiguredEmbeddingMode", func(t *testing.T) {
		requirePostgresSourceLineageUpgrade(t, dsn)
	})
	// Repeat the exact failing mode while a preceding full migration test has
	// already created the parent database's public vector table and its guard.
	u, err := url.Parse(dsn)
	require.NoError(t, err)
	query := u.Query()
	query.Set("options", strings.TrimSpace(query.Get("options")+" -c app.skip_embedding=true"))
	u.RawQuery = query.Encode()
	t.Run("EmbeddingSkipped", func(t *testing.T) {
		requirePostgresSourceLineageUpgrade(t, u.String())
	})
}

func requirePostgresSourceLineageUpgrade(t *testing.T, dsn string) {
	t.Helper()
	root := sqliteRepoRoot(t)
	base, err := sql.Open("postgres", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { _ = base.Close() })
	parentEmbeddingState := snapshotPostgresPublicEmbedding(t, base)
	t.Logf("parent public.embeddings present: %t", parentEmbeddingState != "absent")
	t.Cleanup(func() {
		require.Equal(t, parentEmbeddingState, snapshotPostgresPublicEmbedding(t, base),
			"the upgrade fixture must preserve the parent public.embeddings table and its guards")
	})
	// The application migration chain contains unqualified relation/extension
	// lookups. A schema,public search_path can resolve the parent's embeddings
	// when the fixture skips vector setup and then attach its guard there.
	// Use a truly separate database; default CI exercises embedding migrations,
	// while a caller may explicitly retain app.skip_embedding in its DSN options.
	isolatedDSN := postgresLineageDatabaseDSN(t, base, dsn)
	db, err := sql.Open("postgres", isolatedDSN)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	var parentDatabase, fixtureDatabase string
	require.NoError(t, base.QueryRow("SELECT current_database()").Scan(&parentDatabase))
	require.NoError(t, db.QueryRow("SELECT current_database()").Scan(&fixtureDatabase))
	require.NotEqual(t, parentDatabase, fixtureDatabase)
	require.Equal(t, "absent", snapshotPostgresPublicEmbedding(t, db),
		"the fixture must start without the parent's embedding table")
	var skipEmbedding bool
	require.NoError(t, db.QueryRow(`SELECT COALESCE(current_setting('app.skip_embedding', true), '') = 'true`+
		`'`).Scan(&skipEmbedding))
	t.Logf("fixture embedding migrations enabled: %t", !skipEmbedding)
	m, err := migrate.New("file://"+filepath.Join(root, "migrations/versioned"), isolatedDSN)
	require.NoError(t, err)
	t.Cleanup(func() { _, _ = m.Close() })
	require.NoError(t, m.Migrate(131))
	requirePostgresMigrationVersion(t, db, 131)
	if skipEmbedding {
		require.Equal(t, "absent", snapshotPostgresPublicEmbedding(t, db))
	} else {
		require.NotEqual(t, "absent", snapshotPostgresPublicEmbedding(t, db))
		var guards int
		require.NoError(t, db.QueryRow(`SELECT count(*) FROM pg_trigger
			WHERE tgrelid = 'public.embeddings'::`+
			`regclass AND tgname = 'nextcloud_indexed_embedding_guard'`).Scan(&guards))
		require.Equal(t, 1, guards, "the fixture's own embeddings must retain its production withdrawal guard")
	}
	seedSourceLineageLegacy(t, db, "postgres")
	require.NoError(t, m.Migrate(132))
	requirePostgresMigrationVersion(t, db, 132)
	var dataType, nullable string
	var defaultValue sql.NullString
	require.NoError(t, db.QueryRow(`SELECT data_type, is_nullable, column_default
		FROM information_schema.columns WHERE table_schema = current_schema()
		AND table_name = 'messages' AND column_name = 'source_lineage'`).Scan(&dataType, &nullable, &defaultValue))
	require.Equal(t, "jsonb", dataType)
	require.Equal(t, "YES", nullable)
	require.False(t, defaultValue.Valid, "legacy NULL is not a complete empty default")
	requireSourceLineageMigrationBehavior(t, db, "postgres")
	_, err = db.Exec("TRUNCATE nextcloud_source_tombstones")
	require.ErrorContains(t, err, "nextcloud_source_tombstones_immutable")
	require.ErrorContains(t, m.Steps(-1), "message_source_lineage_rollback_requires_audit_export")
	requireSourceTombstone(t, db, "postgres", 7, "tenant", "7", 1)
}

// These PostgreSQL fixtures require CREATEDB on a disposable server. A new
// database avoids cross-schema table/index visibility in the production chain.
func postgresLineageDatabaseDSN(t *testing.T, base *sql.DB, dsn string) string {
	t.Helper()
	var random [8]byte
	_, err := rand.Read(random[:])
	require.NoError(t, err)
	name := "weknora_lineage_" + hex.EncodeToString(random[:])
	_, err = base.Exec("CREATE DATABASE " + name + " TEMPLATE template0")
	require.NoError(t, err)
	t.Cleanup(func() {
		_, err := base.Exec("DROP DATABASE " + name + " WITH (FORCE)")
		require.NoError(t, err)
	})
	u, err := url.Parse(dsn)
	require.NoError(t, err)
	u.Path = "/" + name
	u.RawPath = ""
	query := u.Query()
	query.Set("search_path", "public")
	u.RawQuery = query.Encode()
	return u.String()
}

// Capture table identity, row count, columns, indexes and trigger identity/body.
// Checking again after fixture cleanup catches both relation mutation and a
// replacement/foreign trigger, including when a preceding full migration test
// has already populated the parent database's public schema.
func snapshotPostgresPublicEmbedding(t *testing.T, db *sql.DB) string {
	t.Helper()
	var state string
	err := db.QueryRow(`SELECT jsonb_build_object(
		'table_oid', c.oid,
		'columns', (SELECT js` +
		`onb_agg(jsonb_build_object('name', a.attname,
			'type', format_type(a.a` +
		`tttypid, a.atttypmod), 'not_null', a.attnotnull,
			'default', pg_get_ex` +
		`pr(d.adbin, d.adrelid)) ORDER BY a.attnum)
			FROM pg_attribute a LEFT J` +
		`OIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
			WHE` +
		`RE a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped),
		'index` +
		`es', (SELECT jsonb_agg(jsonb_build_object('oid', i.indexrelid,
			'defin` +
		`ition', pg_get_indexdef(i.indexrelid), 'valid', i.indisvalid) ORDER BY i` +
		`.indexrelid)
			FROM pg_index i WHERE i.indrelid = c.oid),
		'triggers',` +
		` (SELECT jsonb_agg(jsonb_build_object('oid', g.oid, 'function', g.tgfoid` +
		`,
			'definition', pg_get_triggerdef(g.oid), 'function_definition', pg_g` +
		`et_functiondef(g.tgfoid),
			'enabled', g.tgenabled) ORDER BY g.oid)
			` +
		`FROM pg_trigger g WHERE g.tgrelid = c.oid AND NOT g.tgisinternal)
		)::t` +
		`ext FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
		WHER` +
		`E n.nspname = 'public' AND c.relname = 'embeddings'`).Scan(&state)
	if err == sql.ErrNoRows {
		return "absent"
	}
	require.NoError(t, err)
	var rows int64
	require.NoError(t, db.QueryRow("SELECT count(*) FROM public.embeddings").Scan(&rows))
	return fmt.Sprintf("%s rows=%d", state, rows)
}

func lineageMigrationQuery(dialect, query string) string {
	if dialect != "postgres" {
		return query
	}
	var result strings.Builder
	n := 0
	for _, c := range query {
		if c == '?' {
			n++
			fmt.Fprintf(&result, "$%d", n)
		} else {
			result.WriteRune(c)
		}
	}
	return result.String()
}

func lineageMigrationExec(t *testing.T, db *sql.DB, dialect, query string, args ...any) {
	t.Helper()
	_, err := db.Exec(lineageMigrationQuery(dialect, query), args...)
	require.NoError(t, err, query)
}

func seedSourceLineageLegacy(t *testing.T, db *sql.DB, dialect string) {
	t.Helper()
	for _, id := range []int{7, 8, 9} {
		lineageMigrationExec(t, db, dialect,
			`INSERT INTO tenants(id, name, business) VALUES (?, 'lineage', 'test')`, id)
	}
	lineageMigrationExec(t, db, dialect, `INSERT INTO messages(id, request_id, session_id, role, content)
		VALUES`+
		` ('legacy-answer', 'req', 'session', 'assistant', 'legacy copied answer'`+
		`)`)
	lineageMigrationExec(t, db, dialect, `INSERT INTO knowledge_bases
		(id, name, tenant_id, embedding_model_id, summary_model_id)
		VALUES ('legacy-kb', 'legacy', 7, 'embed', 'summary')`)
	lineageMigrationExec(t, db, dialect, `INSERT INTO data_sources
		(id, tenant_id, knowledge_base_id, name, type, config, status)
		VALUES ('legacy-source', 7, 'legacy-kb', 'legacy', 'nextcloud', '{}', 'paused')`)
	lineageMigrationExec(t, db, dialect, `INSERT INTO nextcloud_source_pairings
		(operation_id, tenant_id, knowledge_base_id, datasource_id, nextcloud_instance_id,
		binding_id, datasource_base_url, datasource_config_sha256, publication_epoch, key_id, state)
		VALUES ('legacy-pair', 7, 'legacy-kb', 'legacy-source', 'instance', 'binding',
		'https://nextcloud.test', 'hash', 1, 'key', 'pending')`)
	// Ledger evidence survives even when business rows/knowledge no longer exist.
	lineageMigrationExec(t, db, dialect, `INSERT INTO nextcloud_source_versions
		(tenant_id, knowledge_base_id, d`+
		`atasource_id, external_id, desired_etag, candidate_knowledge_id, state)
`+
		`		VALUES (9, 'orphan-kb', 'orphan-source', 'nextcloud:old:1', 'old-etag'`+
		`, 'old-knowledge', 'tombstone')`)
	lineageMigrationExec(t, db, dialect, `INSERT INTO knowledge_bases
		(id, name, tenant_id, embedding_model_id, summary_model_id, ever_had_nextcloud_source)
		VALUES ('marker-only-kb', 'marker', 9, 'embed', 'summary', TRUE)`)
	lineageMigrationExec(t, db, dialect, `INSERT INTO knowledges
		(id, tenant_id, knowledge_base_id, type, title, source, metadata)
		VALUES ('metadata-only', 7, 'metadata-only-kb', 'file', 'metadata', '',
		'{"nextcloud_instance_id":"instance","datasource_id":"deleted-metadata-source"}')`)
}

func requireSourceTombstone(t *testing.T, db *sql.DB, dialect string, tenant int, scope, id string, want int) {
	t.Helper()
	var count int
	query := lineageMigrationQuery(dialect, `SELECT count(*)
		FROM nextcloud_source_tombstones WHERE tenant_id = ? AND scope_type = ? AND scope_id = ?`)
	require.NoError(t, db.QueryRow(query, tenant, scope, id).Scan(&count))
	require.Equal(t, want, count, "%d/%s/%s", tenant, scope, id)
}

func requireSourceLineageMigrationBehavior(t *testing.T, db *sql.DB, dialect string) {
	t.Helper()
	var legacyNull bool
	require.NoError(t, db.QueryRow(
		`SELECT source_lineage IS NULL FROM messages WHERE id = 'legacy-answer'`).Scan(&legacyNull))
	require.True(t, legacyNull, "migration must not certify old answers")
	for _, scope := range []struct {
		tenant   int
		kind, id string
	}{
		{7, "tenant", "7"},
		{7, "knowledge_base", "legacy-kb"},
		{7, "datasource", "legacy-source"},
		{7, "pair", "legacy-pair"},
		{9, "tenant", "9"},
		{9, "knowledge_base", "orphan-kb"},
		{9, "datasource", "orphan-source"},
		{9, "knowledge_base", "marker-only-kb"},
		{7, "knowledge_base", "metadata-only-kb"},
		{7, "datasource", "deleted-metadata-source"},
	} {
		requireSourceTombstone(t, db, dialect, scope.tenant, scope.kind, scope.id, 1)
	}
	requireSourceTombstone(t, db, dialect, 8, "tenant", "8", 0) // absence remains unknown
	for _, invalid := range []string{
		`{}`, `{"version":1,"sources":[]}`, `{"version":2,"state":"complete","sources":[]}`,
		`{"version":1,"state":"complete","sources":null}`,
		`{"version":1,"state":"complete","sources":[],"extra":true} trailing`,
	} {
		_, err := db.Exec(lineageMigrationQuery(dialect,
			`UPDATE messages SET source_lineage = ? WHERE id = 'legacy-answer'`), invalid)
		require.Error(t, err, invalid)
	}
	lineageMigrationExec(t, db, dialect, `UPDATE messages SET source_lineage = ? WHERE id = 'legacy-answer'`,
		`{"version":1,"state":"unknown","sources":[]}`)
	// Complete empty is representable, but no producer or read path is enabled by
	// this schema test. It is not a claim that the legacy content is independent.
	lineageMigrationExec(t, db, dialect, `INSERT INTO messages(id, request_id, session_id, role, content, source_l`+
		`ineage)
		VALUES ('typed-answer', 'req2', 'session', 'assistant', 'ordin`+
		`ary', ?)`, `{"version":1,"state":"complete","sources":[]}`)
	_, err := db.Exec(`UPDATE nextcloud_source_tombstones SET scope_id = 'changed' WHERE tenant` +
		`_id = 7`)
	require.ErrorContains(t, err, "nextcloud_source_tombstones_immutable")
	_, err = db.Exec(`DELETE FROM nextcloud_source_tombstones WHERE tenant_id = 7`)
	require.ErrorContains(t, err, "nextcloud_source_tombstones_immutable")
	if dialect == "sqlite" {
		var original string
		require.NoError(t, db.QueryRow(`SELECT recorded_at FROM nextcloud_source_tombstones WHERE tenant_id = 7 `+
			`AND scope_type = 'tenant'`).Scan(&original))
		lineageMigrationExec(t, db, dialect, `INSERT OR REPLACE INTO nextcloud_source_tombstones
			(tenant_id, scope_type, scope_id, recorded_at) VALUES (7, 'tenant', '7', '2099-01-01')`)
		var stillOriginal string
		require.NoError(t, db.QueryRow(`SELECT recorded_at FROM nextcloud_source_tombstones WHERE tenant_id = 7 `+
			`AND scope_type = 'tenant'`).Scan(&stillOriginal))
		require.Equal(t, original, stillOriginal, "REPLACE must not rewrite the first fact")
	}
	// Delete/recreate business scope without deleting its independent fact.
	lineageMigrationExec(t, db, dialect, `DELETE FROM nextcloud_source_pairings WHERE operation_id = 'legacy-pair'`)
	lineageMigrationExec(t, db, dialect, `DELETE FROM data_sources WHERE id = 'legacy-source'`)
	lineageMigrationExec(t, db, dialect, `DELETE FROM knowledge_bases WHERE id = 'legacy-kb'`)
	requireSourceTombstone(t, db, dialect, 7, "pair", "legacy-pair", 1)
	requireSourceTombstone(t, db, dialect, 7, "datasource", "legacy-source", 1)
	requireSourceTombstone(t, db, dialect, 7, "knowledge_base", "legacy-kb", 1)
	lineageMigrationExec(t, db, dialect, `INSERT INTO knowledge_bases
		(id, name, tenant_id, embedding_model_id, summary_model_id)
		VALUES ('legacy-kb', 'recreated', 7, 'embed', 'summary')`)
	requireSourceTombstone(t, db, dialect, 7, "knowledge_base", "legacy-kb", 1)
	// New raw source admission creates independent evidence in the same commit;
	// transaction rollback must not leave an ever fact for an uncommitted source.
	tx, err := db.Begin()
	require.NoError(t, err)
	_, err = tx.Exec(`INSERT INTO data_sources (id, tenant_id, knowledge_base_id, name, type, ` +
		`config, status)
		VALUES ('rolled-back-source', 8, 'never-committed', 'r` +
		`ollback', 'nextcloud', '{}', 'paused')`)
	require.NoError(t, err)
	require.NoError(t, tx.Rollback())
	requireSourceTombstone(t, db, dialect, 8, "tenant", "8", 0)
	lineageMigrationExec(t, db, dialect, `INSERT INTO data_sources (id, tenant_id, knowledge_base_id, name, type, `+
		`config, status)
		VALUES ('new-source', 8, 'new-kb', 'new', 'nextcloud',`+
		` '{}', 'paused')`)
	requireSourceTombstone(t, db, dialect, 8, "tenant", "8", 1)
	requireSourceTombstone(t, db, dialect, 8, "datasource", "new-source", 1)
	lineageMigrationExec(t, db, dialect, `DELETE FROM data_sources WHERE id = 'new-source'`)
	requireSourceTombstone(t, db, dialect, 8, "datasource", "new-source", 1)
}
