package database

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/golang-migrate/migrate/v4"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func checkKnowledgeActorMigration(t *testing.T, db *sql.DB) {
	t.Helper()
	var count int
	require.NoError(t, db.QueryRow("SELECT count(*) FROM knowledge_input_actor_selectors").Scan(&count))
	require.Zero(t, count, "migration must not fabricate original actors for old admissions")
	digest := strings.Repeat("a", 64)
	_, err := db.Exec(
		`INSERT INTO knowledge_input_admissions(admission_id,tenant_id,knowledge_base_id,knowledge_id,build_generation,origin_kind,input_digest,root_digest,config_digest,config_revision,source_lineage) VALUES('actor-migration',7,'kb','actor-doc',1,'history',$1,$2,$3,1,'{"version":1,"state":"complete","sources":[]}')`,
		digest,
		digest,
		digest,
	)
	require.NoError(t, err)
	for _, statement := range []string{
		`INSERT INTO knowledge_input_actor_selectors(admission_id,caller_tenant_id,user_id,principal_type,principal_id,api_key_id) VALUES('actor-migration',7,'','api_tenant','7',NULL)`,
		`INSERT INTO knowledge_input_actor_selectors(admission_id,caller_tenant_id,user_id,principal_type,principal_id,api_key_id) VALUES('actor-migration',7,'user','web_user','other',NULL)`,
		`INSERT INTO knowledge_input_actor_selectors(admission_id,caller_tenant_id,user_id,principal_type,principal_id,api_key_id) VALUES('actor-migration',7,'','api_platform','platform',1)`,
	} {
		_, err := db.Exec(statement)
		require.Error(t, err, "malformed original selector must be rejected by SQL")
	}
	_, err = db.Exec(
		`INSERT INTO knowledge_input_actor_selectors(admission_id,caller_tenant_id,user_id,principal_type,principal_id) VALUES('actor-migration',3,'user','web_user','user')`,
	)
	require.NoError(t, err)
	_, err = db.Exec(
		`UPDATE knowledge_input_actor_selectors SET caller_tenant_id=7 WHERE admission_id='actor-migration'`,
	)
	require.Error(t, err, "caller workspace cannot be replaced by resource owner")
	_, err = db.Exec(`DELETE FROM knowledge_input_actor_selectors WHERE admission_id='actor-migration'`)
	require.Error(t, err, "original selector cannot be removed to bypass restoration")
}

func TestSQLiteKnowledgeActorMigrationThrough56AndUpgrade55(t *testing.T) {
	repoRoot := sqliteRepoRoot(t)
	for _, start := range []int{0, 55} {
		t.Run(fmt.Sprintf("from%d", start), func(t *testing.T) {
			dbPath := filepath.Join(t.TempDir(), "actor.db")
			if start != 0 {
				chdirAndRestore(t, copySQLiteMigrationsThrough(t, repoRoot, start))
				require.NoError(t, RunMigrationsWithOptions("sqlite3://unused", MigrationOptions{SQLiteDBPath: dbPath}))
			}
			chdirAndRestore(t, copySQLiteMigrationsThrough(t, repoRoot, 56))
			require.NoError(t, RunMigrationsWithOptions("sqlite3://unused", MigrationOptions{SQLiteDBPath: dbPath}))
			db := openSQLiteDB(t, dbPath)
			version, dirty := sqliteMigrationState(t, db)
			require.Equal(t, 56, version)
			require.False(t, dirty)
			checkKnowledgeActorMigration(t, db)
		})
	}
}

func TestPostgresKnowledgeActorMigrationThrough137AndUpgrade136(t *testing.T) {
	dsn := os.Getenv("WEKNORA_MIGRATION_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("set disposable PostgreSQL migration DSN")
	}
	db, err := sql.Open("postgres", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	repoRoot := sqliteRepoRoot(t)
	for _, start := range []uint{0, 136} {
		t.Run(fmt.Sprintf("from%d", start), func(t *testing.T) {
			schema := "knowledge_actor_" + strings.ReplaceAll(uuid.NewString(), "-", "")
			_, err := db.Exec("CREATE SCHEMA " + schema)
			require.NoError(t, err)
			t.Cleanup(func() { _, err := db.Exec("DROP SCHEMA " + schema + " CASCADE"); require.NoError(t, err) })
			isolatedDSN := postgresSchemaDSN(t, dsn, schema)
			m, err := migrate.New("file://"+filepath.Join(repoRoot, "migrations/versioned"), isolatedDSN)
			require.NoError(t, err)
			t.Cleanup(func() { _, _ = m.Close() })
			if start != 0 {
				require.NoError(t, m.Migrate(start))
			}
			require.NoError(t, m.Migrate(137))
			isolated, err := sql.Open("postgres", isolatedDSN)
			require.NoError(t, err)
			t.Cleanup(func() { _ = isolated.Close() })
			requirePostgresMigrationVersion(t, isolated, 137)
			checkKnowledgeActorMigration(t, isolated)
		})
	}
}
