package database

import (
	"database/sql"
	"os"
	"testing"

	"github.com/golang-migrate/migrate/v4"
	"github.com/stretchr/testify/require"
)

// This test requires a disposable database: it deliberately rolls the schema
// back across the directory migration, then restores the latest version.
func TestPostgresDirectoryMigrationUpgradeAndRollback(t *testing.T) {
	dsn := os.Getenv("WEKNORA_MIGRATION_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("set WEKNORA_MIGRATION_TEST_POSTGRES_DSN to a disposable PostgreSQL database")
	}
	root := sqliteRepoRoot(t)
	chdirAndRestore(t, root)
	m, err := migrate.New("file://migrations/versioned", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { _, _ = m.Close() })
	latest := uint(latestVersionedMigration(t, root))
	t.Cleanup(func() {
		err := m.Migrate(latest)
		if err != migrate.ErrNoChange {
			require.NoError(t, err)
		}
	})
	err = m.Migrate(108)
	if err != migrate.ErrNoChange {
		require.NoError(t, err)
	}
	db, err := sql.Open("postgres", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	var tenantID int64
	require.NoError(t, db.QueryRow(`INSERT INTO tenants (name, business)
		VALUES ('directory-upgrade-sentinel', 'migration-test') RETURNING id`).Scan(&tenantID))
	t.Cleanup(func() { _, _ = db.Exec("DELETE FROM tenants WHERE id = $1", tenantID) })
	require.NoError(t, m.Migrate(109))
	requirePostgresDirectoryConfigVersionSchema(t, db)

	// Installing LDAP does not grant or restrict existing resources or members.
	var count int
	for _, table := range []string{"resource_access_policies", "resource_group_grants", "tenant_group_role_grants"} {
		require.NoError(t, db.QueryRow("SELECT count(*) FROM "+table).Scan(&count))
		require.Zero(t, count, table)
	}
	_, err = db.Exec(`INSERT INTO directories (id, name, protocol, tls_mode, base_dn,
		user_base_dn, group_base_dn, user_filter, group_filter, service_account_dn, password_ciphertext)
		VALUES ('migration-directory', 'Fixture', 'active_directory', 'ldaps', 'dc=test',
		'dc=test', 'dc=test', '(objectClass=user)', '(objectClass=group)', 'cn=reader,dc=test', '')`)
	require.NoError(t, err)
	var enabled bool
	require.NoError(t, db.QueryRow("SELECT enabled FROM directories WHERE id = 'migration-directory'").Scan(&enabled))
	require.False(t, enabled, "directory authentication must default off")
	_, err = db.Exec(`INSERT INTO directory_groups (id, directory_id, object_guid, dn, display_name)
		VALUES ('migration-group', 'migration-directory', 'stable-group-guid', 'cn=group,dc=test', 'Fixture')`)
	require.NoError(t, err)
	_, err = db.Exec(`INSERT INTO tenant_group_role_grants (tenant_id, directory_group_id, role)
		VALUES ($1, 'migration-group', 'owner')`, tenantID)
	require.Error(t, err, "AD groups cannot grant owner")
	_, err = db.Exec(`INSERT INTO resource_group_grants
		(tenant_id, resource_type, resource_id, directory_group_id, permission)
		VALUES ($1, 'knowledge_base', 'fixture-kb', 'migration-group', 'use')`, tenantID)
	require.Error(t, err, "agent permissions cannot be assigned to knowledge bases")
	_, err = db.Exec(`INSERT INTO resource_access_policies (tenant_id, resource_type, resource_id)
		VALUES ($1, 'knowledge_base', 'fixture-kb')`, tenantID)
	require.NoError(t, err)
	var mode string
	require.NoError(t, db.QueryRow("SELECT mode FROM resource_access_policies WHERE tenant_id = $1", tenantID).Scan(&mode))
	require.Equal(t, "inherit", mode)

	require.NoError(t, m.Migrate(108))
	var table sql.NullString
	require.NoError(t, db.QueryRow("SELECT to_regclass('public.directories')::text").Scan(&table))
	require.False(t, table.Valid, "rollback must remove the directory tables")
	require.NoError(t, db.QueryRow("SELECT count(*) FROM tenants WHERE id = $1", tenantID).Scan(&count))
	require.Equal(t, 1, count, "directory rollback must preserve pre-existing workspace data")
	require.NoError(t, m.Migrate(latest))
	requirePostgresDirectoryConfigVersionSchema(t, db)
}
