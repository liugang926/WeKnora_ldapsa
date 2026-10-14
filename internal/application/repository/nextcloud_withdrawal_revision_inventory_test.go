package repository

import (
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/require"
)

func nextcloudWithdrawalInventorySQLiteDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite3", "file:"+filepath.Join(t.TempDir(), "withdrawal.db")+"?_foreign_keys=on")
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	_, err = db.Exec(`CREATE TABLE nextcloud_source_pairings (operation_id TEXT PRIMARY KEY);
		CREATE TABLE nextcloud_source_decommissions (pair_operation_id TEXT NOT NULL);
		CREATE TABLE data_sources (id TEXT PRIMARY KEY, status TEXT NOT NULL);
		INSERT INTO nextcloud_source_pairings (operation_id) VALUES ('pair');
		INSERT INTO data_sources (id, status) VALUES ('ds', 'paused')`)
	require.NoError(t, err)
	script, err := os.ReadFile("../../../migrations/sqlite/000044_nextcloud_indexed_withdrawal.up.sql")
	require.NoError(t, err)
	_, err = db.Exec(string(script))
	require.NoError(t, err)
	_, err = db.Exec(`INSERT INTO nextcloud_indexed_withdrawals
		(operation_id, pair_operation_id, tenant_id, knowledge_base_id,
		 datasource_id, nextcloud_instance_id, binding_id, publication_epoch, key_id)
		VALUES ('withdrawal', 'pair', 7, 'kb', 'ds', 'instance', 'binding', 1, 'key');
		INSERT INTO nextcloud_indexed_withdrawal_items
		(operation_id, kind, object_ref, knowledge_id)
		VALUES ('withdrawal', 'source_version', 'source', 'current')`)
	require.NoError(t, err)
	return db
}

func TestSQLiteNextcloudWithdrawalRevisionInventoryMigration(t *testing.T) {
	for _, keepRevision := range []bool{false, true} {
		t.Run(map[bool]string{false: "reversible_empty", true: "retained_revision"}[keepRevision],
			func(t *testing.T) {
				db := nextcloudWithdrawalInventorySQLiteDB(t)
				up, err := os.ReadFile(("../../../migrations/sqlite/000048_nextcloud_withdrawal_" +
					"revision_inventory.up.sql"))
				require.NoError(t, err)
				_, err = db.Exec(string(up))
				require.NoError(t, err)
				var count int
				require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM nextcloud_indexed_withdrawal_items
					WHERE operation_id = 'withdrawal' AND kind = 'source_version'
					AND object_ref = 'source' AND knowledge_id = 'current'`).Scan(&count))
				require.Equal(t, 1, count, "the old inventory must survive the table rebuild")
				_, err = db.Exec(`UPDATE nextcloud_indexed_withdrawal_items SET knowledge_id = 'forged'
					WHERE operation_id = 'withdrawal'`)
				require.ErrorContains(t, err, "nextcloud_indexed_withdrawal_item_immutable")
				if keepRevision {
					_, err = db.Exec(`INSERT INTO nextcloud_indexed_withdrawal_items
						(operation_id, kind, object_ref, knowledge_id)
						VALUES ('withdrawal', 'source_revision', '["source",2]', 'old-copy')`)
					require.NoError(t, err)
				}
				down, err := os.ReadFile(("../../../migrations/sqlite/000048_nextcloud_withdrawal_" +
					"revision_inventory.down.sql"))
				require.NoError(t, err)
				_, err = db.Exec(string(down))
				if keepRevision {
					require.Error(t, err, "observed revisions must block a destructive downgrade")
					require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM nextcloud_indexed_withdrawal_items
						WHERE kind = 'source_revision'`).Scan(&count))
					require.Equal(t, 1, count)
					return
				}
				require.NoError(t, err)
				_, err = db.Exec(`INSERT INTO nextcloud_indexed_withdrawal_items
					(operation_id, kind, object_ref, knowledge_id)
					VALUES ('withdrawal', 'source_revision', '["source",2]', 'old-copy')`)
				require.Error(t, err, "the old schema must reject the new inventory kind")
				require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM nextcloud_indexed_withdrawal_items
					WHERE kind = 'source_version'`).Scan(&count))
				require.Equal(t, 1, count)
			})
	}
}
