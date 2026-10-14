package repository

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/stretchr/testify/require"
)

func TestPostgresNextcloudSourceRevisionUpgradeAndIndexedFence(t *testing.T) {
	dsn := os.Getenv("WEKNORA_REVISION_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("set WEKNORA_REVISION_TEST_POSTGRES_DSN for disposable PostgreSQL")
	}
	admin, err := sql.Open("pgx", dsn)
	require.NoError(t, err)
	defer func() { require.NoError(t, admin.Close()) }()
	schema := "source_revisions_" + strings.ReplaceAll(uuid.NewString()[:8], "-", "")
	_, err = admin.Exec(`CREATE SCHEMA "` + schema + `"`)
	require.NoError(t, err)
	defer func() {
		_, cleanupErr := admin.Exec(`DROP SCHEMA "` + schema + `" CASCADE`)
		require.NoError(t, cleanupErr)
	}()
	u, err := url.Parse(dsn)
	require.NoError(t, err)
	q := u.Query()
	q.Set("search_path", schema)
	u.RawQuery = q.Encode()
	db, err := sql.Open("pgx", u.String())
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()
	db.SetMaxOpenConns(12)
	_, err = db.Exec(`CREATE TABLE nextcloud_source_pairings (
		operation_id TEXT PRIMARY KEY, knowledge_base_id TEXT NOT NULL,
		datasource_id TEXT NOT NULL);
		CREATE TABLE knowledge_bases (id TEXT PRIMARY KEY);
		CREATE TABLE data_sources (id TEXT PRIMARY KEY, status TEXT NOT NULL);
		CREATE TABLE knowledges (knowledge_base_id TEXT);
		CREATE TABLE chunks (knowledge_base_id TEXT);
		CREATE TABLE sync_logs (data_source_id TEXT);
		CREATE TABLE nextcloud_event_connections (datasource_id TEXT);
		INSERT INTO nextcloud_source_pairings VALUES ('pair', 'kb', 'ds');
		INSERT INTO knowledge_bases VALUES ('kb');
		INSERT INTO data_sources VALUES ('ds', 'active')`)
	require.NoError(t, err)
	for _, number := range []string{"000112_nextcloud_source_versions.up.sql"} {
		script, readErr := os.ReadFile("../../../migrations/versioned/" + number)
		require.NoError(t, readErr)
		_, err = db.Exec(string(script))
		require.NoError(t, err, number)
	}
	_, err = db.Exec(`INSERT INTO nextcloud_source_versions
		(tenant_id, knowledge_base_id, datasource_id, external_id,
		 desired_etag, candidate_knowledge_id, state)
		VALUES (7, 'kb', 'ds', 'legacy', 'old-etag', 'old-candidate', 'published')`)
	require.NoError(t, err)
	// Run the migration chain on the same disposable schema; migration 124
	// invalidates virgin proofs, and 125 installs the indexed-write fence.
	for _, number := range []string{
		"000123_nextcloud_empty_decommission.up.sql",
		"000124_nextcloud_virgin_rebuild_guard.up.sql",
		"000125_nextcloud_indexed_withdrawal.up.sql",
	} {
		script, readErr := os.ReadFile("../../../migrations/versioned/" + number)
		require.NoError(t, readErr)
		_, err = db.Exec(string(script))
		require.NoError(t, err, number)
	}
	// A writer that started before the migration must be in its backfill;
	// a writer queued after the migration lock must instead hit its trigger.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	inflight, err := db.Begin()
	require.NoError(t, err)
	var migrationConn, queuedConn *sql.Conn
	defer func() {
		// A failed assertion can leave both operations waiting on inflight.
		// Release that row/table lock before closing their connections.
		_ = inflight.Rollback()
		if queuedConn != nil {
			_ = queuedConn.Close()
		}
		if migrationConn != nil {
			_ = migrationConn.Close()
		}
	}()
	_, err = inflight.Exec(`UPDATE nextcloud_source_versions
		SET desired_etag = 'pre-lock-etag', candidate_knowledge_id = 'pre-lock-candidate'
		WHERE external_id = 'legacy'`)
	require.NoError(t, err)
	migrationConn, err = db.Conn(ctx)
	require.NoError(t, err)
	var migrationPID int
	require.NoError(t, migrationConn.QueryRowContext(ctx, `SELECT pg_backend_pid()`).Scan(&migrationPID))
	script, err := os.ReadFile("../../../migrations/versioned/000126_nextcloud_source_revisions.up.sql")
	require.NoError(t, err)
	migrationDone := make(chan error, 1)
	go func() {
		_, migrationErr := migrationConn.ExecContext(ctx, string(script))
		migrationDone <- migrationErr
	}()
	waitForNextcloudPGMigrationLock(t, db, migrationPID)
	queuedConn, err = db.Conn(ctx)
	require.NoError(t, err)
	var queuedPID int
	require.NoError(t, queuedConn.QueryRowContext(ctx, `SELECT pg_backend_pid()`).Scan(&queuedPID))
	queuedDone := make(chan error, 1)
	go func() {
		_, writeErr := queuedConn.ExecContext(ctx, `UPDATE nextcloud_source_versions
			SET desired_etag = 'post-lock-etag', candidate_knowledge_id = 'post-lock-candidate'
			WHERE external_id = 'legacy'`)
		queuedDone <- writeErr
	}()
	waitForNextcloudPGMigrationLock(t, db, queuedPID)
	require.NoError(t, inflight.Commit())
	select {
	case err = <-migrationDone:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("source revision migration did not acquire its queued table lock")
	}
	select {
	case err = <-queuedDone:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("source writer did not resume after migration")
	}
	var revision int64
	var unknown bool
	var etag, candidate, state string
	require.NoError(t, db.QueryRow(`SELECT revision, desired_etag, candidate_knowledge_id,
		state, legacy_history_unknown FROM nextcloud_source_revisions
		WHERE external_id = 'legacy' AND revision = 1`).Scan(&revision, &etag, &candidate, &state, &unknown))
	require.Equal(t, int64(1), revision)
	require.Equal(t, "pre-lock-etag", etag)
	require.Equal(t, "pre-lock-candidate", candidate)
	require.Equal(t, "published", state)
	require.True(t, unknown)
	require.NoError(t, db.QueryRow(`SELECT revision, desired_etag, candidate_knowledge_id,
		legacy_history_unknown FROM nextcloud_source_revisions
		WHERE external_id = 'legacy' ORDER BY revision DESC LIMIT 1`).
		Scan(&revision, &etag, &candidate, &unknown))
	require.Equal(t, int64(2), revision)
	require.Equal(t, "post-lock-etag", etag)
	require.Equal(t, "post-lock-candidate", candidate)
	require.True(t, unknown)
	_, err = db.Exec(`UPDATE nextcloud_source_versions SET desired_etag = 'new-etag',
		candidate_knowledge_id = 'new-candidate', state = 'staging' WHERE external_id = 'legacy'`)
	require.NoError(t, err)
	require.NoError(t, db.QueryRow(`SELECT revision, legacy_history_unknown
		FROM nextcloud_source_revisions WHERE external_id = 'legacy'
		ORDER BY revision DESC LIMIT 1`).Scan(&revision, &unknown))
	require.Equal(t, int64(3), revision)
	require.True(t, unknown)

	const writers = 8
	var wg sync.WaitGroup
	errors := make(chan error, writers)
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, writeErr := db.Exec(`INSERT INTO nextcloud_source_versions
				(tenant_id, knowledge_base_id, datasource_id, external_id,
				 desired_etag, candidate_knowledge_id, state)
				VALUES (7, 'kb', 'ds', 'concurrent', $1, $2, 'staging')
				ON CONFLICT (tenant_id, knowledge_base_id, datasource_id, external_id)
				DO UPDATE SET desired_etag = excluded.desired_etag,
				candidate_knowledge_id = excluded.candidate_knowledge_id,
				state = excluded.state`, fmt.Sprintf("etag-%d", i), fmt.Sprintf("candidate-%d", i))
			errors <- writeErr
		}(i)
	}
	wg.Wait()
	close(errors)
	for err := range errors {
		require.NoError(t, err)
	}
	var count, maxRevision int64
	require.NoError(t, db.QueryRow(`SELECT COUNT(*), MAX(revision)
		FROM nextcloud_source_revisions WHERE external_id = 'concurrent'`).Scan(&count, &maxRevision))
	require.Equal(t, int64(writers), count)
	require.Equal(t, int64(writers), maxRevision)
	require.NoError(t, db.QueryRow(`SELECT desired_etag, candidate_knowledge_id, state
		FROM nextcloud_source_revisions WHERE external_id = 'concurrent'
		ORDER BY revision DESC LIMIT 1`).Scan(&etag, &candidate, &state))
	var currentETag, currentCandidate, currentState string
	require.NoError(t, db.QueryRow(`SELECT desired_etag, candidate_knowledge_id, state
		FROM nextcloud_source_versions WHERE external_id = 'concurrent'`).
		Scan(&currentETag, &currentCandidate, &currentState))
	require.Equal(t, []string{etag, candidate, state},
		[]string{currentETag, currentCandidate, currentState})

	_, err = db.Exec(`UPDATE nextcloud_source_revisions SET desired_etag = 'forged'
		WHERE external_id = 'legacy'`)
	require.Error(t, err)
	_, err = db.Exec(`TRUNCATE nextcloud_source_revisions`)
	require.ErrorContains(t, err, "nextcloud_source_revision_immutable")
	_, err = db.Exec(`TRUNCATE nextcloud_source_versions`)
	require.ErrorContains(t, err, "nextcloud_source_revision_immutable")
	_, err = db.Exec(`DELETE FROM nextcloud_source_versions WHERE external_id = 'legacy'`)
	require.Error(t, err)
	_, err = db.Exec(`UPDATE nextcloud_source_versions SET external_id = 'moved'
		WHERE external_id = 'legacy'`)
	require.Error(t, err)

	tx, err := db.Begin()
	require.NoError(t, err)
	_, err = tx.Exec(`UPDATE nextcloud_source_versions SET state = 'published'
		WHERE external_id = 'legacy'`)
	require.NoError(t, err)
	require.NoError(t, tx.Rollback())
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM nextcloud_source_revisions
		WHERE external_id = 'legacy'`).Scan(&count))
	require.Equal(t, int64(3), count)

	_, err = db.Exec(`INSERT INTO nextcloud_indexed_withdrawals
		(operation_id, pair_operation_id, tenant_id, knowledge_base_id,
		 datasource_id, nextcloud_instance_id, binding_id, publication_epoch, key_id)
		VALUES ('withdrawal', 'pair', 7, 'kb', 'ds', 'instance', 'binding', 0, 'key')`)
	require.NoError(t, err)
	_, err = db.Exec(`UPDATE nextcloud_source_versions SET state = 'published'
		WHERE external_id = 'legacy'`)
	require.ErrorContains(t, err, "nextcloud_indexed_source_withdrawn")
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM nextcloud_source_revisions
		WHERE external_id = 'legacy'`).Scan(&count))
	require.Equal(t, int64(3), count,
		"the indexed write fence rejects the source mutation before ledger append")
	down, err := os.ReadFile("../../../migrations/versioned/000126_nextcloud_source_revisions.down.sql")
	require.NoError(t, err)
	_, err = db.Exec(string(down))
	require.ErrorContains(t, err, "nextcloud_source_revision_rollback_requires_audit_export")
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM nextcloud_source_revisions
		WHERE external_id = 'legacy'`).Scan(&count))
	require.Equal(t, int64(3), count)
}

func waitForNextcloudPGMigrationLock(t *testing.T, db *sql.DB, pid int) {
	t.Helper()
	require.Eventually(t, func() bool {
		var event sql.NullString
		err := db.QueryRow(`SELECT wait_event_type FROM pg_stat_activity WHERE pid = $1`, pid).
			Scan(&event)
		return err == nil && event.Valid && event.String == "Lock"
	}, 5*time.Second, 10*time.Millisecond,
		"backend %d did not wait for the migration/source table lock", pid)
}
