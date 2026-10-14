package repository

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/require"
)

type nextcloudRevisionObservation struct {
	Revision             int64
	DesiredETag          string
	CandidateKnowledgeID string
	State                string
	LegacyHistoryUnknown bool
}

func nextcloudRevisionSQLiteDB(t *testing.T) *sql.DB {
	t.Helper()
	path := filepath.Join(t.TempDir(), "revisions-"+uuid.NewString()+".db")
	db, err := sql.Open("sqlite3", "file:"+path+"?_busy_timeout=5000&_journal_mode=WAL")
	require.NoError(t, err)
	db.SetMaxOpenConns(12)
	t.Cleanup(func() { _ = db.Close() })
	for _, name := range []string{
		"000031_nextcloud_source_versions.up.sql",
	} {
		script, err := os.ReadFile("../../../migrations/sqlite/" + name)
		require.NoError(t, err)
		_, err = db.Exec(string(script))
		require.NoError(t, err)
	}
	return db
}

func installNextcloudRevisionSQLite(t *testing.T, db *sql.DB) {
	t.Helper()
	script, err := os.ReadFile("../../../migrations/sqlite/000045_nextcloud_source_revisions.up.sql")
	require.NoError(t, err)
	_, err = db.Exec(string(script))
	require.NoError(t, err)
}

func nextcloudRevisionRows(t *testing.T, db *sql.DB, externalID string) []nextcloudRevisionObservation {
	t.Helper()
	rows, err := db.Query(`SELECT revision, desired_etag, candidate_knowledge_id, state,
		legacy_history_unknown FROM nextcloud_source_revisions
		WHERE tenant_id = 7 AND knowledge_base_id = 'kb' AND datasource_id = 'ds'
		AND external_id = ? ORDER BY revision`, externalID)
	require.NoError(t, err)
	defer func() { require.NoError(t, rows.Close()) }()
	var observed []nextcloudRevisionObservation
	for rows.Next() {
		var row nextcloudRevisionObservation
		require.NoError(t, rows.Scan(&row.Revision, &row.DesiredETag,
			&row.CandidateKnowledgeID, &row.State, &row.LegacyHistoryUnknown))
		observed = append(observed, row)
	}
	require.NoError(t, rows.Err())
	return observed
}

func TestSQLiteNextcloudRevisionUpgradeAndMutationLedger(t *testing.T) {
	db := nextcloudRevisionSQLiteDB(t)
	_, err := db.Exec(`INSERT INTO nextcloud_source_versions
		(tenant_id, knowledge_base_id, datasource_id, external_id,
		 desired_etag, candidate_knowledge_id, state)
		VALUES (7, 'kb', 'ds', 'legacy', 'old-etag', 'old-candidate', 'published')`)
	require.NoError(t, err)
	installNextcloudRevisionSQLite(t, db)
	initial := nextcloudRevisionRows(t, db, "legacy")
	require.Equal(t, []nextcloudRevisionObservation{{1, "old-etag", "old-candidate", "published", true}}, initial)

	_, err = db.Exec(`INSERT INTO nextcloud_source_versions
		(tenant_id, knowledge_base_id, datasource_id, external_id,
		 desired_etag, candidate_knowledge_id, state)
		VALUES (7, 'kb', 'ds', 'legacy', 'new-etag', 'new-candidate', 'staging')
		ON CONFLICT (tenant_id, knowledge_base_id, datasource_id, external_id)
		DO UPDATE SET desired_etag = excluded.desired_etag,
		candidate_knowledge_id = excluded.candidate_knowledge_id, state = excluded.state`)
	require.NoError(t, err)
	_, err = db.Exec(`UPDATE nextcloud_source_versions SET state = 'published'
		WHERE external_id = 'legacy'`)
	require.NoError(t, err)
	_, err = db.Exec(`UPDATE nextcloud_source_versions SET desired_etag = '',
		candidate_knowledge_id = '', state = 'tombstone' WHERE external_id = 'legacy'`)
	require.NoError(t, err)
	rows := nextcloudRevisionRows(t, db, "legacy")
	require.Equal(t, []nextcloudRevisionObservation{
		{1, "old-etag", "old-candidate", "published", true},
		{2, "new-etag", "new-candidate", "staging", true},
		{3, "new-etag", "new-candidate", "published", true},
		{4, "", "", "tombstone", true},
	}, rows)

	_, err = db.Exec(`INSERT INTO nextcloud_source_versions
		(tenant_id, knowledge_base_id, datasource_id, external_id,
		 desired_etag, candidate_knowledge_id, state)
		VALUES (7, 'kb', 'ds', 'new-source', 'etag-1', 'candidate-1', 'staging')`)
	require.NoError(t, err)
	require.Equal(t, []nextcloudRevisionObservation{
		{1, "etag-1", "candidate-1", "staging", false},
	}, nextcloudRevisionRows(t, db, "new-source"))

	for _, statement := range []string{
		`UPDATE nextcloud_source_revisions SET desired_etag = 'forged'`,
		`DELETE FROM nextcloud_source_revisions`,
		`DELETE FROM nextcloud_source_versions WHERE external_id = 'legacy'`,
		`UPDATE nextcloud_source_versions SET external_id = 'moved' WHERE external_id = 'legacy'`,
	} {
		_, err = db.Exec(statement)
		require.Error(t, err, statement)
	}
	require.Equal(t, rows, nextcloudRevisionRows(t, db, "legacy"))

	tx, err := db.Begin()
	require.NoError(t, err)
	_, err = tx.Exec(`UPDATE nextcloud_source_versions SET state = 'staging' WHERE external_id = 'legacy'`)
	require.NoError(t, err)
	require.NoError(t, tx.Rollback())
	require.Equal(t, rows, nextcloudRevisionRows(t, db, "legacy"),
		"source mutation and ledger append must roll back together")
	down, err := os.ReadFile("../../../migrations/sqlite/000045_nextcloud_source_revisions.down.sql")
	require.NoError(t, err)
	_, err = db.Exec(string(down))
	require.ErrorContains(t, err, "rollback_requires_audit_export")
	require.Equal(t, rows, nextcloudRevisionRows(t, db, "legacy"))
}

func TestSQLiteNextcloudRevisionConcurrentUpsertsStayMonotonic(t *testing.T) {
	db := nextcloudRevisionSQLiteDB(t)
	installNextcloudRevisionSQLite(t, db)
	const writers = 8
	var wg sync.WaitGroup
	errors := make(chan error, writers)
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for attempt := 0; attempt < 12; attempt++ {
				_, err := db.Exec(`INSERT INTO nextcloud_source_versions
					(tenant_id, knowledge_base_id, datasource_id, external_id,
					 desired_etag, candidate_knowledge_id, state)
					VALUES (7, 'kb', 'ds', 'concurrent', ?, ?, 'staging')
					ON CONFLICT (tenant_id, knowledge_base_id, datasource_id, external_id)
					DO UPDATE SET desired_etag = excluded.desired_etag,
					candidate_knowledge_id = excluded.candidate_knowledge_id,
					state = excluded.state`, fmt.Sprintf("etag-%d", i), fmt.Sprintf("candidate-%d", i))
				if err == nil {
					return
				}
				if !strings.Contains(err.Error(), "locked") && !strings.Contains(err.Error(), "busy") {
					errors <- err
					return
				}
				time.Sleep(time.Duration(attempt+1) * time.Millisecond)
			}
			errors <- fmt.Errorf("writer %d exhausted SQLite lock retries", i)
		}(i)
	}
	wg.Wait()
	close(errors)
	for err := range errors {
		require.NoError(t, err)
	}
	rows := nextcloudRevisionRows(t, db, "concurrent")
	require.Len(t, rows, writers)
	for i, row := range rows {
		require.Equal(t, int64(i+1), row.Revision)
		require.False(t, row.LegacyHistoryUnknown)
	}
	var desired, candidate, state string
	require.NoError(t, db.QueryRow(`SELECT desired_etag, candidate_knowledge_id, state
		FROM nextcloud_source_versions WHERE external_id = 'concurrent'`).
		Scan(&desired, &candidate, &state))
	require.Equal(t, rows[len(rows)-1].DesiredETag, desired)
	require.Equal(t, rows[len(rows)-1].CandidateKnowledgeID, candidate)
	require.Equal(t, rows[len(rows)-1].State, state)
}
