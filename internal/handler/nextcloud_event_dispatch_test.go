package handler

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/application/service"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/gin-gonic/gin"
	"github.com/hibiken/asynq"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func setupNextcloudDispatchTest(t *testing.T, usePostgres bool) (*gorm.DB, *gin.Engine, string) {
	t.Helper()
	var db *gorm.DB
	var router *gin.Engine
	var secret string
	if usePostgres {
		dsn := os.Getenv("NEXTCLOUD_EVENT_TEST_POSTGRES_DSN")
		if dsn == "" {
			t.Skip("set NEXTCLOUD_EVENT_TEST_POSTGRES_DSN for isolated PostgreSQL dispatch test")
		}
		var err error
		db, err = gorm.Open(postgres.Open(dsn), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
		require.NoError(t, err)
		sqlDB, err := db.DB()
		require.NoError(t, err)
		sqlDB.SetMaxOpenConns(1)
		t.Cleanup(func() { _ = sqlDB.Close() })
		random := make([]byte, 6)
		_, err = rand.Read(random)
		require.NoError(t, err)
		schema := "nextcloud_dispatch_test_" + hex.EncodeToString(random)
		require.NoError(t, db.Exec("CREATE SCHEMA "+schema).Error)
		t.Cleanup(func() { _ = db.Exec("DROP SCHEMA " + schema + " CASCADE").Error })
		require.NoError(t, db.Exec("SET search_path TO "+schema).Error)
		db, router, secret = prepareNextcloudEventHTTPTest(t, db,
			"../../migrations/versioned/000113_nextcloud_event_inbox.up.sql")
	} else {
		db, router, secret = setupNextcloudEventHTTPTest(t)
	}
	createNextcloudEventSyncLogFixture(t, db)
	applyNextcloudEventDispatchMigration(t, db)
	cursorColumnType := "TEXT"
	if usePostgres {
		cursorColumnType = "JSONB"
	}
	require.NoError(t, db.Exec("ALTER TABLE data_sources ADD COLUMN last_sync_cursor "+cursorColumnType).Error)
	require.NoError(t, db.Exec(`CREATE TABLE nextcloud_source_versions (
		tenant_id BIGINT NOT NULL, knowledge_base_id TEXT NOT NULL,
		datasource_id TEXT NOT NULL, external_id TEXT NOT NULL,
		desired_etag TEXT NOT NULL DEFAULT '', candidate_knowledge_id TEXT NOT NULL DEFAULT '',
		state TEXT NOT NULL
	)`).Error)
	require.NoError(t, db.Exec(`CREATE TABLE knowledges (
		id TEXT PRIMARY KEY, tenant_id BIGINT NOT NULL,
		knowledge_base_id TEXT NOT NULL, channel TEXT NOT NULL,
		parse_status TEXT NOT NULL DEFAULT 'pending',
		enable_status TEXT NOT NULL DEFAULT 'disabled',
		file_name TEXT NOT NULL DEFAULT '', deleted_at TIMESTAMP,
		metadata `+cursorColumnType+` NOT NULL
	)`).Error)
	return db, router, secret
}

func nextcloudEventChangeCursor(id string) string {
	payload, _ := json.Marshal([]any{1, testEventBinding, id, strings.Repeat("0", 64)})
	return base64.RawURLEncoding.EncodeToString(payload)
}

func receiveDispatchHint(t *testing.T, router *gin.Engine, secret, after, event, nonce string) {
	t.Helper()
	batch := sampleNextcloudEventBatch(after, event)
	etag, relative := "v1", "Notes.md"
	if event == "43" {
		// This synthetic event deliberately leaves the importable manifest
		// empty; its file is outside the connector's supported formats.
		relative = "ignored.bin"
	}
	absolute := "/Published/" + relative
	batch.Events[0].ETag, batch.Events[0].Path = &etag, &absolute
	batch.Events[0].RelativePath = &relative
	response := postNextcloudEvent(router, signedNextcloudEventRequest(t, secret,
		batch, strings.Repeat(nonce, 32)))
	require.Equal(t, http.StatusAccepted, response.Code, response.Body.String())
}

func testNextcloudEventDispatchTransaction(t *testing.T, usePostgres bool) {
	t.Helper()
	db, router, secret := setupNextcloudDispatchTest(t, usePostgres)
	repo := repository.NewNextcloudEventInboxRepository(db)
	ctx := context.Background()
	now := time.Now().UTC()
	receiveDispatchHint(t, router, secret, "0", "41", "a")
	ids, err := repo.DispatchCandidates(ctx, now)
	require.NoError(t, err)
	require.Contains(t, ids, testEventConnection)
	first, err := repo.ClaimDispatch(ctx, testEventConnection, now)
	require.NoError(t, err)
	require.NotNil(t, first)
	require.Equal(t, int64(41), first.TargetEventID)
	duplicate, err := repo.ClaimDispatch(ctx, testEventConnection, now)
	require.NoError(t, err)
	require.Nil(t, duplicate)
	status, err := repo.ConnectionStatus(ctx, 7, "ds-synthetic")
	require.NoError(t, err)
	require.Equal(t, "41", status.ReceivedThroughEventID)
	require.Equal(t, "0", status.DispatchedThroughEventID)
	require.Equal(t, "0", status.AppliedThroughEventID)
	require.Equal(t, int64(1), status.BacklogCount)
	require.Equal(t, int64(1), status.UndispatchedCount)

	require.NoError(t, repo.FailDispatch(ctx, *first, "queue_unavailable", now))
	require.ErrorIs(t, repo.FinishDispatch(ctx, *first, "old-log", now),
		repository.ErrNextcloudEventConflict)
	status, err = repo.ConnectionStatus(ctx, 7, "ds-synthetic")
	require.NoError(t, err)
	require.Equal(t, "retry", status.DispatchState)
	require.Equal(t, "queue_unavailable", status.LastErrorCode)
	beforeBackoff, err := repo.ClaimDispatch(ctx, testEventConnection, now.Add(30*time.Second))
	require.NoError(t, err)
	require.Nil(t, beforeBackoff)

	retry, err := repo.ClaimDispatch(ctx, testEventConnection, now.Add(2*time.Minute))
	require.NoError(t, err)
	require.NotNil(t, retry)
	require.Equal(t, int64(41), retry.TargetEventID)
	require.NotEqual(t, first.LeaseToken, retry.LeaseToken)
	require.NoError(t, repo.PrepareDispatch(ctx, *retry, "log-41", now.Add(2*time.Minute)))
	require.NoError(t, db.Exec("INSERT INTO sync_logs (id, data_source_id, status, started_at) "+
		"VALUES (?, 'ds-synthetic', ?, ?)",
		"log-41", types.SyncLogStatusRunning, now).Error)
	require.NoError(t, repo.FinishDispatch(ctx, *retry, "log-41", now.Add(2*time.Minute)))
	require.ErrorIs(t, repo.FinishDispatch(ctx, *retry, "log-41", now.Add(2*time.Minute)),
		repository.ErrNextcloudEventConflict)
	status, err = repo.ConnectionStatus(ctx, 7, "ds-synthetic")
	require.NoError(t, err)
	require.Equal(t, "41", status.DispatchedThroughEventID)
	require.Equal(t, "0", status.AppliedThroughEventID)
	require.Equal(t, int64(1), status.BacklogCount)
	require.Zero(t, status.UndispatchedCount)
	require.Equal(t, "queued", status.DispatchState)
	var inboxState string
	require.NoError(t, db.Raw(`SELECT state FROM nextcloud_event_inbox
		WHERE connection_id = ? AND event_id = 41`, testEventConnection).Scan(&inboxState).Error)
	require.Equal(t, "dispatched", inboxState)

	// A partial sync is retryable, even when the task itself returned nil.
	require.NoError(t, db.Exec(`UPDATE sync_logs SET status = ? WHERE id = ?`,
		types.SyncLogStatusPartial, "log-41").Error)
	failed, err := repo.ClaimDispatch(ctx, testEventConnection, now.Add(4*time.Minute))
	require.NoError(t, err)
	require.Nil(t, failed)
	status, err = repo.ConnectionStatus(ctx, 7, "ds-synthetic")
	require.NoError(t, err)
	require.Equal(t, "retry", status.DispatchState)
	require.Equal(t, "sync_not_successful", status.LastErrorCode)
	require.Equal(t, "0", status.AppliedThroughEventID)

	receiveDispatchHint(t, router, secret, "41", "43", "b")
	retry, err = repo.ClaimDispatch(ctx, testEventConnection, now.Add(6*time.Minute))
	require.NoError(t, err)
	require.NotNil(t, retry)
	require.Equal(t, int64(43), retry.TargetEventID)
	require.NoError(t, repo.PrepareDispatch(ctx, *retry, "log-43", now.Add(6*time.Minute)))
	cursor, err := json.Marshal(map[string]any{
		"last_sync_time": now.Add(6 * time.Minute),
		"connector_cursor": map[string]any{
			"instance_id":       testEventInstance,
			"files":             map[string]any{testEventBinding: map[string]any{}},
			"last_reconcile_at": now.Add(6 * time.Minute).Unix(),
			"missing":           map[string]any{},
			"changes":           map[string]any{testEventBinding: nextcloudEventChangeCursor("43")},
		},
	})
	require.NoError(t, err)
	require.NoError(t, db.Exec(`UPDATE data_sources SET last_sync_cursor = ? WHERE id = ?`,
		string(cursor), "ds-synthetic").Error)
	require.NoError(t, db.Exec("INSERT INTO sync_logs (id, data_source_id, status, started_at, "+
		"finished_at) VALUES (?, 'ds-synthetic', ?, ?, ?)",
		"log-43", types.SyncLogStatusSuccess, now, now).Error)
	require.NoError(t, repo.FinishDispatch(ctx, *retry, "log-43", now.Add(6*time.Minute)))
	status, err = repo.ConnectionStatus(ctx, 7, "ds-synthetic")
	require.NoError(t, err)
	require.Equal(t, "43", status.ReceivedThroughEventID)
	require.Equal(t, "43", status.DispatchedThroughEventID)
	require.Equal(t, "0", status.AppliedThroughEventID)
	require.Equal(t, int64(2), status.BacklogCount)
	require.Zero(t, status.UndispatchedCount)
	finished, err := repo.ClaimDispatch(ctx, testEventConnection, now.Add(8*time.Minute))
	require.NoError(t, err)
	require.Nil(t, finished)
	status, err = repo.ConnectionStatus(ctx, 7, "ds-synthetic")
	require.NoError(t, err)
	require.Equal(t, "idle", status.DispatchState)
	require.Equal(t, "43", status.AppliedThroughEventID)
	require.Zero(t, status.BacklogCount)
}

func TestNextcloudEventDispatchSQLiteTransaction(t *testing.T) {
	testNextcloudEventDispatchTransaction(t, false)
}

func TestNextcloudEventFailedSyncRemainsRetryable(t *testing.T) {
	db, router, secret := setupNextcloudDispatchTest(t, false)
	repo := repository.NewNextcloudEventInboxRepository(db)
	ctx := context.Background()
	now := time.Now().UTC()
	receiveDispatchHint(t, router, secret, "0", "41", "a")
	claim, err := repo.ClaimDispatch(ctx, testEventConnection, now)
	require.NoError(t, err)
	require.NotNil(t, claim)
	require.NoError(t, repo.PrepareDispatch(ctx, *claim, "stale-content-log", now))
	require.NoError(t, db.Exec(`INSERT INTO sync_logs (id, data_source_id, status, started_at, finished_at)
		VALUES ('stale-content-log', 'ds-synthetic', 'failed', ?, ?)`, now, now).Error)
	require.NoError(t, repo.FinishDispatch(ctx, *claim, "stale-content-log", now))

	first, err := repo.ClaimDispatch(ctx, testEventConnection, now.Add(6*time.Second))
	require.NoError(t, err)
	require.Nil(t, first)
	status, err := repo.ConnectionStatus(ctx, 7, "ds-synthetic")
	require.NoError(t, err)
	require.Equal(t, "retry", status.DispatchState)
	require.Equal(t, "sync_not_successful", status.LastErrorCode)
	require.Equal(t, "0", status.AppliedThroughEventID)

	retry, err := repo.ClaimDispatch(ctx, testEventConnection, now.Add(2*time.Minute))
	require.NoError(t, err)
	require.NotNil(t, retry)
	require.Equal(t, int64(41), retry.TargetEventID)
}

func TestNextcloudEventDispatchPostgresTransaction(t *testing.T) {
	testNextcloudEventDispatchTransaction(t, true)
}

func testNextcloudEventAppliedAfterParse(t *testing.T, usePostgres bool) {
	t.Helper()
	db, router, secret := setupNextcloudDispatchTest(t, usePostgres)
	repo := repository.NewNextcloudEventInboxRepository(db)
	ctx := context.Background()
	now := time.Now().UTC()
	receiveDispatchHint(t, router, secret, "0", "41", "a")
	claim, err := repo.ClaimDispatch(ctx, testEventConnection, now)
	require.NoError(t, err)
	require.NotNil(t, claim)
	require.NoError(t, repo.PrepareDispatch(ctx, *claim, "parse-log", now))
	require.NoError(t, db.Exec(`INSERT INTO sync_logs (id, data_source_id, status, started_at, finished_at)
		VALUES ('parse-log', 'ds-synthetic', 'success', ?, ?)`, now, now).Error)
	require.NoError(t, repo.FinishDispatch(ctx, *claim, "parse-log", now))
	cursor, err := json.Marshal(map[string]any{
		"last_sync_time": now.Add(time.Minute),
		"connector_cursor": map[string]any{
			"instance_id": testEventInstance,
			"files": map[string]any{testEventBinding: map[string]any{
				"41": map[string]any{"etag": "v1", "name": "Notes.md", "path": "Notes.md"},
			}},
			"last_reconcile_at": now.Add(time.Minute).Unix(),
			"changes":           map[string]any{testEventBinding: nextcloudEventChangeCursor("41")},
		},
	})
	require.NoError(t, err)
	require.NoError(t, db.Exec(`UPDATE data_sources SET last_sync_cursor = ? WHERE id = 'ds-synthetic'`,
		string(cursor)).Error)
	require.NoError(t, db.Exec("INSERT INTO nextcloud_source_versions\n\t\t(tenant_id, "+
		"knowledge_base_id, datasource_id, external_id, desired_etag, "+
		"candidate_knowledge_id, state)\n\t\tVALUES (7, 'kb-synthetic', "+
		"'ds-synthetic', 'nextcloud:instance_synthetic:41', 'v1', "+
		"'candidate-41', 'staging')").Error)
	metadata := `{"datasource_id":"ds-synthetic","external_id":"nextcloud:instance_synthetic:41",` +
		`"source_resource_id":"binding_synthetic","nextcloud_instance_id":"instance_synthetic",` +
		`"nextcloud_binding_id":"binding_synthetic","nextcloud_file_id":"41",` +
		`"nextcloud_path":"Notes.md","nextcloud_etag":""}`
	require.NoError(t, db.Exec(`INSERT INTO knowledges
		(id, tenant_id, knowledge_base_id, channel, parse_status, enable_status, file_name, metadata)
		VALUES ('candidate-41', 7, 'kb-synthetic', 'nextcloud', 'processing', 'enabled', 'Notes.md', ?)`,
		metadata).Error)
	waiting, err := repo.ClaimDispatch(ctx, testEventConnection, now.Add(2*time.Minute))
	require.NoError(t, err)
	require.Nil(t, waiting)
	status, err := repo.ConnectionStatus(ctx, 7, "ds-synthetic")
	require.NoError(t, err)
	require.Equal(t, "queued", status.DispatchState)
	require.Equal(t, "publication_pending", status.LastErrorCode)
	require.Equal(t, "0", status.AppliedThroughEventID)

	metadata = strings.Replace(metadata, `"nextcloud_etag":""`, `"nextcloud_etag":"v1"`, 1)
	require.NoError(t, db.Exec(`UPDATE knowledges SET parse_status = 'completed', metadata = ?
		WHERE id = 'candidate-41'`, metadata).Error)
	require.NoError(t, db.Exec(`UPDATE nextcloud_source_versions SET state = 'published'
		WHERE candidate_knowledge_id = 'candidate-41'`).Error)
	applied, err := repo.ClaimDispatch(ctx, testEventConnection, now.Add(4*time.Minute))
	require.NoError(t, err)
	require.Nil(t, applied)
	status, err = repo.ConnectionStatus(ctx, 7, "ds-synthetic")
	require.NoError(t, err)
	require.Equal(t, "idle", status.DispatchState)
	require.Equal(t, "41", status.AppliedThroughEventID)
	require.Zero(t, status.BacklogCount)
	var state string
	require.NoError(t, db.Raw(`SELECT state FROM nextcloud_event_inbox
		WHERE connection_id = ? AND event_id = 41`, testEventConnection).Scan(&state).Error)
	require.Equal(t, "applied", state)
	_, err = repo.ClaimDispatch(ctx, testEventConnection, now.Add(5*time.Minute))
	require.NoError(t, err)
}

func TestNextcloudEventAppliedAfterParseSQLite(t *testing.T) {
	testNextcloudEventAppliedAfterParse(t, false)
}

func TestNextcloudEventAppliedAfterParsePostgres(t *testing.T) {
	testNextcloudEventAppliedAfterParse(t, true)
}

func TestNextcloudEventAppliedRequiresSourceCursorCoveringReceipt(t *testing.T) {
	db, router, secret := setupNextcloudDispatchTest(t, false)
	repo := repository.NewNextcloudEventInboxRepository(db)
	ctx := context.Background()
	now := time.Now().UTC()
	receiveDispatchHint(t, router, secret, "0", "41", "7")
	claim, err := repo.ClaimDispatch(ctx, testEventConnection, now)
	require.NoError(t, err)
	require.NotNil(t, claim)
	require.NoError(t, repo.PrepareDispatch(ctx, *claim, "cursor-behind", now))
	require.NoError(t, db.Exec(`INSERT INTO sync_logs (id, data_source_id, status, started_at, finished_at)
		VALUES ('cursor-behind', 'ds-synthetic', 'success', ?, ?)`, now, now).Error)
	require.NoError(t, repo.FinishDispatch(ctx, *claim, "cursor-behind", now))
	cursor, err := json.Marshal(map[string]any{
		"last_sync_time": now.Add(time.Minute),
		"connector_cursor": map[string]any{
			"instance_id":       testEventInstance,
			"files":             map[string]any{testEventBinding: map[string]any{}},
			"last_reconcile_at": now.Add(time.Minute).Unix(),
			"changes":           map[string]any{testEventBinding: nextcloudEventChangeCursor("40")},
		},
	})
	require.NoError(t, err)
	require.NoError(t, db.Exec(`UPDATE data_sources SET last_sync_cursor = ? WHERE id = 'ds-synthetic'`,
		string(cursor)).Error)
	_, err = repo.ClaimDispatch(ctx, testEventConnection, now.Add(2*time.Minute))
	require.NoError(t, err)
	status, err := repo.ConnectionStatus(ctx, 7, "ds-synthetic")
	require.NoError(t, err)
	require.Equal(t, "0", status.AppliedThroughEventID)
	require.Equal(t, "retry", status.DispatchState)
	require.Equal(t, "publication_unproven", status.LastErrorCode)
}

func TestNextcloudEventDispatchRejectsLostOrMismatchedInventory(t *testing.T) {
	for _, scenario := range []string{
		"legacy_knowledge_without_cursor", "version_without_cursor",
		"wrong_instance", "missing_binding", "truncated_inventory", "valid_inventory",
	} {
		t.Run(scenario, func(t *testing.T) {
			db, router, secret := setupNextcloudDispatchTest(t, false)
			receiveDispatchHint(t, router, secret, "0", "41", "a")
			if scenario == "legacy_knowledge_without_cursor" {
				require.NoError(t, db.Exec(`INSERT INTO knowledges
					(id, tenant_id, knowledge_base_id, channel, metadata)
					VALUES ('old-knowledge', 7, 'kb-synthetic', 'nextcloud', ?)`,
					`{"datasource_id":"ds-synthetic","external_id":"nextcloud:instance_synthetic:41"}`).Error)
			} else {
				if scenario == "version_without_cursor" || scenario == "truncated_inventory" ||
					scenario == "valid_inventory" {
					require.NoError(t, db.Exec("INSERT INTO nextcloud_source_versions\n\t\t\t\t\t\t(tenant_id, "+
						"knowledge_base_id, datasource_id, external_id, "+
						"state)\n\t\t\t\t\t\tVALUES (7, 'kb-synthetic', 'ds-synthetic', "+
						"'nextcloud:instance_synthetic:41', 'published')").Error)
				}
				if scenario != "version_without_cursor" {
					instance := testEventInstance
					if scenario == "wrong_instance" {
						instance = "another_instance"
					}
					files := map[string]any{testEventBinding: map[string]any{}}
					if scenario == "missing_binding" {
						files = map[string]any{"another_binding": map[string]any{}}
					}
					if scenario == "valid_inventory" {
						files[testEventBinding] = map[string]any{"41": map[string]any{"etag": "v1"}}
					}
					cursor, err := json.Marshal(map[string]any{"connector_cursor": map[string]any{
						"instance_id": instance, "files": files,
					}})
					require.NoError(t, err)
					require.NoError(t, db.Exec(`UPDATE data_sources SET last_sync_cursor = ? WHERE id = 'ds-synthetic'`,
						string(cursor)).Error)
				}
			}
			repo := repository.NewNextcloudEventInboxRepository(db)
			claim, err := repo.ClaimDispatch(context.Background(), testEventConnection, time.Now().UTC())
			require.NoError(t, err)
			status, err := repo.ConnectionStatus(context.Background(), 7, "ds-synthetic")
			require.NoError(t, err)
			if scenario == "valid_inventory" {
				require.NotNil(t, claim)
				return
			}
			require.Nil(t, claim)
			require.Equal(t, "blocked", status.DispatchState)
			require.Equal(t, "cursor_missing_manual_review", status.LastErrorCode)
		})
	}
}

func TestNextcloudEventDispatchRejectsLiveInstanceChange(t *testing.T) {
	db, router, secret := setupNextcloudDispatchTest(t, false)
	receiveDispatchHint(t, router, secret, "0", "41", "b")
	repo := repository.NewNextcloudEventInboxRepository(db)
	ctx := context.Background()
	now := time.Now().UTC()
	claim, err := repo.ClaimDispatch(ctx, testEventConnection, now)
	require.NoError(t, err)
	require.NotNil(t, claim)
	require.NoError(t, repo.PrepareDispatch(ctx, *claim, "live-instance-log", now))
	require.NoError(t, repo.ValidateEventSyncTask(ctx, testEventConnection,
		"ds-synthetic", 7, claim.ConfigSHA256, "live-instance-log", testEventInstance))
	require.ErrorIs(t, repo.ValidateEventSyncTask(ctx, testEventConnection,
		"ds-synthetic", 7, claim.ConfigSHA256, "live-instance-log", "cloned_instance"),
		repository.ErrNextcloudEventScope)
}

func testNextcloudEventSyncAdmission(t *testing.T, usePostgres bool) {
	t.Helper()
	db, _, _ := setupNextcloudDispatchTest(t, usePostgres)
	now := time.Now().UTC()
	require.NoError(t, db.Exec(`INSERT INTO sync_logs (id, data_source_id, status, started_at)
		VALUES ('first-running', 'ds-synthetic', 'running', ?)`, now).Error)
	err := db.Exec(`INSERT INTO sync_logs (id, data_source_id, status, started_at)
		VALUES ('second-running', 'ds-synthetic', 'running', ?)`, now).Error
	require.ErrorContains(t, err, "nextcloud_source_sync_busy")
	require.NoError(t, db.Exec(`INSERT INTO sync_logs (id, data_source_id, status, started_at)
		VALUES ('later-failed', 'ds-synthetic', 'failed', ?)`, now).Error)
	err = db.Exec(`UPDATE sync_logs SET status = 'running' WHERE id = 'later-failed'`).Error
	require.ErrorContains(t, err, "nextcloud_source_sync_busy")
	require.NoError(t, db.Exec(`UPDATE sync_logs SET status = 'success' WHERE id = 'first-running'`).Error)
	require.NoError(t, db.Exec(`UPDATE sync_logs SET status = 'running' WHERE id = 'later-failed'`).Error)
}

func TestNextcloudEventSyncAdmissionSQLite(t *testing.T) {
	testNextcloudEventSyncAdmission(t, false)
}

func TestNextcloudEventSyncAdmissionPostgres(t *testing.T) {
	testNextcloudEventSyncAdmission(t, true)
}

func TestNextcloudEventSyncAdmissionPostgresConcurrent(t *testing.T) {
	dsn := os.Getenv("NEXTCLOUD_EVENT_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("set NEXTCLOUD_EVENT_TEST_POSTGRES_DSN")
	}
	db, _, _ := setupNextcloudDispatchTest(t, true)
	var schema string
	require.NoError(t, db.Raw(`SELECT current_schema()`).Scan(&schema).Error)
	other, err := gorm.Open(postgres.Open(dsn),
		&gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	require.NoError(t, err)
	otherSQL, err := other.DB()
	require.NoError(t, err)
	otherSQL.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = otherSQL.Close() })
	require.NoError(t, other.Exec(`SET search_path TO `+schema).Error)
	tx := db.Begin()
	require.NoError(t, tx.Error)
	require.NoError(t, tx.Exec(`INSERT INTO sync_logs (id, data_source_id, status, started_at)
		VALUES ('concurrent-first', 'ds-synthetic', 'running', ?)`, time.Now().UTC()).Error)
	done := make(chan error, 1)
	go func() {
		done <- other.Exec(`INSERT INTO sync_logs (id, data_source_id, status, started_at)
			VALUES ('concurrent-second', 'ds-synthetic', 'running', ?)`, time.Now().UTC()).Error
	}()
	select {
	case err := <-done:
		_ = tx.Rollback()
		t.Fatalf("second admission returned before first transaction committed: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	require.NoError(t, tx.Commit().Error)
	select {
	case err := <-done:
		require.ErrorContains(t, err, "nextcloud_source_sync_busy")
	case <-time.After(5 * time.Second):
		t.Fatal("second admission did not resolve after first commit")
	}
}

func TestNextcloudEventDispatchRequiresSecondDeletionScan(t *testing.T) {
	db, router, secret := setupNextcloudDispatchTest(t, false)
	deletion := sampleNextcloudEventBatch("0", "41")
	deletion.Events[0].Type = "delete"
	response := postNextcloudEvent(router, signedNextcloudEventRequest(t, secret,
		deletion, strings.Repeat("c", 32)))
	require.Equal(t, http.StatusAccepted, response.Code, response.Body.String())
	repo := repository.NewNextcloudEventInboxRepository(db)
	ctx := context.Background()
	now := time.Now().UTC()
	claim, err := repo.ClaimDispatch(ctx, testEventConnection, now)
	require.NoError(t, err)
	require.NotNil(t, claim)
	require.NoError(t, repo.PrepareDispatch(ctx, *claim, "missing-first", now))
	require.NoError(t, db.Exec(`INSERT INTO sync_logs (id, data_source_id, status, started_at, finished_at)
		VALUES ('missing-first', 'ds-synthetic', 'success', ?, ?)`, now, now).Error)
	require.NoError(t, repo.FinishDispatch(ctx, *claim, "missing-first", now))
	require.NoError(t, db.Exec(`INSERT INTO knowledges
		(id, tenant_id, knowledge_base_id, channel, parse_status, enable_status, file_name, metadata)
		VALUES ('deleted-candidate', 7, 'kb-synthetic', 'nextcloud', 'completed', 'enabled', 'Notes.md', ?)`,
		`{"datasource_id":"ds-synthetic","external_id":"nextcloud:instance_synthetic:41",`+
			`"nextcloud_etag":"old"}`).Error)
	writeCursor := func(missing bool, scannedAt time.Time) {
		files := map[string]any{"41": map[string]any{"etag": "old"}}
		tombstones := map[string]any{}
		if !missing {
			files = map[string]any{}
			tombstones["41"] = map[string]any{"etag": "old"}
		}
		cursor, marshalErr := json.Marshal(map[string]any{
			"last_sync_time": scannedAt,
			"connector_cursor": map[string]any{
				"instance_id":       testEventInstance,
				"files":             map[string]any{testEventBinding: files},
				"last_reconcile_at": scannedAt.Unix(),
				"missing":           map[string]any{testEventBinding: map[string]any{"41": missing}},
				"tombstones":        map[string]any{testEventBinding: tombstones},
				"changes":           map[string]any{testEventBinding: nextcloudEventChangeCursor("41")},
			},
		})
		require.NoError(t, marshalErr)
		require.NoError(t, db.Exec(`UPDATE data_sources SET last_sync_cursor = ? WHERE id = 'ds-synthetic'`,
			string(cursor)).Error)
	}
	writeCursor(true, now.Add(time.Minute))
	first, err := repo.ClaimDispatch(ctx, testEventConnection, now.Add(2*time.Minute))
	require.NoError(t, err)
	require.Nil(t, first)
	status, err := repo.ConnectionStatus(ctx, 7, "ds-synthetic")
	require.NoError(t, err)
	require.Equal(t, "retry", status.DispatchState)
	require.Equal(t, "deletion_or_cursor_pending", status.LastErrorCode)
	require.Equal(t, "0", status.AppliedThroughEventID)
	second, err := repo.ClaimDispatch(ctx, testEventConnection, now.Add(4*time.Minute))
	require.NoError(t, err)
	require.NotNil(t, second)
	require.NoError(t, repo.PrepareDispatch(ctx, *second, "missing-second", now.Add(4*time.Minute)))
	require.NoError(t, db.Exec("INSERT INTO sync_logs (id, data_source_id, status, started_at, "+
		"finished_at)\n\t\tVALUES ('missing-second', 'ds-synthetic', "+
		"'success', ?, ?)", now.Add(4*time.Minute), now.Add(4*time.Minute)).Error)
	require.NoError(t, repo.FinishDispatch(ctx, *second, "missing-second", now.Add(4*time.Minute)))
	require.NoError(t, db.Exec(`INSERT INTO nextcloud_source_versions
		(tenant_id, knowledge_base_id, datasource_id, external_id, state)
		VALUES (7, 'kb-synthetic', 'ds-synthetic', 'nextcloud:instance_synthetic:41', 'tombstone')`).Error)
	require.NoError(t, db.Exec(`UPDATE knowledges SET deleted_at = ?, metadata = ?
		WHERE id = 'deleted-candidate'`, now.Add(5*time.Minute),
		`{"datasource_id":"ds-synthetic","external_id":"nextcloud:instance_synthetic:41",`+
			`"nextcloud_etag":""}`).Error)
	writeCursor(false, now.Add(5*time.Minute))
	finished, err := repo.ClaimDispatch(ctx, testEventConnection, now.Add(6*time.Minute))
	require.NoError(t, err)
	require.Nil(t, finished)
	status, err = repo.ConnectionStatus(ctx, 7, "ds-synthetic")
	require.NoError(t, err)
	require.Equal(t, "idle", status.DispatchState)
	require.Equal(t, "41", status.AppliedThroughEventID)
}

func TestNextcloudEventDispatchRevocationAndSourceDrift(t *testing.T) {
	for _, scenario := range []string{"source_changed", "revoked", "missing_inbox"} {
		t.Run(scenario, func(t *testing.T) {
			db, router, secret := setupNextcloudDispatchTest(t, false)
			repo := repository.NewNextcloudEventInboxRepository(db)
			ctx := context.Background()
			now := time.Now().UTC()
			receiveDispatchHint(t, router, secret, "0", "41", "c")
			switch scenario {
			case "source_changed":
				changed := syntheticNextcloudEventDataSourceConfig(t, "https://other.example.test", true)
				require.NoError(t, db.Exec(`UPDATE data_sources SET config = ? WHERE id = ?`,
					string(changed), "ds-synthetic").Error)
			case "revoked":
				require.NoError(t, repo.Revoke(ctx, 7, "ds-synthetic"))
			case "missing_inbox":
				receiveDispatchHint(t, router, secret, "41", "43", "e")
				require.NoError(t, db.Exec("DELETE FROM nextcloud_event_inbox WHERE connection_id = ? AND "+
					"event_id = 43",
					testEventConnection).Error)
			}
			claim, err := repo.ClaimDispatch(ctx, testEventConnection, now)
			require.NoError(t, err)
			require.Nil(t, claim)
			status, err := repo.ConnectionStatus(ctx, 7, "ds-synthetic")
			require.NoError(t, err)
			require.Equal(t, "0", status.DispatchedThroughEventID)
			require.Equal(t, "0", status.AppliedThroughEventID)
			require.Equal(t, "blocked", status.DispatchState)
			switch scenario {
			case "revoked":
				require.Equal(t, "revoked", status.LastErrorCode)
			case "source_changed":
				require.Equal(t, "source_changed", status.LastErrorCode)
			default:
				require.Equal(t, "inbox_missing", status.LastErrorCode)
			}
			var configSHA string
			require.NoError(t, db.Raw(`SELECT datasource_config_sha256 FROM nextcloud_event_connections
				WHERE connection_id = ?`, testEventConnection).Scan(&configSHA).Error)
			if scenario != "missing_inbox" {
				require.ErrorIs(t, repo.ValidateEventSyncTask(ctx, testEventConnection,
					"ds-synthetic", 7, configSHA, "stale-log"), repository.ErrNextcloudEventScope)
			}
		})
	}
}

func TestNextcloudEventDispatchExpiredLeaseCannotFinish(t *testing.T) {
	db, router, secret := setupNextcloudDispatchTest(t, false)
	repo := repository.NewNextcloudEventInboxRepository(db)
	ctx := context.Background()
	now := time.Now().UTC()
	receiveDispatchHint(t, router, secret, "0", "41", "d")
	stale, err := repo.ClaimDispatch(ctx, testEventConnection, now)
	require.NoError(t, err)
	require.NotNil(t, stale)
	require.NoError(t, repo.PrepareDispatch(ctx, *stale, "never-enqueued-log", now))
	current, err := repo.ClaimDispatch(ctx, testEventConnection, now.Add(4*time.Minute))
	require.NoError(t, err)
	require.NotNil(t, current)
	require.NotEqual(t, stale.LeaseToken, current.LeaseToken)
	require.ErrorIs(t, repo.ValidateEventSyncTask(ctx, testEventConnection,
		"ds-synthetic", 7, stale.ConfigSHA256, "never-enqueued-log"), repository.ErrNextcloudEventScope)
	require.ErrorIs(t, repo.FinishDispatch(ctx, *stale, "stale-log", now.Add(4*time.Minute)),
		repository.ErrNextcloudEventConflict)
	require.NoError(t, db.Exec("INSERT INTO sync_logs (id, data_source_id, status, started_at) "+
		"VALUES (?, 'ds-synthetic', ?, ?)",
		"current-log", types.SyncLogStatusRunning, now).Error)
	require.NoError(t, repo.PrepareDispatch(ctx, *current, "current-log", now.Add(4*time.Minute)))
	require.NoError(t, repo.FinishDispatch(ctx, *current, "current-log", now.Add(4*time.Minute)))
	require.NoError(t, repo.Revoke(ctx, 7, "ds-synthetic"))
	require.ErrorIs(t, repo.ValidateEventSyncTask(ctx, testEventConnection,
		"ds-synthetic", 7, current.ConfigSHA256, "current-log"), repository.ErrNextcloudEventScope)
	_, err = repo.ClaimDispatch(ctx, testEventConnection, now.Add(10*time.Minute))
	require.NoError(t, err)
	status, err := repo.ConnectionStatus(ctx, 7, "ds-synthetic")
	require.NoError(t, err)
	require.Equal(t, "0", status.AppliedThroughEventID)
	require.True(t, errors.Is(repo.FinishDispatch(ctx, *current, "current-log", now.Add(10*time.Minute)),
		repository.ErrNextcloudEventScope))
}

func TestNextcloudEventDispatchCrashAfterEnqueueReconcilesIntent(t *testing.T) {
	db, router, secret := setupNextcloudDispatchTest(t, false)
	repo := repository.NewNextcloudEventInboxRepository(db)
	ctx := context.Background()
	now := time.Now().UTC()
	receiveDispatchHint(t, router, secret, "0", "41", "f")
	claim, err := repo.ClaimDispatch(ctx, testEventConnection, now)
	require.NoError(t, err)
	require.NotNil(t, claim)
	require.NoError(t, repo.PrepareDispatch(ctx, *claim, "completed-before-record", now))
	require.NoError(t, db.Exec("INSERT INTO sync_logs (id, data_source_id, status, started_at, "+
		"finished_at) VALUES (?, 'ds-synthetic', ?, ?, ?)",
		"completed-before-record", types.SyncLogStatusSuccess, now, now).Error)
	// Simulate process death after the task ran but before FinishDispatch.
	recovered, err := repo.ClaimDispatch(ctx, testEventConnection, now.Add(4*time.Minute))
	require.NoError(t, err)
	require.Nil(t, recovered)
	status, err := repo.ConnectionStatus(ctx, 7, "ds-synthetic")
	require.NoError(t, err)
	require.Equal(t, "41", status.DispatchedThroughEventID)
	require.Equal(t, "0", status.AppliedThroughEventID)
	require.Equal(t, "queued", status.DispatchState)
	require.Equal(t, "completed-before-record", status.LastSyncLogID)
}

func TestNextcloudEventDispatchStaleRunningTaskBlocksReplacement(t *testing.T) {
	db, router, secret := setupNextcloudDispatchTest(t, false)
	repo := repository.NewNextcloudEventInboxRepository(db)
	ctx := context.Background()
	now := time.Now().UTC()
	receiveDispatchHint(t, router, secret, "0", "41", "9")
	claim, err := repo.ClaimDispatch(ctx, testEventConnection, now)
	require.NoError(t, err)
	require.NotNil(t, claim)
	require.NoError(t, repo.PrepareDispatch(ctx, *claim, "still-running", now))
	require.NoError(t, db.Exec("INSERT INTO sync_logs (id, data_source_id, status, started_at) "+
		"VALUES (?, 'ds-synthetic', ?, ?)",
		"still-running", types.SyncLogStatusRunning, now).Error)
	waiting, err := repo.ClaimDispatch(ctx, testEventConnection, now.Add(4*time.Minute))
	require.NoError(t, err)
	require.Nil(t, waiting)
	status, err := repo.ConnectionStatus(ctx, 7, "ds-synthetic")
	require.NoError(t, err)
	require.Equal(t, "leased", status.DispatchState)
	blocked, err := repo.ClaimDispatch(ctx, testEventConnection, now.Add(151*time.Minute))
	require.NoError(t, err)
	require.Nil(t, blocked)
	status, err = repo.ConnectionStatus(ctx, 7, "ds-synthetic")
	require.NoError(t, err)
	require.Equal(t, "blocked", status.DispatchState)
	require.Equal(t, "sync_stale_manual_review", status.LastErrorCode)
	require.Equal(t, "0", status.DispatchedThroughEventID)
	require.Equal(t, "0", status.AppliedThroughEventID)
}

type eventDispatchDataSources struct {
	interfaces.DataSourceRepository
	ds *types.DataSource
}

func (r eventDispatchDataSources) FindByID(context.Context, string) (*types.DataSource, error) {
	return r.ds, nil
}

type eventDispatchSyncLogs struct {
	interfaces.SyncLogRepository
	created *bool
}

func (r eventDispatchSyncLogs) HasRunningSync(context.Context, string) (bool, error) {
	return false, nil
}

func (r eventDispatchSyncLogs) Create(context.Context, *types.SyncLog) error {
	*r.created = true
	return nil
}

type eventDispatchQueue struct {
	tasks chan *asynq.Task
	fail  bool
}

func (q eventDispatchQueue) Enqueue(task *asynq.Task, _ ...asynq.Option) (*asynq.TaskInfo, error) {
	q.tasks <- task
	if q.fail {
		return nil, errors.New("synthetic queue failure")
	}
	return &asynq.TaskInfo{ID: "accepted", Queue: types.QueueSync}, nil
}

func TestNextcloudEventDispatcherQueuesIncrementalManifestWithoutPrematureSyncLog(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(map[bool]string{false: "accepted", true: "queue_failed"}[fail], func(t *testing.T) {
			db, router, secret := setupNextcloudDispatchTest(t, false)
			receiveDispatchHint(t, router, secret, "0", "41", "8")
			var ds types.DataSource
			require.NoError(t, db.Where("id = ?", "ds-synthetic").Take(&ds).Error)
			ds.Status = types.DataSourceStatusActive
			created := false
			queue := eventDispatchQueue{tasks: make(chan *asynq.Task, 1), fail: fail}
			dispatcher := service.NewNextcloudEventDispatcher(
				repository.NewNextcloudEventInboxRepository(db),
				eventDispatchDataSources{ds: &ds},
				eventDispatchSyncLogs{created: &created}, queue)
			dispatcher.Start()
			var task *asynq.Task
			select {
			case task = <-queue.tasks:
			case <-time.After(3 * time.Second):
				dispatcher.Stop()
				t.Fatal("dispatcher did not enqueue due event")
			}
			wantState := "queued"
			if fail {
				wantState = "leased"
			}
			var status repository.NextcloudEventConnectionStatus
			var err error
			deadline := time.Now().Add(3 * time.Second)
			for time.Now().Before(deadline) {
				status, err = repository.NewNextcloudEventInboxRepository(db).
					ConnectionStatus(context.Background(), 7, "ds-synthetic")
				if err == nil && status.DispatchState == wantState &&
					(!fail || status.LastErrorCode == "queue_uncertain") {
					break
				}
				time.Sleep(10 * time.Millisecond)
			}
			dispatcher.Stop()
			require.NoError(t, err)
			require.Equal(t, wantState, status.DispatchState)
			require.False(t, created, "dispatcher must not create an orphan running log")
			require.Equal(t, types.TypeDataSourceSync, task.Type())
			var payload types.DataSourceSyncPayload
			require.NoError(t, json.Unmarshal(task.Payload(), &payload))
			require.Equal(t, "nextcloud_event", payload.Trigger)
			require.False(t, payload.ForceFull)
			require.Equal(t, testEventConnection, payload.NextcloudEventConnectionID)
			require.NotEmpty(t, payload.SyncLogID)
			var count int64
			require.NoError(t, db.Table("sync_logs").Count(&count).Error)
			require.Zero(t, count)
			require.Equal(t, "0", status.AppliedThroughEventID)
			if fail {
				require.Equal(t, "leased", status.DispatchState)
				require.Equal(t, "queue_uncertain", status.LastErrorCode)
				require.Equal(t, "0", status.DispatchedThroughEventID)
				// The queue may have accepted the task before losing its reply.
				// A running task must prevent a second claim after lease expiry.
				require.NoError(t, db.Exec("INSERT INTO sync_logs (id, data_source_id, status, started_at) "+
					"VALUES (?, 'ds-synthetic', ?, ?)",
					payload.SyncLogID, types.SyncLogStatusRunning, time.Now().UTC()).Error)
				again, err := repository.NewNextcloudEventInboxRepository(db).
					ClaimDispatch(context.Background(), testEventConnection, time.Now().UTC().Add(4*time.Minute))
				require.NoError(t, err)
				require.Nil(t, again)
			} else {
				require.Equal(t, "queued", status.DispatchState)
				require.Equal(t, "41", status.DispatchedThroughEventID)
				require.Equal(t, payload.SyncLogID, status.LastSyncLogID)
			}
		})
	}
}

func TestNextcloudEventDispatcherPollsForPostStartupHint(t *testing.T) {
	t.Setenv("WEKNORA_NEXTCLOUD_EVENT_DISPATCH_INTERVAL", "1s")
	db, router, secret := setupNextcloudDispatchTest(t, false)
	var ds types.DataSource
	require.NoError(t, db.Where("id = ?", "ds-synthetic").Take(&ds).Error)
	ds.Status = types.DataSourceStatusActive
	created := false
	queue := eventDispatchQueue{tasks: make(chan *asynq.Task, 1)}
	repo := repository.NewNextcloudEventInboxRepository(db)
	dispatcher := service.NewNextcloudEventDispatcher(
		repo,
		eventDispatchDataSources{ds: &ds},
		eventDispatchSyncLogs{created: &created}, queue)
	dispatcher.Start()
	defer dispatcher.Stop()

	// Let the startup sweep and one timer tick observe the empty inbox. The
	// next hint must then be picked up by a later configured timer tick.
	time.Sleep(1200 * time.Millisecond)
	receiveDispatchHint(t, router, secret, "0", "41", "9")
	receivedAt := time.Now()
	select {
	case task := <-queue.tasks:
		t.Logf("local SQLite receipt-to-enqueue sample: %s", time.Since(receivedAt))
		var payload types.DataSourceSyncPayload
		require.NoError(t, json.Unmarshal(task.Payload(), &payload))
		require.Equal(t, "nextcloud_event", payload.Trigger)
		require.False(t, payload.ForceFull)
	case <-time.After(3 * time.Second):
		t.Fatal("post-startup Nextcloud hint was not queued by the configured polling interval")
	}
	require.Eventually(t, func() bool {
		status, err := repo.ConnectionStatus(context.Background(), 7, "ds-synthetic")
		return err == nil && status.DispatchState == "queued" && status.DispatchedThroughEventID == "41"
	}, 2*time.Second, 10*time.Millisecond)
	require.False(t, created)
}
