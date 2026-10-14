package repository

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// The sync log can finish before the asynchronous version and parser records
// are visible. A fresh, incomplete proof must retain the exact queued task.
func newNextcloudQueuedPublicationTest(t *testing.T) (*gorm.DB, *NextcloudEventInboxRepository, time.Time, string) {
	t.Helper()
	_, db := newNextcloudVersionTestRepo(t)
	require.NoError(t, db.AutoMigrate(&types.SyncLog{}))
	require.NoError(t, db.Exec(`CREATE TABLE knowledge_bases
		(id TEXT PRIMARY KEY, tenant_id INTEGER, deleted_at DATETIME)`).Error)
	require.NoError(t, db.Exec(`INSERT INTO knowledge_bases (id, tenant_id) VALUES ('kb', 7)`).Error)
	for _, name := range []string{
		"000032_nextcloud_event_inbox.up.sql",
		"000033_nextcloud_event_dispatch.up.sql",
		"000049_nextcloud_event_hint_etag.up.sql",
	} {
		schema, err := os.ReadFile("../../../migrations/sqlite/" + name)
		require.NoError(t, err)
		require.NoError(t, db.Exec(string(schema)).Error)
	}
	var source types.DataSource
	require.NoError(t, db.Where("id = ?", "ds").Take(&source).Error)
	baseURL, configSHA, bindingID, err := NextcloudEventDataSourceIdentity(source.Config)
	require.NoError(t, err)
	require.NoError(t, db.Exec(`INSERT INTO nextcloud_event_connections
		(connection_id, tenant_id, knowledge_base_id, datasource_id, nextcloud_instance_id,
		 binding_id, datasource_base_url, datasource_config_sha256, status,
		 current_key_id, current_secret_ciphertext)
		VALUES ('event-connection', 7, 'kb', 'ds', 'instance', ?, ?, ?, 'active', 'test-key', 'enc:v1:test')`,
		bindingID, baseURL, configSHA).Error)
	require.NoError(t, db.Exec(`INSERT INTO nextcloud_event_checkpoint (connection_id, received_id)
		VALUES ('event-connection', 1)`).Error)
	require.NoError(t, db.Exec(("INSERT INTO nextcloud_event_inbox\n"+
		"\t\t(connection_id, event_id, payload_sha256, event_type,"+
		" file_id, etag, path, relative_path, state)\n"+
		"\t\tVALUES ('event-connection', 1, ?, 'upsert', 77, 'etag"+
		"-v1', '/Published/file.md', 'file.md', 'dispatched')"), strings.Repeat("0", 64)).Error)
	now := time.Now().UTC().Truncate(time.Second)
	require.NoError(t, db.Create(&types.SyncLog{
		ID: "event-log", DataSourceID: "ds",
		TenantID: 7, Status: types.SyncLogStatusRunning, StartedAt: now.Add(-time.Minute),
	}).Error)
	require.NoError(t, db.Exec(`INSERT INTO nextcloud_event_dispatch
		(connection_id, dispatched_id, target_event_id, state, last_sync_log_id, next_attempt_at)
		VALUES ('event-connection', 1, 1, 'queued', 'event-log', ?)`, now).Error)
	changeBytes, err := json.Marshal([]any{1, bindingID, "1", strings.Repeat("0", 64)})
	require.NoError(t, err)
	cursor := &types.SyncCursor{LastSyncTime: now, ConnectorCursor: map[string]interface{}{
		"instance_id": "instance", "last_reconcile_at": now.Unix(),
		"changes": map[string]interface{}{bindingID: base64.RawURLEncoding.EncodeToString(changeBytes)},
		"files": map[string]interface{}{bindingID: map[string]interface{}{
			"77": map[string]string{"etag": "etag-v1", "name": "file.md", "path": "file.md"},
		}},
		"missing": map[string]interface{}{}, "tombstones": map[string]interface{}{},
	}}
	rawCursor, err := cursor.ToJSON()
	require.NoError(t, err)
	require.NoError(t, db.Model(&types.DataSource{}).Where("id = ?", "ds").
		Update("last_sync_cursor", rawCursor).Error)
	return db, NewNextcloudEventInboxRepository(db), now, bindingID
}

func publicationDispatchRow(t *testing.T, db *gorm.DB) nextcloudEventDispatchRow {
	t.Helper()
	var row nextcloudEventDispatchRow
	require.NoError(t, db.Table("nextcloud_event_dispatch").
		Where("connection_id = 'event-connection'").Take(&row).Error)
	return row
}

func TestNextcloudQueuedPublicationWaitsForBriefRegistrationGap(t *testing.T) {
	ctx := context.Background()
	db, inbox, now, _ := newNextcloudQueuedPublicationTest(t)
	claim, err := inbox.ClaimDispatch(ctx, "event-connection", now)
	require.NoError(t, err)
	require.Nil(t, claim)
	row := publicationDispatchRow(t, db)
	require.Equal(t, "queued", row.State)
	require.Equal(t, now.Add(nextcloudQueuedPollInterval), row.NextAttemptAt)
	require.Zero(t, row.AppliedID)

	require.NoError(t, db.Exec(`UPDATE sync_logs SET status = 'success', finished_at = ?
		WHERE id = 'event-log'`, now).Error)
	claim, err = inbox.ClaimDispatch(ctx, "event-connection", now.Add(nextcloudQueuedPollInterval))
	require.NoError(t, err)
	require.Nil(t, claim)
	row = publicationDispatchRow(t, db)
	require.Equal(t, "queued", row.State)
	require.Equal(t, "publication_pending", row.LastErrorCode)
	require.Equal(t, now.Add(2*nextcloudQueuedPollInterval), row.NextAttemptAt)
	require.Zero(t, row.AppliedID)

	ready := insertNextcloudVersionTestKnowledge(t, db, "published-after-sync", "etag-v1",
		types.ParseStatusCompleted, "enabled")
	require.NoError(t, db.Model(&types.Knowledge{}).Where("id = ?", ready.ID).
		Update("file_name", "file.md").Error)
	require.NoError(t, db.Create(&nextcloudSourceVersion{
		TenantID: 7, KnowledgeBaseID: "kb",
		DataSourceID: "ds", ExternalID: "nextcloud:instance:77", DesiredETag: "etag-v1",
		CandidateKnowledgeID: ready.ID, State: "published", UpdatedAt: now,
	}).Error)
	claim, err = inbox.ClaimDispatch(ctx, "event-connection", now.Add(2*nextcloudQueuedPollInterval))
	require.NoError(t, err)
	require.Nil(t, claim)
	row = publicationDispatchRow(t, db)
	require.Equal(t, "idle", row.State)
	require.Equal(t, int64(1), row.AppliedID)
}

func TestNextcloudQueuedPublicationUnprovenAfterGraceRetriesWithoutAck(t *testing.T) {
	ctx := context.Background()
	db, inbox, now, _ := newNextcloudQueuedPublicationTest(t)
	require.NoError(t, db.Exec(`UPDATE sync_logs SET status = 'success', finished_at = ?
		WHERE id = 'event-log'`, now).Error)
	checkAt := now.Add(nextcloudPublicationRetryGrace + time.Second)
	claim, err := inbox.ClaimDispatch(ctx, "event-connection", checkAt)
	require.NoError(t, err)
	require.Nil(t, claim)
	row := publicationDispatchRow(t, db)
	require.Equal(t, "retry", row.State)
	require.Equal(t, "publication_unproven", row.LastErrorCode)
	require.Equal(t, checkAt.Add(time.Minute), row.NextAttemptAt)
	require.Zero(t, row.AppliedID)
}

func TestNextcloudQueuedPublicationWaitsForParserWithoutAck(t *testing.T) {
	ctx := context.Background()
	db, inbox, now, _ := newNextcloudQueuedPublicationTest(t)
	require.NoError(t, db.Exec(`UPDATE sync_logs SET status = 'success', finished_at = ?
		WHERE id = 'event-log'`, now).Error)
	pending := insertNextcloudVersionTestKnowledge(t, db, "pending-after-sync", "",
		types.ParseStatusPending, "disabled")
	require.NoError(t, db.Create(&nextcloudSourceVersion{
		TenantID: 7, KnowledgeBaseID: "kb",
		DataSourceID: "ds", ExternalID: "nextcloud:instance:77", DesiredETag: "etag-v1",
		CandidateKnowledgeID: pending.ID, State: "staging", UpdatedAt: now,
	}).Error)
	for _, checkAt := range []time.Time{now.Add(5 *
		time.Second), now.Add(nextcloudPublicationFastPollWindow + time.Second)} {
		claim, err := inbox.ClaimDispatch(ctx, "event-connection", checkAt)
		require.NoError(t, err)
		require.Nil(t, claim)
		row := publicationDispatchRow(t, db)
		require.Equal(t, "queued", row.State)
		require.Equal(t, "publication_pending", row.LastErrorCode)
		require.Zero(t, row.AppliedID)
		wantDelay := nextcloudQueuedPollInterval
		if checkAt.Sub(now) >= nextcloudPublicationFastPollWindow {
			wantDelay = time.Minute
		}
		require.Equal(t, checkAt.Add(wantDelay), row.NextAttemptAt)
	}
}

func TestNextcloudQueuedPublicationSupersededByNewReceipt(t *testing.T) {
	ctx := context.Background()
	db, inbox, now, bindingID := newNextcloudQueuedPublicationTest(t)
	// The old candidate completed parsing, but a 409 from the source left it
	// unpublished. Its proof cannot become complete after the file changes.
	stale := insertNextcloudVersionTestKnowledge(t, db, "stale-candidate", "",
		types.ParseStatusCompleted, "disabled")
	require.NoError(t, db.Create(&nextcloudSourceVersion{
		TenantID: 7, KnowledgeBaseID: "kb",
		DataSourceID: "ds", ExternalID: "nextcloud:instance:77", DesiredETag: "etag-v1",
		CandidateKnowledgeID: stale.ID, State: "staging", UpdatedAt: now,
	}).Error)
	require.NoError(t, db.Exec(`UPDATE sync_logs SET status = 'success', finished_at = ?
		WHERE id = 'event-log'`, now).Error)
	require.NoError(t, db.Exec(("INSERT INTO nextcloud_event_inbox\n"+
		"\t\t(connection_id, event_id, payload_sha256, event_type,"+
		" file_id, etag, path, relative_path, state)\n"+
		"\t\tVALUES ('event-connection', 2, ?, 'upsert', 77, 'etag"+
		"-v2', '/Published/file.md', 'file.md', 'pending')"), strings.Repeat("1", 64)).Error)
	require.NoError(t, db.Exec(`UPDATE nextcloud_event_checkpoint SET received_id = 2
		WHERE connection_id = 'event-connection'`).Error)

	claim, err := inbox.ClaimDispatch(ctx, "event-connection", now.Add(time.Second))
	require.NoError(t, err)
	require.NotNil(t, claim)
	require.Equal(t, int64(2), claim.TargetEventID)
	row := publicationDispatchRow(t, db)
	require.Equal(t, "leased", row.State)
	require.Equal(t, int64(1), row.DispatchedID)
	require.Zero(t, row.AppliedID, "superseded content must not be acknowledged")
	require.NoError(t, inbox.PrepareDispatch(ctx, *claim, "new-event-log", now.Add(time.Second)))
	require.NoError(t, inbox.FinishDispatch(ctx, *claim, "new-event-log", now.Add(time.Second)))

	finished := now.Add(3 * time.Second)
	require.NoError(t, db.Create(&types.SyncLog{
		ID: "new-event-log", DataSourceID: "ds",
		TenantID: 7, Status: types.SyncLogStatusSuccess,
		StartedAt: now.Add(2 * time.Second), FinishedAt: &finished,
	}).Error)
	oldReady := insertNextcloudVersionTestKnowledge(t, db, "old-published", "etag-v1",
		types.ParseStatusCompleted, "enabled")
	require.NoError(t, db.Model(&types.Knowledge{}).Where("id = ?", oldReady.ID).
		Update("file_name", "file.md").Error)
	require.NoError(t, db.Model(&nextcloudSourceVersion{}).
		Where("tenant_id = ? AND knowledge_base_id = ? AND datasource_id = ? AND external_id = ?",
			7, "kb", "ds", "nextcloud:instance:77").
		Updates(map[string]any{
			"desired_etag": "etag-v1", "candidate_knowledge_id": oldReady.ID,
			"state": "published", "updated_at": now.Add(4 * time.Second),
		}).Error)
	var source types.DataSource
	require.NoError(t, db.Where("id = ?", "ds").Take(&source).Error)
	cursor, err := source.ParseSyncCursor()
	require.NoError(t, err)
	changeBytes, err := json.Marshal([]any{1, bindingID, "2", strings.Repeat("0", 64)})
	require.NoError(t, err)
	cursor.LastSyncTime = now.Add(4 * time.Second)
	cursor.ConnectorCursor["last_reconcile_at"] = now.Add(4 * time.Second).Unix()
	cursor.ConnectorCursor["changes"] = map[string]interface{}{
		bindingID: base64.RawURLEncoding.EncodeToString(changeBytes),
	}
	cursor.ConnectorCursor["files"] = map[string]interface{}{bindingID: map[string]interface{}{
		"77": map[string]string{"etag": "etag-v1", "name": "file.md", "path": "file.md"},
	}}
	rawCursor, err := cursor.ToJSON()
	require.NoError(t, err)
	require.NoError(t, db.Model(&types.DataSource{}).Where("id = ?", "ds").
		Update("last_sync_cursor", rawCursor).Error)

	claim, err = inbox.ClaimDispatch(ctx, "event-connection", now.Add(6*time.Second))
	require.NoError(t, err)
	require.Nil(t, claim)
	row = publicationDispatchRow(t, db)
	require.Zero(t, row.AppliedID, "a changes cursor through event 2 cannot prove a stale V1 manifest")
	require.Equal(t, "queued", row.State)

	ready := insertNextcloudVersionTestKnowledge(t, db, "new-candidate", "etag-v2",
		types.ParseStatusCompleted, "enabled")
	require.NoError(t, db.Model(&types.Knowledge{}).Where("id = ?", ready.ID).
		Update("file_name", "file.md").Error)
	require.NoError(t, db.Model(&nextcloudSourceVersion{}).
		Where("tenant_id = ? AND knowledge_base_id = ? AND datasource_id = ? AND external_id = ?",
			7, "kb", "ds", "nextcloud:instance:77").
		Updates(map[string]any{
			"desired_etag": "etag-v2", "candidate_knowledge_id": ready.ID,
			"state": "published", "updated_at": now.Add(8 * time.Second),
		}).Error)
	require.NoError(t, writeNextcloudETag(db, oldReady, ""))
	cursor.ConnectorCursor["files"] = map[string]interface{}{bindingID: map[string]interface{}{
		"77": map[string]string{"etag": "etag-v2", "name": "file.md", "path": "file.md"},
	}}
	cursor.LastSyncTime = now.Add(8 * time.Second)
	rawCursor, err = cursor.ToJSON()
	require.NoError(t, err)
	require.NoError(t, db.Model(&types.DataSource{}).Where("id = ?", "ds").
		Update("last_sync_cursor", rawCursor).Error)
	claim, err = inbox.ClaimDispatch(ctx, "event-connection", now.Add(11*time.Second))
	require.NoError(t, err)
	require.Nil(t, claim)
	row = publicationDispatchRow(t, db)
	require.Equal(t, "idle", row.State)
	require.Equal(t, int64(2), row.AppliedID,
		"the newer complete manifest and publication proof cover both hints")
	var unapplied int64
	require.NoError(t, db.Table("nextcloud_event_inbox").
		Where("connection_id = ? AND state <> 'applied'", "event-connection").Count(&unapplied).Error)
	require.Zero(t, unapplied)
}

func TestNextcloudPublicationUnprovenRetryWakesOnlyForSignedImportableHint(t *testing.T) {
	ctx := context.Background()
	db, inbox, now, _ := newNextcloudQueuedPublicationTest(t)
	require.NoError(t, db.Exec(`UPDATE sync_logs SET status = 'success', finished_at = ?
		WHERE id = 'event-log'`, now).Error)
	checkAt := now.Add(nextcloudPublicationRetryGrace + time.Second)
	claim, err := inbox.ClaimDispatch(ctx, "event-connection", checkAt)
	require.NoError(t, err)
	require.Nil(t, claim)
	row := publicationDispatchRow(t, db)
	require.Equal(t, "retry", row.State)
	require.Equal(t, "publication_unproven", row.LastErrorCode)
	require.NoError(t, db.Exec(`INSERT INTO nextcloud_event_inbox
		(connection_id, event_id, payload_sha256, event_type, file_id, etag, path, relative_path, state)
		VALUES ('event-connection', 2, ?, 'upsert', 77, 'etag-v2', '/Published/file.md', 'file.md', 'pending')`,
		strings.Repeat("2", 64)).Error)
	require.NoError(t, db.Exec(`UPDATE nextcloud_event_checkpoint SET received_id = 2
		WHERE connection_id = 'event-connection'`).Error)
	wakeAt := checkAt.Add(time.Second)
	candidates, err := inbox.DispatchCandidates(ctx, wakeAt)
	require.NoError(t, err)
	require.Equal(t, []string{"event-connection"}, candidates)
	claim, err = inbox.ClaimDispatch(ctx, "event-connection", wakeAt)
	require.NoError(t, err)
	require.NotNil(t, claim)
	require.Equal(t, int64(2), claim.TargetEventID)
	row = publicationDispatchRow(t, db)
	require.Zero(t, row.AppliedID)
}

func TestNextcloudUnprovenDoesNotCoalesceUnsupportedOrUnsignedHint(t *testing.T) {
	for _, newer := range []struct {
		name, etag, path string
	}{
		{"unsupported", "etag-v2", "/Published/file.bin"},
		{"unsigned-etag", "", "/Published/file.md"},
		{"missing-path", "etag-v2", ""},
	} {
		t.Run(newer.name, func(t *testing.T) {
			ctx := context.Background()
			db, inbox, now, _ := newNextcloudQueuedPublicationTest(t)
			require.NoError(t, db.Exec(`UPDATE sync_logs SET status = 'success', finished_at = ?
				WHERE id = 'event-log'`, now).Error)
			require.NoError(t, db.Exec(`INSERT INTO nextcloud_event_inbox
				(connection_id, event_id, payload_sha256, event_type, file_id, etag, path, relative_path, state)
				VALUES ('event-connection', 2, ?, 'upsert', 77, ?, ?, ?, 'pending')`,
				strings.Repeat("2", 64), newer.etag, newer.path,
				strings.TrimPrefix(newer.path, "/Published/")).Error)
			require.NoError(t, db.Exec(`UPDATE nextcloud_event_checkpoint SET received_id = 2
				WHERE connection_id = 'event-connection'`).Error)
			claim, err := inbox.ClaimDispatch(ctx, "event-connection", now.Add(time.Second))
			require.NoError(t, err)
			require.Nil(t, claim)
			row := publicationDispatchRow(t, db)
			require.Equal(t, "queued", row.State)
			require.Zero(t, row.AppliedID)
		})
	}
}

func TestNextcloudPublicationProofRejectsUnboundLatestHints(t *testing.T) {
	for _, latest := range []struct {
		name, eventType          string
		etag, path, relativePath any
		fileID                   any
	}{
		{"null-etag", "upsert", nil, "/Published/file.md", "file.md", int64(77)},
		{"metadata-rename", "metadata", "etag-v1", "/Published/renamed.md", "renamed.md", int64(77)},
		{"broad", "upsert", "etag-v1", "/Published/file.md", "file.md", nil},
	} {
		t.Run(latest.name, func(t *testing.T) {
			ctx := context.Background()
			db, inbox, now, _ := newNextcloudQueuedPublicationTest(t)
			ready := insertNextcloudVersionTestKnowledge(t, db, "ready-unbound", "etag-v1",
				types.ParseStatusCompleted, "enabled")
			require.NoError(t, db.Model(&types.Knowledge{}).Where("id = ?", ready.ID).
				Update("file_name", "file.md").Error)
			require.NoError(t, db.Create(&nextcloudSourceVersion{
				TenantID: 7, KnowledgeBaseID: "kb",
				DataSourceID: "ds", ExternalID: "nextcloud:instance:77", DesiredETag: "etag-v1",
				CandidateKnowledgeID: ready.ID, State: "published", UpdatedAt: now,
			}).Error)
			require.NoError(t, db.Exec(("UPDATE nextcloud_event_inbox SET event_type = ?, file_i"+
				"d = ?, etag = ?, path = ?, relative_path = ?\n"+
				"\t\t\t\tWHERE connection_id = 'event-connection' AND event_"+
				"id = 1"),
				latest.eventType, latest.fileID, latest.etag, latest.path, latest.relativePath).Error)
			require.NoError(t, db.Exec(`UPDATE sync_logs SET status = 'success', finished_at = ?
				WHERE id = 'event-log'`, now).Error)
			claim, err := inbox.ClaimDispatch(ctx, "event-connection", now.Add(time.Second))
			require.NoError(t, err)
			require.Nil(t, claim)
			row := publicationDispatchRow(t, db)
			require.Zero(t, row.AppliedID)
			require.Equal(t, "queued", row.State)
			claim, err = inbox.ClaimDispatch(ctx, "event-connection", now.Add(
				nextcloudPublicationRetryGrace+time.Second,
			))
			require.NoError(t, err)
			require.Nil(t, claim)
			row = publicationDispatchRow(t, db)
			if latest.name == "metadata-rename" {
				require.Equal(t, "retry", row.State, "a signed relative path can become provable on a later scan")
			} else {
				require.Equal(t, "blocked", row.State)
				require.Equal(t, "hint_unbound_manual_review", row.LastErrorCode)
			}
		})
	}
}

func TestNextcloudSubtreeHintRequiresSecondCompleteManifest(t *testing.T) {
	ctx := context.Background()
	db, inbox, now, _ := newNextcloudQueuedPublicationTest(t)
	ready := insertNextcloudVersionTestKnowledge(t, db, "subtree-ready", "etag-v1",
		types.ParseStatusCompleted, "enabled")
	require.NoError(t, db.Model(&types.Knowledge{}).Where("id = ?", ready.ID).
		Update("file_name", "file.md").Error)
	require.NoError(t, db.Create(&nextcloudSourceVersion{
		TenantID: 7, KnowledgeBaseID: "kb",
		DataSourceID: "ds", ExternalID: "nextcloud:instance:77", DesiredETag: "etag-v1",
		CandidateKnowledgeID: ready.ID, State: "published", UpdatedAt: now,
	}).Error)
	require.NoError(t, db.Exec(`UPDATE nextcloud_event_inbox
		SET event_type = 'subtree_moved', file_id = NULL, etag = NULL, relative_path = NULL
		WHERE connection_id = 'event-connection' AND event_id = 1`).Error)
	require.NoError(t, db.Exec(`UPDATE sync_logs SET status = 'success', finished_at = ?
		WHERE id = 'event-log'`, now).Error)
	claim, err := inbox.ClaimDispatch(ctx, "event-connection", now.Add(time.Second))
	require.NoError(t, err)
	require.Nil(t, claim)
	row := publicationDispatchRow(t, db)
	require.Equal(t, "retry", row.State)
	require.Equal(t, "broad_confirm:1", row.LastErrorCode)
	require.Zero(t, row.AppliedID)
	claim, err = inbox.ClaimDispatch(ctx, "event-connection", now.Add(6*time.Second))
	require.NoError(t, err)
	require.NotNil(t, claim)
	require.Equal(t, int64(1), claim.TargetEventID)
	require.NoError(t, inbox.PrepareDispatch(ctx, *claim, "subtree-second-log", now.Add(6*time.Second)))
	require.NoError(t, inbox.FinishDispatch(ctx, *claim, "subtree-second-log", now.Add(6*time.Second)))
	row = publicationDispatchRow(t, db)
	require.Equal(t, "broad_confirm:1", row.LastErrorCode)
	finished := now.Add(7 * time.Second)
	require.NoError(t, db.Create(&types.SyncLog{
		ID: "subtree-second-log", DataSourceID: "ds",
		TenantID: 7, Status: types.SyncLogStatusSuccess,
		StartedAt: now.Add(6 * time.Second), FinishedAt: &finished,
	}).Error)
	var source types.DataSource
	require.NoError(t, db.Where("id = ?", "ds").Take(&source).Error)
	cursor, err := source.ParseSyncCursor()
	require.NoError(t, err)
	cursor.LastSyncTime = finished
	cursor.ConnectorCursor["last_reconcile_at"] = finished.Unix()
	raw, err := cursor.ToJSON()
	require.NoError(t, err)
	require.NoError(t, db.Model(&types.DataSource{}).Where("id = ?", "ds").
		Update("last_sync_cursor", raw).Error)
	claim, err = inbox.ClaimDispatch(ctx, "event-connection", now.Add(12*time.Second))
	require.NoError(t, err)
	require.Nil(t, claim)
	row = publicationDispatchRow(t, db)
	require.Equal(t, "idle", row.State)
	require.Equal(t, int64(1), row.AppliedID)
}

func TestNextcloudFileScopedReconcileTransitionsAckAfterSecondCompleteManifest(t *testing.T) {
	for _, scenario := range []struct {
		name           string
		firstETag      any
		latestETag     any
		finalPublished bool
	}{
		{name: "withdraw-then-republish", firstETag: nil, latestETag: "etag-v2", finalPublished: true},
		{name: "republish-then-withdraw", firstETag: "etag-v2", latestETag: nil},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			ctx := context.Background()
			db, inbox, now, bindingID := newNextcloudQueuedPublicationTest(t)
			// Both file-scoped receipts were dispatched in one reconciliation.
			// Their event-time states conflict, so only the final inventory and
			// publication rows can prove what should be acknowledged.
			require.NoError(t, db.Exec(`UPDATE nextcloud_event_inbox
				SET event_type = 'reconcile', etag = ?
				WHERE connection_id = 'event-connection' AND event_id = 1`, scenario.firstETag).Error)
			require.NoError(t, db.Exec(`INSERT INTO nextcloud_event_inbox
				(connection_id, event_id, payload_sha256, event_type, file_id, etag, path, relative_path, state)
				VALUES ('event-connection', 2, ?, 'reconcile', 77, ?, '/Published/file.md', 'file.md', 'dispatched')`,
				strings.Repeat("2", 64), scenario.latestETag).Error)
			require.NoError(t, db.Exec(`UPDATE nextcloud_event_checkpoint SET received_id = 2
				WHERE connection_id = 'event-connection'`).Error)
			require.NoError(t, db.Exec(`UPDATE nextcloud_event_dispatch
				SET dispatched_id = 2, target_event_id = 2
				WHERE connection_id = 'event-connection'`).Error)
			require.NoError(t, db.Exec(`UPDATE sync_logs SET status = 'success', finished_at = ?
				WHERE id = 'event-log'`, now).Error)

			var source types.DataSource
			require.NoError(t, db.Where("id = ?", "ds").Take(&source).Error)
			cursor, err := source.ParseSyncCursor()
			require.NoError(t, err)
			changeBytes, err := json.Marshal([]any{1, bindingID, "2", strings.Repeat("0", 64)})
			require.NoError(t, err)
			cursor.ConnectorCursor["changes"] = map[string]interface{}{
				bindingID: base64.RawURLEncoding.EncodeToString(changeBytes),
			}
			if scenario.finalPublished {
				old := insertNextcloudVersionTestKnowledge(t, db, "withdrawn-before-republish", "etag-v1",
					types.ParseStatusCompleted, "enabled")
				require.NoError(t, writeNextcloudETag(db, old, ""))
				require.NoError(t, db.Delete(old).Error)
				ready := insertNextcloudVersionTestKnowledge(t, db, "republished", "etag-v2",
					types.ParseStatusCompleted, "enabled")
				require.NoError(t, db.Model(&types.Knowledge{}).Where("id = ?", ready.ID).
					Update("file_name", "file.md").Error)
				require.NoError(t, db.Create(&nextcloudSourceVersion{
					TenantID: 7, KnowledgeBaseID: "kb",
					DataSourceID: "ds", ExternalID: "nextcloud:instance:77", DesiredETag: "etag-v2",
					CandidateKnowledgeID: ready.ID, State: "published", UpdatedAt: now,
				}).Error)
				cursor.ConnectorCursor["files"] = map[string]interface{}{bindingID: map[string]interface{}{
					"77": map[string]string{"etag": "etag-v2", "name": "file.md", "path": "file.md"},
				}}
			} else {
				old := insertNextcloudVersionTestKnowledge(t, db, "withdrawn", "etag-v1",
					types.ParseStatusCompleted, "enabled")
				require.NoError(t, writeNextcloudETag(db, old, ""))
				require.NoError(t, db.Delete(old).Error)
				require.NoError(t, db.Create(&nextcloudSourceVersion{
					TenantID: 7, KnowledgeBaseID: "kb",
					DataSourceID: "ds", ExternalID: "nextcloud:instance:77",
					State: "tombstone", UpdatedAt: now,
				}).Error)
				cursor.ConnectorCursor["files"] = map[string]interface{}{bindingID: map[string]interface{}{}}
				cursor.ConnectorCursor["tombstones"] = map[string]interface{}{bindingID: map[string]interface{}{
					"77": map[string]string{"etag": "etag-v1", "name": "file.md", "path": "file.md"},
				}}
			}
			raw, err := cursor.ToJSON()
			require.NoError(t, err)
			require.NoError(t, db.Model(&types.DataSource{}).Where("id = ?", "ds").
				Update("last_sync_cursor", raw).Error)

			claim, err := inbox.ClaimDispatch(ctx, "event-connection", now.Add(time.Second))
			require.NoError(t, err)
			require.Nil(t, claim)
			row := publicationDispatchRow(t, db)
			require.Equal(t, "retry", row.State)
			require.Equal(t, "broad_confirm:2", row.LastErrorCode)
			require.Zero(t, row.AppliedID, "one complete inventory cannot ACK file-scoped reconcile")

			claim, err = inbox.ClaimDispatch(ctx, "event-connection", now.Add(6*time.Second))
			require.NoError(t, err)
			require.NotNil(t, claim)
			require.Equal(t, int64(2), claim.TargetEventID)
			require.NoError(t, inbox.PrepareDispatch(ctx, *claim, "reconcile-second-log", now.Add(6*time.Second)))
			require.NoError(t, inbox.FinishDispatch(ctx, *claim, "reconcile-second-log", now.Add(6*time.Second)))
			finished := now.Add(7 * time.Second)
			require.NoError(t, db.Create(&types.SyncLog{
				ID: "reconcile-second-log", DataSourceID: "ds",
				TenantID: 7, Status: types.SyncLogStatusSuccess,
				StartedAt: now.Add(6 * time.Second), FinishedAt: &finished,
			}).Error)
			cursor.LastSyncTime = finished
			cursor.ConnectorCursor["last_reconcile_at"] = finished.Unix()
			raw, err = cursor.ToJSON()
			require.NoError(t, err)
			require.NoError(t, db.Model(&types.DataSource{}).Where("id = ?", "ds").
				Update("last_sync_cursor", raw).Error)

			claim, err = inbox.ClaimDispatch(ctx, "event-connection", now.Add(12*time.Second))
			require.NoError(t, err)
			require.Nil(t, claim)
			row = publicationDispatchRow(t, db)
			require.Equal(t, "idle", row.State)
			require.Equal(t, int64(2), row.AppliedID,
				"the latest published or tombstone state covers both reconcile receipts")
			var unapplied int64
			require.NoError(t, db.Table("nextcloud_event_inbox").
				Where("connection_id = ? AND state <> 'applied'", "event-connection").Count(&unapplied).Error)
			require.Zero(t, unapplied)
		})
	}
}

func TestNextcloudExcludedImportableUpsertNeedsTwoAbsentManifests(t *testing.T) {
	ctx := context.Background()
	db, inbox, now, bindingID := newNextcloudQueuedPublicationTest(t)
	// The signed event described an importable file, but the authoritative
	// publication manifest excluded it. There is no prior indexed version.
	var source types.DataSource
	require.NoError(t, db.Where("id = ?", "ds").Take(&source).Error)
	cursor, err := source.ParseSyncCursor()
	require.NoError(t, err)
	cursor.ConnectorCursor["files"] = map[string]interface{}{bindingID: map[string]interface{}{}}
	raw, err := cursor.ToJSON()
	require.NoError(t, err)
	require.NoError(t, db.Model(&types.DataSource{}).Where("id = ?", "ds").
		Update("last_sync_cursor", raw).Error)
	require.NoError(t, db.Exec(`UPDATE sync_logs SET status = 'success', finished_at = ?
		WHERE id = 'event-log'`, now).Error)
	claim, err := inbox.ClaimDispatch(ctx, "event-connection", now.Add(time.Second))
	require.NoError(t, err)
	require.Nil(t, claim)
	row := publicationDispatchRow(t, db)
	require.Equal(t, "retry", row.State)
	require.Equal(t, "broad_confirm:1", row.LastErrorCode)
	require.Zero(t, row.AppliedID)
	claim, err = inbox.ClaimDispatch(ctx, "event-connection", now.Add(6*time.Second))
	require.NoError(t, err)
	require.NotNil(t, claim)
	require.NoError(t, inbox.PrepareDispatch(ctx, *claim, "excluded-second-log", now.Add(6*time.Second)))
	require.NoError(t, inbox.FinishDispatch(ctx, *claim, "excluded-second-log", now.Add(6*time.Second)))
	finished := now.Add(7 * time.Second)
	require.NoError(t, db.Create(&types.SyncLog{
		ID: "excluded-second-log", DataSourceID: "ds",
		TenantID: 7, Status: types.SyncLogStatusSuccess,
		StartedAt: now.Add(6 * time.Second), FinishedAt: &finished,
	}).Error)
	cursor.LastSyncTime = finished
	cursor.ConnectorCursor["last_reconcile_at"] = finished.Unix()
	raw, err = cursor.ToJSON()
	require.NoError(t, err)
	require.NoError(t, db.Model(&types.DataSource{}).Where("id = ?", "ds").
		Update("last_sync_cursor", raw).Error)
	claim, err = inbox.ClaimDispatch(ctx, "event-connection", now.Add(12*time.Second))
	require.NoError(t, err)
	require.Nil(t, claim)
	row = publicationDispatchRow(t, db)
	require.Equal(t, "idle", row.State)
	require.Equal(t, int64(1), row.AppliedID)
}

func TestNextcloudExcludedPreviouslyPublishedFileRequiresWithdrawal(t *testing.T) {
	ctx := context.Background()
	db, inbox, now, bindingID := newNextcloudQueuedPublicationTest(t)
	ready := insertNextcloudVersionTestKnowledge(t, db, "formerly-published", "etag-v1",
		types.ParseStatusCompleted, "enabled")
	require.NoError(t, db.Model(&types.Knowledge{}).Where("id = ?", ready.ID).
		Update("file_name", "file.md").Error)
	require.NoError(t, db.Create(&nextcloudSourceVersion{
		TenantID: 7, KnowledgeBaseID: "kb",
		DataSourceID: "ds", ExternalID: "nextcloud:instance:77", DesiredETag: "etag-v1",
		CandidateKnowledgeID: ready.ID, State: "published", UpdatedAt: now,
	}).Error)
	var source types.DataSource
	require.NoError(t, db.Where("id = ?", "ds").Take(&source).Error)
	cursor, err := source.ParseSyncCursor()
	require.NoError(t, err)
	cursor.ConnectorCursor["files"] = map[string]interface{}{bindingID: map[string]interface{}{
		"77": map[string]string{"etag": "etag-v1", "name": "file.md", "path": "file.md"},
	}}
	cursor.ConnectorCursor["missing"] = map[string]interface{}{bindingID: map[string]interface{}{
		"77": true,
	}}
	raw, err := cursor.ToJSON()
	require.NoError(t, err)
	require.NoError(t, db.Model(&types.DataSource{}).Where("id = ?", "ds").
		Update("last_sync_cursor", raw).Error)
	require.NoError(t, db.Exec(`UPDATE sync_logs SET status = 'success', finished_at = ?
		WHERE id = 'event-log'`, now).Error)
	claim, err := inbox.ClaimDispatch(ctx, "event-connection", now.Add(time.Second))
	require.NoError(t, err)
	require.Nil(t, claim)
	row := publicationDispatchRow(t, db)
	require.Zero(t, row.AppliedID)
	require.Equal(t, "retry", row.State)
	require.Equal(t, "deletion_or_cursor_pending", row.LastErrorCode,
		"the first missing scan cannot withdraw a published file")

	// The retry runs the connector's second absence scan. It tombstones the
	// previously indexed version and withdraws the old knowledge row.
	claim, err = inbox.ClaimDispatch(ctx, "event-connection", now.Add(61*time.Second))
	require.NoError(t, err)
	require.NotNil(t, claim)
	require.NoError(t, inbox.PrepareDispatch(ctx, *claim, "withdrawn-first-complete-log", now.Add(61*time.Second)))
	require.NoError(t, inbox.FinishDispatch(ctx, *claim, "withdrawn-first-complete-log", now.Add(61*time.Second)))
	require.NoError(t, writeNextcloudETag(db, ready, ""))
	require.NoError(t, db.Delete(ready).Error)
	require.NoError(t, db.Model(&nextcloudSourceVersion{}).
		Where("tenant_id = ? AND knowledge_base_id = ? AND datasource_id = ? AND external_id = ?",
			7, "kb", "ds", "nextcloud:instance:77").
		Updates(map[string]any{
			"desired_etag": "", "candidate_knowledge_id": "",
			"state": "tombstone",
		}).Error)
	cursor.ConnectorCursor["files"] = map[string]interface{}{bindingID: map[string]interface{}{}}
	cursor.ConnectorCursor["missing"] = map[string]interface{}{bindingID: map[string]interface{}{}}
	cursor.ConnectorCursor["tombstones"] = map[string]interface{}{bindingID: map[string]interface{}{
		"77": map[string]string{"etag": "etag-v1", "name": "file.md", "path": "file.md"},
	}}
	firstComplete := now.Add(62 * time.Second)
	require.NoError(t, db.Create(&types.SyncLog{
		ID: "withdrawn-first-complete-log", DataSourceID: "ds",
		TenantID: 7, Status: types.SyncLogStatusSuccess,
		StartedAt: now.Add(61 * time.Second), FinishedAt: &firstComplete,
	}).Error)
	cursor.LastSyncTime = firstComplete
	cursor.ConnectorCursor["last_reconcile_at"] = firstComplete.Unix()
	raw, err = cursor.ToJSON()
	require.NoError(t, err)
	require.NoError(t, db.Model(&types.DataSource{}).Where("id = ?", "ds").
		Update("last_sync_cursor", raw).Error)
	claim, err = inbox.ClaimDispatch(ctx, "event-connection", now.Add(63*time.Second))
	require.NoError(t, err)
	require.Nil(t, claim)
	row = publicationDispatchRow(t, db)
	require.Equal(t, "retry", row.State)
	require.Equal(t, "broad_confirm:1", row.LastErrorCode)
	require.Zero(t, row.AppliedID)
	claim, err = inbox.ClaimDispatch(ctx, "event-connection", now.Add(68*time.Second))
	require.NoError(t, err)
	require.NotNil(t, claim)
	require.NoError(t, inbox.PrepareDispatch(ctx, *claim, "withdrawn-second-log", now.Add(68*time.Second)))
	require.NoError(t, inbox.FinishDispatch(ctx, *claim, "withdrawn-second-log", now.Add(68*time.Second)))
	finished := now.Add(69 * time.Second)
	require.NoError(t, db.Create(&types.SyncLog{
		ID: "withdrawn-second-log", DataSourceID: "ds",
		TenantID: 7, Status: types.SyncLogStatusSuccess,
		StartedAt: now.Add(68 * time.Second), FinishedAt: &finished,
	}).Error)
	cursor.LastSyncTime = finished
	cursor.ConnectorCursor["last_reconcile_at"] = finished.Unix()
	raw, err = cursor.ToJSON()
	require.NoError(t, err)
	require.NoError(t, db.Model(&types.DataSource{}).Where("id = ?", "ds").
		Update("last_sync_cursor", raw).Error)
	claim, err = inbox.ClaimDispatch(ctx, "event-connection", now.Add(74*time.Second))
	require.NoError(t, err)
	require.Nil(t, claim)
	row = publicationDispatchRow(t, db)
	require.Equal(t, "idle", row.State)
	require.Equal(t, int64(1), row.AppliedID)
}
