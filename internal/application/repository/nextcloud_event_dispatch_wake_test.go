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
)

func TestNextcloudQueuedDispatchWakesForNewHintAfterMatchingSyncCompletes(t *testing.T) {
	ctx := context.Background()
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

	ready := insertNextcloudVersionTestKnowledge(t, db, "wake-ready", "etag-v1",
		types.ParseStatusCompleted, "enabled")
	require.NoError(t, db.Model(&types.Knowledge{}).Where("id = ?", ready.ID).
		Update("file_name", "file.md").Error)
	require.NoError(t, db.Create(&nextcloudSourceVersion{
		TenantID: 7, KnowledgeBaseID: "kb",
		DataSourceID: "ds", ExternalID: "nextcloud:instance:77", DesiredETag: "etag-v1",
		CandidateKnowledgeID: ready.ID, State: "published", UpdatedAt: time.Now().UTC(),
	}).Error)
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
	startedAt := now.Add(-time.Minute)
	require.NoError(t, db.Create(&types.SyncLog{
		ID: "first-event-log", DataSourceID: "ds",
		TenantID: 7, Status: types.SyncLogStatusRunning, StartedAt: startedAt,
	}).Error)
	require.NoError(t, db.Exec(`INSERT INTO nextcloud_event_dispatch
		(connection_id, dispatched_id, target_event_id, state, last_sync_log_id, next_attempt_at)
		VALUES ('event-connection', 1, 1, 'queued', 'first-event-log', ?)`, now.Add(time.Minute)).Error)
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

	inbox := NewNextcloudEventInboxRepository(db)
	candidates, err := inbox.DispatchCandidates(ctx, now)
	require.NoError(t, err)
	require.Empty(t, candidates, "a completed scan alone must not queue duplicate work")
	require.NoError(t, db.Exec(("INSERT INTO nextcloud_event_inbox\n"+
		"\t\t(connection_id, event_id, payload_sha256, event_type,"+
		" file_id, etag, path, relative_path, state)\n"+
		"\t\tVALUES ('event-connection', 2, ?, 'upsert', 77, 'etag"+
		"-v2', '/Published/file.md', 'file.md', 'pending')"), strings.Repeat("1", 64)).Error)
	require.NoError(t, db.Exec(`UPDATE nextcloud_event_checkpoint SET received_id = 2
		WHERE connection_id = 'event-connection'`).Error)
	candidates, err = inbox.DispatchCandidates(ctx, now)
	require.NoError(t, err)
	require.Empty(t, candidates, "a running same-source sync must keep single-flight admission")

	require.NoError(t, db.Exec(`UPDATE sync_logs SET status = 'success'
		WHERE id = 'first-event-log'`).Error)
	candidates, err = inbox.DispatchCandidates(ctx, now)
	require.NoError(t, err)
	require.Empty(t, candidates, "success without a completion timestamp is not a finished task")
	require.NoError(t, db.Exec(`UPDATE sync_logs SET finished_at = ?, tenant_id = 8
		WHERE id = 'first-event-log'`, now).Error)
	candidates, err = inbox.DispatchCandidates(ctx, now)
	require.NoError(t, err)
	require.Empty(t, candidates, "another tenant's log cannot wake this connection")
	require.NoError(t, db.Exec(`UPDATE sync_logs SET tenant_id = 7
		WHERE id = 'first-event-log'`).Error)
	require.NoError(t, db.Exec(`UPDATE nextcloud_event_dispatch SET last_error_code = 'publication_pending'
		WHERE connection_id = 'event-connection'`).Error)
	candidates, err = inbox.DispatchCandidates(ctx, now)
	require.NoError(t, err)
	require.Empty(t, candidates, "publication waiting retains its existing retry schedule")
	require.NoError(t, db.Exec(`UPDATE nextcloud_event_dispatch SET last_error_code = ''
		WHERE connection_id = 'event-connection'`).Error)
	candidates, err = inbox.DispatchCandidates(ctx, now)
	require.NoError(t, err)
	require.Equal(t, []string{"event-connection"}, candidates,
		"new hints should wake on the next dispatcher tick, before the one-minute poll")

	claim, err := inbox.ClaimDispatch(ctx, "event-connection", now)
	require.NoError(t, err)
	require.NotNil(t, claim)
	require.Equal(t, int64(2), claim.TargetEventID)
	var dispatch nextcloudEventDispatchRow
	require.NoError(t, db.Table("nextcloud_event_dispatch").Where("connection_id = ?", "event-connection").
		Take(&dispatch).Error)
	require.Equal(t, int64(1), dispatch.AppliedID,
		"the first event must receive a complete publication proof before the second is claimed")
	require.Equal(t, "leased", dispatch.State)
	require.NoError(t, inbox.PrepareDispatch(ctx, *claim, "second-event-log", now))
	require.NoError(t, inbox.FinishDispatch(ctx, *claim, "second-event-log", now))
	require.NoError(t, db.Table("nextcloud_event_dispatch").Where("connection_id = ?", "event-connection").
		Take(&dispatch).Error)
	require.Equal(t, "queued", dispatch.State)
	require.Equal(t, int64(2), dispatch.DispatchedID)
	require.Equal(t, int64(1), dispatch.AppliedID)
	require.Equal(t, now.Add(nextcloudQueuedPollInterval), dispatch.NextAttemptAt)
}
