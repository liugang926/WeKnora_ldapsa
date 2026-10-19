package repository

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/utils"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

type nextcloudPausedParserFixture struct {
	db        *gorm.DB
	inbox     *NextcloudEventInboxRepository
	versions  *knowledgeRepository
	lease     NextcloudContentLease
	now       time.Time
	bindingID string
	secret    string
}

func newNextcloudPausedParserFixture(t *testing.T) nextcloudPausedParserFixture {
	t.Helper()
	db, inbox, now, bindingID := newNextcloudQueuedPublicationTest(t)
	versions := &knowledgeRepository{db: db, nextcloudPublicationCheck: func(
		context.Context, *types.DataSourceConfig, string, string, int64, string, string,
	) error {
		return nil
	}}
	old := insertNextcloudVersionTestKnowledge(t, db, "paused-v1", "",
		types.ParseStatusProcessing, "disabled")
	require.NoError(t, db.Model(old).Update("file_name", "file.md").Error)
	require.NoError(t, versions.StageNextcloudVersion(context.Background(), 7, "kb", "ds",
		"nextcloud:instance:77", "etag-v1", old.ID))
	store := NewNextcloudContentLeaseStore(db)
	lease, err := store.AcquireKnowledge(context.Background(), NextcloudContentScope{
		TenantID: 7, KnowledgeBaseID: "kb", KnowledgeID: old.ID,
		DataSourceID: "ds", ExternalID: "nextcloud:instance:77",
	}, NextcloudContentBuildLease, "paused-v1-worker", 5*time.Minute)
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.ReleaseLease(context.Background(), lease.ID) })
	require.NoError(t, db.Exec(`UPDATE sync_logs SET status = 'success', finished_at = ?
		WHERE id = 'event-log'`, now).Error)
	// The first minute has passed. Without an early wake, the old pending
	// parser would hold this new receipt until the next one-minute poll.
	claim, err := inbox.ClaimDispatch(context.Background(), "event-connection", now.Add(61*time.Second))
	require.NoError(t, err)
	require.Nil(t, claim)
	row := publicationDispatchRow(t, db)
	require.Equal(t, "publication_pending", row.LastErrorCode)
	require.Equal(t, now.Add(121*time.Second), row.NextAttemptAt)
	secret := strings.Repeat("s", 32)
	ciphertext, err := utils.EncryptAESGCM(secret, utils.GetAESKey())
	require.NoError(t, err)
	require.NoError(t, db.Exec(`UPDATE nextcloud_event_connections
		SET current_secret_ciphertext = ? WHERE connection_id = 'event-connection'`, ciphertext).Error)
	return nextcloudPausedParserFixture{
		db: db, inbox: inbox, versions: versions,
		lease: lease, now: now, bindingID: bindingID, secret: secret,
	}
}

func (f nextcloudPausedParserFixture) receiveSignedVersion(t *testing.T, eventID int64, etag string) {
	t.Helper()
	canonical := []byte("signed-nextcloud-parser-overlap-event")
	mac := hmac.New(sha256.New, []byte(f.secret))
	_, err := mac.Write(canonical)
	require.NoError(t, err)
	path, relative := "/Published/file.md", "file.md"
	previous := eventID - 1
	got, err := f.inbox.IngestSigned(context.Background(), SignedNextcloudEventBatch{
		ConnectionID: "event-connection", NextcloudInstanceID: "instance",
		BindingID: f.bindingID, KeyID: "test-key", Nonce: "nonce-parser-" + etag,
		Signature: mac.Sum(nil), CanonicalRequest: canonical,
		AfterEventID: previous, Now: time.Now().UTC(),
		Events: []NextcloudEventHint{{
			EventID:       eventID,
			PayloadSHA256: strings.Repeat("2", 64), EventType: "upsert",
			FileID: overlapFileID(77), ETag: &etag, Path: &path, RelativePath: &relative,
		}},
	})
	require.NoError(t, err)
	require.Equal(t, eventID, got)
}

func overlapFileID(id int64) *int64 { return &id }

func (f nextcloudPausedParserFixture) writeCursor(t *testing.T, eventID int64, etag string, at time.Time) {
	t.Helper()
	var source types.DataSource
	require.NoError(t, f.db.Where("id = 'ds'").Take(&source).Error)
	cursor, err := source.ParseSyncCursor()
	require.NoError(t, err)
	changeBytes, err := json.Marshal([]any{
		1, f.bindingID,
		strconv.FormatInt(eventID, 10), strings.Repeat("0", 64),
	})
	require.NoError(t, err)
	cursor.LastSyncTime = at
	cursor.ConnectorCursor["last_reconcile_at"] = at.Unix()
	cursor.ConnectorCursor["changes"] = map[string]interface{}{
		f.bindingID: base64.RawURLEncoding.EncodeToString(changeBytes),
	}
	cursor.ConnectorCursor["files"] = map[string]interface{}{f.bindingID: map[string]interface{}{
		"77": map[string]string{"etag": etag, "name": "file.md", "path": "file.md"},
	}}
	raw, err := cursor.ToJSON()
	require.NoError(t, err)
	require.NoError(t, f.db.Model(&types.DataSource{}).Where("id = 'ds'").
		Update("last_sync_cursor", raw).Error)
}

func TestNextcloudSignedV2OvertakesPausedV1AndAcksOnlyAfterV2Publication(t *testing.T) {
	f := newNextcloudPausedParserFixture(t)
	ctx := context.Background()
	f.receiveSignedVersion(t, 2, "etag-v2")
	wakeAt := f.now.Add(67 * time.Second)
	candidates, err := f.inbox.DispatchCandidates(ctx, wakeAt)
	require.NoError(t, err)
	require.Equal(t, []string{"event-connection"}, candidates,
		"new signed version must wake the dispatcher before its one-minute poll")
	claim, err := f.inbox.ClaimDispatch(ctx, "event-connection", wakeAt)
	require.NoError(t, err)
	require.NotNil(t, claim)
	require.Equal(t, int64(2), claim.TargetEventID)
	require.Zero(t, publicationDispatchRow(t, f.db).AppliedID)
	require.NoError(t, f.inbox.PrepareDispatch(ctx, *claim, "v2-event-log", wakeAt))
	require.NoError(t, f.inbox.FinishDispatch(ctx, *claim, "v2-event-log", wakeAt))
	finished := wakeAt.Add(2 * time.Second)
	require.NoError(t, f.db.Create(&types.SyncLog{
		ID: "v2-event-log", DataSourceID: "ds",
		TenantID: 7, Status: types.SyncLogStatusSuccess,
		StartedAt: wakeAt.Add(time.Second), FinishedAt: &finished,
	}).Error)
	newer := insertNextcloudVersionTestKnowledge(t, f.db, "v2-candidate", "",
		types.ParseStatusProcessing, "disabled")
	require.NoError(t, f.db.Model(newer).Update("file_name", "file.md").Error)
	require.NoError(t, f.versions.StageNextcloudVersion(ctx, 7, "kb", "ds",
		"nextcloud:instance:77", "etag-v2", newer.ID))
	f.writeCursor(t, 2, "etag-v2", finished)
	scope := NextcloudContentScope{
		TenantID: 7, KnowledgeBaseID: "kb",
		KnowledgeID: "paused-v1", DataSourceID: "ds", ExternalID: "nextcloud:instance:77",
	}
	err = f.db.Transaction(func(tx *gorm.DB) error {
		return NewNextcloudContentLeaseStore(f.db).ValidateBuildLeaseInTx(ctx, tx, f.lease, scope)
	})
	require.ErrorIs(t, err, ErrNextcloudContentLeaseDenied)
	require.NoError(t, f.db.Model(&types.Knowledge{}).Where("id = 'paused-v1'").
		Updates(map[string]any{
			"parse_status":  types.ParseStatusCompleted,
			"enable_status": "enabled",
		}).Error)
	published, err := f.versions.PublishNextcloudVersion(ctx, "paused-v1")
	require.NoError(t, err)
	require.False(t, published, "late V1 parse cannot publish after V2 Stage")
	claim, err = f.inbox.ClaimDispatch(ctx, "event-connection", wakeAt.Add(5*time.Second))
	require.NoError(t, err)
	require.Nil(t, claim)
	require.Zero(t, publicationDispatchRow(t, f.db).AppliedID,
		"V2 source scan alone is not publication evidence")
	require.NoError(t, f.db.Model(&types.Knowledge{}).Where("id = ?", newer.ID).
		Updates(map[string]any{
			"parse_status":  types.ParseStatusCompleted,
			"enable_status": "enabled",
		}).Error)
	published, err = f.versions.PublishNextcloudVersion(ctx, newer.ID)
	require.NoError(t, err)
	require.True(t, published)
	claim, err = f.inbox.ClaimDispatch(ctx, "event-connection", wakeAt.Add(10*time.Second))
	require.NoError(t, err)
	require.Nil(t, claim)
	require.Equal(t, int64(2), publicationDispatchRow(t, f.db).AppliedID)
	var unapplied int64
	require.NoError(t, f.db.Table("nextcloud_event_inbox").
		Where("connection_id = 'event-connection' AND state <> 'applied'").Count(&unapplied).Error)
	require.Zero(t, unapplied)
}

func TestNextcloudPendingParserOverlapBudgetBlocksThirdGeneration(t *testing.T) {
	f := newNextcloudPausedParserFixture(t)
	ctx := context.Background()
	f.receiveSignedVersion(t, 2, "etag-v2")
	firstAt := f.now.Add(67 * time.Second)
	claim, err := f.inbox.ClaimDispatch(ctx, "event-connection", firstAt)
	require.NoError(t, err)
	require.NotNil(t, claim)
	require.NoError(t, f.inbox.PrepareDispatch(ctx, *claim, "v2-event-log", firstAt))
	require.NoError(t, f.inbox.FinishDispatch(ctx, *claim, "v2-event-log", firstAt))
	finished := firstAt.Add(2 * time.Second)
	require.NoError(t, f.db.Create(&types.SyncLog{
		ID: "v2-event-log", DataSourceID: "ds",
		TenantID: 7, Status: types.SyncLogStatusSuccess,
		StartedAt: firstAt.Add(time.Second), FinishedAt: &finished,
	}).Error)
	newer := insertNextcloudVersionTestKnowledge(t, f.db, "v2-pending", "",
		types.ParseStatusProcessing, "disabled")
	require.NoError(t, f.db.Model(newer).Update("file_name", "file.md").Error)
	require.NoError(t, f.versions.StageNextcloudVersion(ctx, 7, "kb", "ds",
		"nextcloud:instance:77", "etag-v2", newer.ID))
	f.writeCursor(t, 2, "etag-v2", finished)
	claim, err = f.inbox.ClaimDispatch(ctx, "event-connection", firstAt.Add(5*time.Second))
	require.NoError(t, err)
	require.Nil(t, claim)
	f.receiveSignedVersion(t, 3, "etag-v3")
	secondAt := firstAt.Add(10 * time.Second)
	claim, err = f.inbox.ClaimDispatch(ctx, "event-connection", secondAt)
	require.NoError(t, err)
	require.Nil(t, claim, "a third parser must wait while V1 and V2 are unfinished")
	require.Zero(t, publicationDispatchRow(t, f.db).AppliedID)
	// Marking V1 terminal is insufficient while its paused worker still owns
	// a live build lease. Releasing that lease makes one slot available.
	require.NoError(t, f.db.Model(&types.Knowledge{}).Where("id = 'paused-v1'").
		Update("parse_status", types.ParseStatusFailed).Error)
	claim, err = f.inbox.ClaimDispatch(ctx, "event-connection", secondAt.Add(5*time.Second))
	require.NoError(t, err)
	require.Nil(t, claim)
	require.NoError(t, NewNextcloudContentLeaseStore(f.db).ReleaseLease(ctx, f.lease.ID))
	claim, err = f.inbox.ClaimDispatch(ctx, "event-connection", secondAt.Add(10*time.Second))
	require.NoError(t, err)
	require.NotNil(t, claim)
	require.Equal(t, int64(3), claim.TargetEventID)
	require.Zero(t, publicationDispatchRow(t, f.db).AppliedID)
}
