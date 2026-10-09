package repository

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/json"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func TestPostgresNextcloudIndexedWithdrawalFencesAndInventories(t *testing.T) {
	dsn := os.Getenv("WEKNORA_INDEXED_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("set WEKNORA_INDEXED_TEST_POSTGRES_DSN for disposable PostgreSQL")
	}
	t.Setenv("SYSTEM_AES_KEY", "0123456789abcdef0123456789abcdef")
	admin, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	require.NoError(t, err)
	adminSQL, err := admin.DB()
	require.NoError(t, err)
	defer func() { require.NoError(t, adminSQL.Close()) }()
	schema := "indexed_withdrawal_" + strings.ReplaceAll(uuid.NewString()[:8], "-", "")
	require.NoError(t, admin.Exec(`CREATE SCHEMA "`+schema+`"`).Error)
	defer admin.Exec(`DROP SCHEMA "` + schema + `" CASCADE`)
	scopedDSN := dsn + " search_path=" + schema
	if strings.Contains(dsn, "://") {
		u, err := url.Parse(dsn)
		require.NoError(t, err)
		q := u.Query()
		q.Set("search_path", schema)
		u.RawQuery = q.Encode()
		scopedDSN = u.String()
	}
	db, err := gorm.Open(postgres.Open(scopedDSN), &gorm.Config{})
	require.NoError(t, err)
	sqlDB, err := db.DB()
	require.NoError(t, err)
	defer func() { require.NoError(t, sqlDB.Close()) }()
	// Migration files contain several SQL statements. Use a separate simple
	// protocol connection for DDL; GORM's JSON fields need the normal pgx
	// parameter protocol for test fixture writes.
	migrationDB, err := gorm.Open(postgres.New(postgres.Config{
		DSN: scopedDSN, PreferSimpleProtocol: true,
	}), &gorm.Config{})
	require.NoError(t, err)
	migrationSQL, err := migrationDB.DB()
	require.NoError(t, err)
	defer func() { require.NoError(t, migrationSQL.Close()) }()
	require.NoError(t, db.Exec(`CREATE TABLE knowledge_bases (
		id TEXT PRIMARY KEY, tenant_id BIGINT NOT NULL, ever_had_nextcloud_source BOOLEAN NOT NULL DEFAULT TRUE,
		deleted_at TIMESTAMPTZ)`).Error)
	require.NoError(t, db.Exec(`INSERT INTO knowledge_bases (id, tenant_id) VALUES ('kb', 7), ('other-kb', 8)`).Error)
	require.NoError(t, db.AutoMigrate(&types.DataSource{}, &types.Knowledge{}, &types.Chunk{},
		&NextcloudSourcePairing{}, &NextcloudSourceDecommission{}, &NextcloudSourceRotation{},
		&nextcloudSourceVersion{}, &nextcloudGCJob{}, &nextcloudGCItem{}))
	// Production migrations use JSONB; GORM's portable test model defaults
	// to JSON and must be aligned before exercising PostgreSQL's barrier.
	require.NoError(t, db.Exec(`ALTER TABLE knowledges ALTER COLUMN metadata TYPE jsonb
		USING metadata::jsonb`).Error)
	require.NoError(t, db.Exec(`CREATE TABLE sync_logs (id TEXT PRIMARY KEY, data_source_id TEXT, status TEXT)`).Error)
	for _, migration := range []string{
		"000113_nextcloud_event_inbox.up.sql", "000114_nextcloud_event_dispatch.up.sql",
		"000130_nextcloud_event_hint_etag.up.sql",
	} {
		script, err := os.ReadFile("../../../migrations/versioned/" + migration)
		require.NoError(t, err)
		require.NoError(t, migrationDB.Exec(string(script)).Error)
	}
	require.NoError(t, db.Exec(`CREATE TABLE embeddings
		(id BIGSERIAL PRIMARY KEY, knowledge_base_id TEXT, knowledge_id TEXT)`).Error)
	cfg := &types.DataSourceConfig{
		Type:        types.ConnectorTypeNextcloud,
		Credentials: map[string]interface{}{"token": "test-token", "key_id": "pair_key"},
		ResourceIDs: []string{"binding"},
		Settings:    map[string]interface{}{"base_url": "https://nextcloud.example.test"},
	}
	blob, err := cfg.ToJSON()
	require.NoError(t, err)
	ds := &types.DataSource{
		ID: uuid.NewString(), TenantID: 7, KnowledgeBaseID: "kb",
		Type: types.ConnectorTypeNextcloud, Config: blob, Status: types.DataSourceStatusActive,
	}
	require.NoError(t, db.Create(ds).Error)
	require.NoError(t, db.Where("id = ?", ds.ID).Take(ds).Error)
	_, digest, _, err := NextcloudEventDataSourceIdentity(ds.Config)
	require.NoError(t, err)
	pair := NextcloudSourcePairing{
		OperationID: uuid.NewString(), TenantID: 7,
		KnowledgeBaseID: "kb", DataSourceID: ds.ID, InstanceID: "instance",
		BindingID: "binding", BaseURL: "https://nextcloud.example.test",
		ConfigSHA: digest, KeyID: "pair_key", State: "active",
	}
	require.NoError(t, db.Create(&pair).Error)
	ctx := context.Background()
	inbox := NewNextcloudEventInboxRepository(db)
	credential, err := inbox.Pair(ctx, NextcloudEventPairingIdentity{
		TenantID: 7, KnowledgeBaseID: "kb", DatasourceID: ds.ID,
		NextcloudInstanceID: pair.InstanceID, BindingID: pair.BindingID,
		DatasourceBaseURL: pair.BaseURL, DatasourceConfigSHA: pair.ConfigSHA,
	})
	require.NoError(t, err)
	knowledge := &types.Knowledge{
		ID: "file-1", TenantID: 7, KnowledgeBaseID: "kb",
		Channel: types.ConnectorTypeNextcloud, Type: "file", Title: "file", Source: "file",
		EnableStatus: "enabled", FilePath: "resource://local-copy", Metadata: types.JSON(
			`{"datasource_id":"` + ds.ID + `","nextcloud_etag":"etag-1","nextcloud_instance_id":"instance"}`),
	}
	require.NoError(t, db.Create(knowledge).Error)
	require.NoError(t, db.Exec(`INSERT INTO chunks (id, tenant_id, knowledge_base_id,
		knowledge_id, content, image_info) VALUES ('chunk-1', 7, 'kb', 'file-1', 'text',
		'[ {"url":"resource://image-1"} ]')`).Error)
	require.NoError(t, db.Exec(`INSERT INTO chunks (id, tenant_id, knowledge_base_id,
		knowledge_id, content, image_info) VALUES ('chunk-bad', 7, 'kb', 'file-1', 'text',
		'not-json')`).Error)
	require.NoError(t, db.Exec(`INSERT INTO embeddings (knowledge_base_id, knowledge_id)
		VALUES ('kb', 'file-1'), ('other-kb', 'other-file')`).Error)
	require.NoError(t, db.Exec(`INSERT INTO sync_logs (id, data_source_id, status)
		VALUES ('sync-1', ?, 'success')`, ds.ID).Error)
	version := nextcloudSourceVersion{
		TenantID: 7, KnowledgeBaseID: "kb", DataSourceID: ds.ID,
		ExternalID: "nextcloud:instance:1", DesiredETag: "etag-1", CandidateKnowledgeID: "file-1",
		State: "published",
	}
	require.NoError(t, db.Create(&version).Error)
	script, err := os.ReadFile("../../../migrations/versioned/000125_nextcloud_indexed_withdrawal.up.sql")
	require.NoError(t, err)
	require.NoError(t, migrationDB.Exec(string(script)).Error)
	for _, migration := range []string{
		"000126_nextcloud_source_revisions.up.sql",
		"000129_nextcloud_withdrawal_revision_inventory.up.sql",
	} {
		script, err := os.ReadFile("../../../migrations/versioned/" + migration)
		require.NoError(t, err)
		require.NoError(t, migrationDB.Exec(string(script)).Error, migration)
	}
	// A candidate can be hard-deleted before withdrawal. The append-only
	// revision still names it even when neither the current source row nor
	// the knowledge scan can discover that old generation.
	oldKnowledge := *knowledge
	oldKnowledge.ID = "old-hard-deleted"
	oldKnowledge.FilePath = "resource://old-copy"
	oldKnowledge.Metadata = types.JSON(
		`{"datasource_id":"` + ds.ID + ("\",\"nextcloud_etag\":\"etag-old\",\"nextcloud_instance_id\":\"" +
			"instance\"}"))
	require.NoError(t, db.Create(&oldKnowledge).Error)
	require.NoError(t, db.Exec(`UPDATE nextcloud_source_versions SET
		desired_etag = 'etag-old', candidate_knowledge_id = 'old-hard-deleted'
		WHERE datasource_id = ? AND external_id = 'nextcloud:instance:1'`, ds.ID).Error)
	require.NoError(t, db.Unscoped().Delete(&oldKnowledge).Error)
	require.NoError(t, db.Exec(`UPDATE nextcloud_source_versions SET
		desired_etag = 'etag-1', candidate_knowledge_id = 'file-1'
		WHERE datasource_id = ? AND external_id = 'nextcloud:instance:1'`, ds.ID).Error)
	require.NoError(t, db.Exec(`INSERT INTO nextcloud_source_revisions
		(tenant_id, knowledge_base_id, datasource_id, external_id, revision,
		 desired_etag, candidate_knowledge_id, state, source_updated_at,
		 legacy_history_unknown)
		VALUES (8, 'other-kb', 'other-ds', 'nextcloud:instance:1', 1,
		 'other-etag', 'other-candidate', 'published', CURRENT_TIMESTAMP, FALSE)`).Error)
	repo := NewNextcloudSourcePairingRepository(db)
	signedBatch := func(afterID, eventID int64) SignedNextcloudEventBatch {
		canonical := []byte("canonical event withdrawal test")
		mac := hmac.New(sha256.New, []byte(credential.Secret))
		_, _ = mac.Write(canonical)
		return SignedNextcloudEventBatch{
			ConnectionID: credential.ConnectionID, NextcloudInstanceID: pair.InstanceID,
			BindingID: pair.BindingID, KeyID: credential.KeyID, Nonce: uuid.NewString(),
			Signature: mac.Sum(nil), CanonicalRequest: canonical,
			AfterEventID: afterID, Events: []NextcloudEventHint{{
				EventID:       eventID,
				PayloadSHA256: strings.Repeat("a", 64), EventType: "updated",
			}},
			Now: time.Now().UTC(),
		}
	}
	receivedID, err := inbox.IngestSigned(ctx, signedBatch(0, 1))
	require.NoError(t, err, "an active source must accept a valid signed batch")
	require.EqualValues(t, 1, receivedID)
	require.NoError(t, db.Model(&types.DataSource{}).Where("id = ?", ds.ID).
		Update("status", types.DataSourceStatusPaused).Error)
	_, err = inbox.IngestSigned(ctx, signedBatch(1, 2))
	require.ErrorIs(t, err, ErrNextcloudEventScope,
		"a paused source must not accept new receipts even before withdrawal")
	require.NoError(t, db.Model(&types.DataSource{}).Where("id = ?", ds.ID).
		Update("status", types.DataSourceStatusActive).Error)
	proposed := NextcloudIndexedWithdrawal{
		OperationID: uuid.NewString(), PairOperationID: pair.OperationID,
		TenantID: 7, KnowledgeBaseID: "kb", DataSourceID: ds.ID, InstanceID: pair.InstanceID,
		BindingID: pair.BindingID, KeyID: pair.KeyID, PublicationEpoch: 3,
	}
	require.NoError(t, db.Exec(`INSERT INTO sync_logs (id, data_source_id, status)
		VALUES ('sync-running', ?, 'running')`, ds.ID).Error)
	_, err = repo.BeginIndexedSourceWithdrawal(ctx, pair, proposed)
	require.ErrorIs(t, err, ErrNextcloudSourcePairingConflict,
		"an in-flight sync must finish before the durable withdrawal")
	require.NoError(t, db.Exec(`UPDATE sync_logs SET status = 'success'
		WHERE id = 'sync-running'`).Error)
	require.NoError(t, db.Exec(`UPDATE nextcloud_event_connections
		SET nextcloud_instance_id = 'wrong-instance' WHERE connection_id = ?`,
		credential.ConnectionID).Error)
	_, err = repo.BeginIndexedSourceWithdrawal(ctx, pair, proposed)
	require.ErrorIs(t, err, ErrNextcloudSourcePairingConflict,
		"an active event connection outside the exact source tuple must block withdrawal")
	require.NoError(t, db.Exec(`UPDATE nextcloud_event_connections
		SET nextcloud_instance_id = ? WHERE connection_id = ?`,
		pair.InstanceID, credential.ConnectionID).Error)
	// A receiver can hold the connection before it requests the KB lock.
	// Withdrawal must fail promptly and leave no partial pause, then retry.
	holdConnection := db.Begin()
	require.NoError(t, holdConnection.Error)
	require.NoError(t, holdConnection.Exec(`SELECT 1 FROM nextcloud_event_connections
		WHERE connection_id = ? FOR UPDATE`, credential.ConnectionID).Error)
	_, err = repo.BeginIndexedSourceWithdrawal(ctx, pair, proposed)
	require.ErrorIs(t, err, ErrNextcloudSourcePairingConflict)
	require.NoError(t, db.Where("id = ?", ds.ID).Take(ds).Error)
	require.Equal(t, types.DataSourceStatusActive, ds.Status)
	require.NoError(t, holdConnection.Rollback().Error)
	// A SQL writer admitted before the withdrawal holds the same KB row.
	// Withdrawal must wait, then inventory its committed copy.
	hold := db.Begin()
	require.NoError(t, hold.Error)
	require.NoError(t, hold.Exec(`INSERT INTO chunks (id, tenant_id, knowledge_base_id,
		knowledge_id, content) VALUES ('chunk-race', 7, 'kb', 'file-1', 'raced')`).Error)
	type begun struct {
		row NextcloudIndexedWithdrawal
		err error
	}
	beginDone := make(chan begun, 1)
	go func() {
		result, err := repo.BeginIndexedSourceWithdrawal(ctx, pair, proposed)
		beginDone <- begun{result, err}
	}()
	select {
	case <-beginDone:
		t.Fatal("withdrawal bypassed the in-flight KB writer")
	case <-time.After(100 * time.Millisecond):
	}
	require.NoError(t, hold.Commit().Error)
	var outcome begun
	select {
	case outcome = <-beginDone:
	case <-time.After(5 * time.Second):
		t.Fatal("withdrawal did not complete after the writer")
	}
	row, err := outcome.row, outcome.err
	require.NoError(t, err)
	require.Equal(t, "withdrawn_inventory_incomplete", row.State)
	require.NoError(t, db.Where("id = ?", ds.ID).Take(ds).Error)
	require.Equal(t, types.DataSourceStatusPaused, ds.Status)
	var eventState struct {
		Status string
	}
	require.NoError(t, db.Table("nextcloud_event_connections").Select("status").
		Where("connection_id = ?", credential.ConnectionID).Take(&eventState).Error)
	require.Equal(t, "revoked", eventState.Status)
	var dispatchState struct {
		State         string
		LastErrorCode string
		LeaseToken    *string
		LeaseUntil    *time.Time
	}
	require.NoError(t, db.Table("nextcloud_event_dispatch").
		Select("state, last_error_code, lease_token, lease_until").
		Where("connection_id = ?", credential.ConnectionID).Take(&dispatchState).Error)
	require.Equal(t, "blocked", dispatchState.State)
	require.Equal(t, "source_withdrawn", dispatchState.LastErrorCode)
	require.Nil(t, dispatchState.LeaseToken)
	require.Nil(t, dispatchState.LeaseUntil)
	_, err = inbox.IngestSigned(ctx, signedBatch(1, 2))
	require.ErrorIs(t, err, ErrNextcloudEventUnauthorized,
		"a signed batch must not receive a 202 after logical withdrawal")
	var checkpoint nextcloudEventCheckpoint
	require.NoError(t, db.Table("nextcloud_event_checkpoint").
		Where("connection_id = ?", credential.ConnectionID).Take(&checkpoint).Error)
	require.EqualValues(t, 1, checkpoint.ReceivedID)
	var inboxCount int64
	require.NoError(t, db.Table("nextcloud_event_inbox").
		Where("connection_id = ?", credential.ConnectionID).Count(&inboxCount).Error)
	require.EqualValues(t, 1, inboxCount)
	require.NoError(t, db.Where("id = ?", knowledge.ID).Take(knowledge).Error)
	require.Equal(t, "disabled", knowledge.EnableStatus)
	meta, err := nextcloudMetadata(knowledge)
	require.NoError(t, err)
	require.Empty(t, nextcloudMetadataString(meta, "nextcloud_etag"))
	status, err := repo.RefreshIndexedWithdrawalInventory(ctx, row)
	require.NoError(t, err)
	require.False(t, status.InventoryComplete)
	require.True(t, status.LogicalWithdrawn)
	require.EqualValues(t, 15, status.ObservedItems)
	var oldRevisionRef string
	require.NoError(t, db.Table("nextcloud_indexed_withdrawal_items").
		Select("object_ref").Where("operation_id = ? AND kind = ? AND knowledge_id = ?",
		row.OperationID, "source_revision", oldKnowledge.ID).Row().Scan(&oldRevisionRef))
	var revisionIdentity []any
	require.NoError(t, json.Unmarshal([]byte(oldRevisionRef), &revisionIdentity))
	require.Equal(t, []any{"nextcloud:instance:1", float64(2)}, revisionIdentity)
	var otherRevision int64
	require.NoError(t, db.Model(&nextcloudIndexedWithdrawalItem{}).
		Where("operation_id = ? AND kind = ? AND knowledge_id = ?", row.OperationID,
			"source_revision", "other-candidate").Count(&otherRevision).Error)
	require.Zero(t, otherRevision, "another tenant/KB must not enter the withdrawal inventory")
	var invalid int64
	require.NoError(t, db.Model(&nextcloudIndexedWithdrawalItem{}).
		Where("operation_id = ? AND kind = ? AND object_ref = ?", row.OperationID,
			"invalid_image_inventory", "chunk-bad").Count(&invalid).Error)
	require.EqualValues(t, 1, invalid, "malformed image metadata must retain a blocker")
	var crossTenant int64
	require.NoError(t, db.Model(&nextcloudIndexedWithdrawalItem{}).
		Where("operation_id = ? AND object_ref = ?", row.OperationID, "2").Count(&crossTenant).Error)
	require.Zero(t, crossTenant)
	_, err = repo.RefreshIndexedWithdrawalInventory(ctx, row)
	require.NoError(t, err)
	var count int64
	require.NoError(t, db.Model(&nextcloudIndexedWithdrawalItem{}).
		Where("operation_id = ?", row.OperationID).Count(&count).Error)
	require.EqualValues(t, 15, count, "retry must not double-count copies")
	lateJob := nextcloudGCJob{
		ID: uuid.NewString(), TenantID: 7,
		KnowledgeBaseID: "kb", DataSourceID: ds.ID,
		ExternalID: "nextcloud:instance:1", KnowledgeID: "file-1",
		State: "pending", Reason: "retired",
	}
	require.NoError(t, db.Create(&lateJob).Error)
	require.NoError(t, db.Create(&nextcloudGCItem{
		JobID: lateJob.ID,
		Kind:  "source_file", ObjectRef: knowledge.FilePath, State: "pending",
	}).Error)
	claim, err := NewNextcloudGCStore(db).claimObject(ctx, lateJob.ID,
		"source_file", knowledge.FilePath, time.Now().UTC())
	require.NoError(t, err)
	require.Equal(t, "source_withdrawal_gc_paused", claim.code)
	status, err = repo.RefreshIndexedWithdrawalInventory(ctx, row)
	require.NoError(t, err)
	require.EqualValues(t, 17, status.ObservedItems,
		"a retry must capture a later GC job and exact item without authorizing deletion")
	_, err = repo.BeginIndexedSourceWithdrawal(ctx, pair, proposed)
	require.NoError(t, err)
	require.Error(t, db.Exec(`UPDATE nextcloud_event_connections SET status = 'active'
		WHERE connection_id = ?`, credential.ConnectionID).Error,
		"a withdrawn source connection must not be revived")
	wrong := proposed
	wrong.OperationID = uuid.NewString()
	_, err = repo.BeginIndexedSourceWithdrawal(ctx, pair, wrong)
	require.ErrorIs(t, err, ErrNextcloudSourcePairingConflict)
	require.Error(t, db.Exec(`UPDATE data_sources SET status = 'active' WHERE id = ?`, ds.ID).Error)
	require.Error(t, db.Exec(`INSERT INTO chunks (id, tenant_id, knowledge_base_id,
		knowledge_id, content) VALUES ('late', 7, 'kb', 'file-1', 'late')`).Error)
	require.Error(t, db.Exec(`INSERT INTO embeddings (knowledge_base_id, knowledge_id)
		VALUES ('kb', 'file-1')`).Error)
	require.NoError(t, db.Exec(`INSERT INTO embeddings (knowledge_base_id, knowledge_id)
		VALUES ('other-kb', 'other-file')`).Error)
	require.Error(t, db.Exec(`UPDATE knowledges SET knowledge_base_id = 'other-kb'
		WHERE id = 'file-1'`).Error, "old KB scope must be checked on a move")
	require.Error(t, db.Exec(`UPDATE chunks SET knowledge_base_id = 'other-kb'
		WHERE id = 'chunk-1'`).Error, "old KB scope must be checked on a move")
	require.Error(t, db.Exec(`UPDATE embeddings SET knowledge_base_id = 'other-kb'
		WHERE knowledge_base_id = 'kb'`).Error, "old KB scope must be checked on a move")
	require.Error(t, db.Exec(`UPDATE embeddings SET knowledge_base_id = 'kb'
		WHERE knowledge_base_id = 'other-kb'`).Error, "new KB scope must be checked on a move")
	require.Error(t, db.Exec(`UPDATE nextcloud_source_versions SET datasource_id = 'other-source'
		WHERE datasource_id = ?`, ds.ID).Error, "old source scope must be checked on a move")
	require.Error(t, db.Exec(`UPDATE nextcloud_event_connections SET datasource_id = 'other-source'
		WHERE datasource_id = ?`, ds.ID).Error, "old event source scope must be checked on a move")
	require.Error(t, db.Exec(`UPDATE sync_logs SET data_source_id = 'other-source'
		WHERE data_source_id = ?`, ds.ID).Error, "old sync source scope must be checked on a move")
	require.Error(t, db.Exec(`DELETE FROM knowledges WHERE id = 'file-1'`).Error)
	require.Error(t, db.Exec(`DELETE FROM nextcloud_indexed_withdrawals
		WHERE operation_id = ?`, row.OperationID).Error)
	require.Error(t, db.Create(&NextcloudSourceDecommission{
		OperationID: uuid.NewString(), PairOperationID: pair.OperationID,
		TenantID: pair.TenantID, KnowledgeBaseID: pair.KnowledgeBaseID,
		DataSourceID: pair.DataSourceID, InstanceID: pair.InstanceID,
		BindingID: pair.BindingID, KeyID: pair.KeyID,
		State: "prepared",
	}).Error, "an indexed withdrawal must never acquire an empty ACK")
	// A legacy active event connection beside a durable withdrawal can still
	// be revoked by an administrator. The exception must not permit rotating
	// its key or changing its source while making that transition.
	otherSource := &types.DataSource{
		ID: uuid.NewString(), TenantID: 8,
		KnowledgeBaseID: "other-kb", Type: types.ConnectorTypeNextcloud,
		Config: blob, Status: types.DataSourceStatusActive,
	}
	require.NoError(t, db.Create(otherSource).Error)
	otherPair := NextcloudSourcePairing{
		OperationID: uuid.NewString(), TenantID: 8,
		KnowledgeBaseID: "other-kb", DataSourceID: otherSource.ID,
		InstanceID: "other-instance", BindingID: "binding", BaseURL: pair.BaseURL,
		ConfigSHA: digest, KeyID: "pair_key", State: "active",
	}
	require.NoError(t, db.Create(&otherPair).Error)
	otherCredential, err := inbox.Pair(ctx, NextcloudEventPairingIdentity{
		TenantID: 8, KnowledgeBaseID: otherPair.KnowledgeBaseID,
		DatasourceID:        otherPair.DataSourceID,
		NextcloudInstanceID: otherPair.InstanceID, BindingID: otherPair.BindingID,
		DatasourceBaseURL: otherPair.BaseURL, DatasourceConfigSHA: otherPair.ConfigSHA,
	})
	require.NoError(t, err)
	require.NoError(t, db.Create(&NextcloudIndexedWithdrawal{
		OperationID: uuid.NewString(), PairOperationID: otherPair.OperationID,
		TenantID: 8, KnowledgeBaseID: otherPair.KnowledgeBaseID,
		DataSourceID: otherPair.DataSourceID, InstanceID: otherPair.InstanceID,
		BindingID: otherPair.BindingID, KeyID: otherPair.KeyID,
		PublicationEpoch: 1, State: "withdrawn_inventory_incomplete",
	}).Error)
	require.Error(t, db.Exec(`UPDATE nextcloud_event_connections
		SET status = 'revoked', current_key_id = 'changed'
		WHERE connection_id = ?`, otherCredential.ConnectionID).Error,
		"emergency revoke must not smuggle a key change")
	require.NoError(t, inbox.Revoke(ctx, 8, otherSource.ID))
	require.NoError(t, db.Table("nextcloud_event_connections").Select("status").
		Where("connection_id = ?", otherCredential.ConnectionID).Take(&eventState).Error)
	require.Equal(t, "revoked", eventState.Status)
	require.Error(t, db.Exec(`UPDATE nextcloud_event_connections SET status = 'active'
		WHERE connection_id = ?`, otherCredential.ConnectionID).Error)
	require.NoError(t, db.Exec(`DROP TABLE embeddings`).Error)
	status, err = repo.RefreshIndexedWithdrawalInventory(ctx, row)
	require.NoError(t, err, "missing optional local table must retain a blocker, not fail refresh")
	require.EqualValues(t, 18, status.ObservedItems)
	require.False(t, status.InventoryComplete)
	down, err := os.ReadFile("../../../migrations/versioned/000129_nextcloud_withdrawal_revision_inventory.down.sql")
	require.NoError(t, err)
	require.ErrorContains(t, migrationDB.Exec(string(down)).Error,
		"nextcloud_withdrawal_revision_inventory_requires_audit_export")
}
