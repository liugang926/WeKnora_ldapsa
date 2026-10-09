package service

import (
	"context"
	"encoding/json"
	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func messageBodyIntegrationFixture(t *testing.T, pg bool) (*messageService, *knowledgeService, *repository.KnowledgeInputProvenanceStore, *gorm.DB, context.Context, *types.Session, *repository.OriginalBodyRetentionStore) {
	t.Helper()
	var messages *messageService
	var kg *knowledgeService
	var store *repository.KnowledgeInputProvenanceStore
	var db *gorm.DB
	var ctx context.Context
	var session *types.Session
	if !pg {
		messages, kg, store, db, ctx, session = messageMaterialActualFixture(t)
	} else {
		kg, store, db, ctx = faqPostgresActualFixture(t)
		kg.graphEngine = parentChildGraphRepo{}
		require.NoError(t, db.Exec("CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\"").Error)
		require.NoError(t, db.Exec("ALTER TABLE messages ADD COLUMN artifacts JSONB").Error)
		for _, name := range []string{"000103_message_artifacts_table.up.sql", "000107_message_artifacts_deleted_at.up.sql", "000127_nextcloud_content_leases.up.sql", "000151_message_original_materials.up.sql"} {
			ddl, e := os.ReadFile("../../../migrations/versioned/" + name)
			require.NoError(t, e)
			require.NoError(t, db.Exec(string(ddl)).Error)
		}
		sql, e := db.DB()
		require.NoError(t, e)
		sql.SetMaxOpenConns(1)
		sql.SetMaxIdleConns(1)
		ctx = context.WithValue(ctx, types.UserIDContextKey, types.CallerFromContext(ctx).UserID)
		kbs := &knowledgeBaseService{repo: repository.NewKnowledgeBaseRepository(db)}
		groups := NewGroupAccessService(repository.NewGroupAccessRepository(db))
		guard := access.NewNextcloudSourceLineageGuard(kg.repo.(access.NextcloudLineageSnapshotLookup), kbs, nil, nil, groups, nil, store)
		messages = &messageService{messageRepo: repository.NewMessageRepository(db), sessionRepo: repository.NewSessionRepository(db), sourceLineageGuard: guard, kbService: kbs, knowService: kg}
		ConfigureMessageMaterialProvenance(messages, store)
		session = &types.Session{ID: "actual-body-message-session", TenantID: 7, UserID: types.CallerFromContext(ctx).UserID}
		require.NoError(t, db.Create(session).Error)
	}
	path := "../../../migrations/sqlite/000071_original_body_retention.up.sql"
	if pg {
		path = "../../../migrations/versioned/000152_original_body_retention.up.sql"
	}
	ddl, e := os.ReadFile(path)
	require.NoError(t, e)
	require.NoError(t, db.Exec(string(ddl)).Error)
	require.NoError(t, repository.InstallOriginalBodyRetentionTriggers(db))
	require.NoError(t, repository.InstallOriginalBodyRetentionCallbacks(db))
	dir := t.TempDir()
	require.NoError(t, os.Chmod(dir, 0700))
	key := filepath.Join(dir, "key")
	pin := filepath.Join(dir, "pin")
	require.NoError(t, os.WriteFile(key, []byte(strings.Repeat("owned-message-body-key-", 3)), 0600))
	t.Setenv("WEKNORA_ORIGINAL_BODY_JOURNAL_DIRECTORY", dir)
	t.Setenv("WEKNORA_ORIGINAL_BODY_JOURNAL_KEY", key)
	t.Setenv("WEKNORA_ORIGINAL_BODY_JOURNAL_PIN", pin)
	body := repository.NewOriginalBodyRetentionStore(db)
	require.NoError(t, body.InitializeJournal())
	return messages, kg, store, db, ctx, session, body
}
func runMessageBodyIntegration(t *testing.T, pg bool) {
	messages, _, store, db, ctx, session, body := messageBodyIntegrationFixture(t, pg)
	checks := 0
	repository.ConfigureMessageOriginalBodyAvailability(store, func(callCtx context.Context, tx *gorm.DB, table, id string) error {
		_, insideTx := tx.Statement.ConnPool.(gorm.TxCommitter)
		require.True(t, insideTx, "original availability must stay on the caller's existing transaction")
		require.Equal(t, "message_generation_origins", table)
		require.NotEmpty(t, id)
		checks++
		return repository.CheckOriginalBodyAvailableInTx(callCtx, tx, table, id)
	})
	user, assistant := produceActualMessagePair(t, messages, ctx, session)
	var origins []struct{ ID, MessageID, InitialInput string }
	require.NoError(t, db.Raw("SELECT id,message_id,initial_input FROM message_generation_origins WHERE session_id=? ORDER BY message_id", session.ID).Scan(&origins).Error)
	require.Len(t, origins, 2)
	for _, origin := range origins {
		require.Contains(t, origin.InitialInput, "_original_body_version")
		require.NotContains(t, origin.InitialInput, "new actual original question")
	}
	// The body scope is the immutable message/session. Linking its actual indexed
	// knowledge is a separate operation and must neither re-sign nor invalidate it.
	require.NoError(t, db.Model(&types.Message{}).Where("id=?", assistant.ID).Update("knowledge_id", "unconsumed-index-pointer").Error)
	request, e := store.CommittedMessageParentRequest(ctx, 7, session.ID, assistant.ID)
	require.NoError(t, e)
	var payloadReads []string
	require.NoError(t, db.Callback().Query().After("gorm:query").Register("body_message_payload_observer", func(tx *gorm.DB) {
		if tx.Statement.Table == "original_body_payloads" {
			payloadReads = append(payloadReads, tx.Statement.SQL.String())
		}
	}))
	defer db.Callback().Query().Remove("body_message_payload_observer")
	request, e = store.CommittedMessageParentRequest(ctx, 7, session.ID, assistant.ID)
	require.NoError(t, e)
	require.Empty(t, payloadReads, "metadata request resolution must not read the vault")
	guard, e := messages.beginMessageMaterialRead(ctx, request)
	require.NoError(t, e)
	require.NoError(t, guard.Recheck())
	guard.Close()
	for _, query := range payloadReads {
		require.NotContains(t, query, "SELECT *")
		require.NotContains(t, query, "payload\"")
		require.Contains(t, query, "state")
	}
	// A read of the actual original body is explicit and resolves the exact bytes.
	var origin struct{ ID, InitialInput string }
	require.NoError(t, db.Table("message_generation_origins").Where("message_id=?", user.ID).Take(&origin).Error)
	var decoded types.Message
	require.NoError(t, json.Unmarshal([]byte(origin.InitialInput), &decoded))
	if decoded.Content == "" {
		var envelope struct{ Body *types.Message }
		require.NoError(t, json.Unmarshal([]byte(origin.InitialInput), &envelope))
		require.NotNil(t, envelope.Body)
		decoded = *envelope.Body
	}
	require.Equal(t, user.Content, decoded.Content)
	n, e := body.PurgeDue(context.Background(), 100)
	require.NoError(t, e)
	require.Zero(t, n, "current saved messages remain owned")
	var bodyID string
	require.NoError(t, db.Raw("SELECT body_id FROM original_body_field_heads WHERE source_table='message_generation_origins' AND row_key=?", origin.ID).Scan(&bodyID).Error)
	require.NotEmpty(t, bodyID)
	actualLease, e := store.ContentLeaseStore().AcquireKBRead(ctx, 7, "kb", "actual-message-material-scope", time.Minute)
	require.NoError(t, e)
	lease, e := body.AcquireLease(context.Background(), bodyID, "read", "actual-message-original-reader", time.Minute)
	require.NoError(t, e)
	deleted := "datetime('now','-2 hours')"
	if pg {
		deleted = "CURRENT_TIMESTAMP-INTERVAL '2 hours'"
	}
	require.NoError(t, db.Exec("UPDATE messages SET deleted_at="+deleted+" WHERE session_id=?", session.ID).Error)
	_, e = body.PurgeDue(context.Background(), 100)
	require.NoError(t, e)
	var state string
	require.NoError(t, db.Raw("SELECT state FROM original_body_payloads WHERE id=?", bodyID).Scan(&state).Error)
	require.Equal(t, "live", state)
	require.NoError(t, body.ReleaseLease(context.Background(), lease))
	_, e = body.PurgeDue(context.Background(), 100)
	require.NoError(t, e)
	require.NoError(t, db.Raw("SELECT state FROM original_body_payloads WHERE id=?", bodyID).Scan(&state).Error)
	require.Equal(t, "live", state, "immutable message admission KB lease still protects the original")
	require.NoError(t, store.ContentLeaseStore().ReleaseLease(context.Background(), actualLease.ID))
	n, e = body.PurgeDue(context.Background(), 100)
	require.NoError(t, e)
	require.Positive(t, n)
	// Roll back only the message/source bytes. External purge evidence remains.
	require.NoError(t, db.Exec("UPDATE messages SET deleted_at=NULL,content=? WHERE id=?", user.Content, user.ID).Error)
	var raw string
	require.NoError(t, db.Raw("SELECT initial_input FROM message_generation_origins WHERE id=?", origin.ID).Scan(&raw).Error)
	require.Contains(t, raw, "_original_body_version")
	require.ErrorIs(t, repository.CheckOriginalBodyAvailableInTx(ctx, db, "message_generation_origins", origin.ID), repository.ErrOriginalBodyUnavailable)
	_, e = messages.beginMessageMaterialRead(ctx, types.MessageMaterialReadRequest{TenantID: 7, SessionID: session.ID, MessageIDs: []string{user.ID}})
	require.Error(t, e)
	require.Error(t, messages.UpdateMessage(ctx, user), "a current copied body cannot re-sign the unavailable original")
	require.Positive(t, checks)
}
func TestMessageBodyIntegrationSQLite(t *testing.T)   { runMessageBodyIntegration(t, false) }
func TestPostgresMessageBodyIntegration(t *testing.T) { runMessageBodyIntegration(t, true) }
