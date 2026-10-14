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

func messageBodyIntegrationFixture(t *testing.T, pg bool, before ...any) (*messageService, *knowledgeService, *repository.KnowledgeInputProvenanceStore, *gorm.DB, context.Context, *types.Session, *repository.OriginalBodyRetentionStore) {
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
		// Extensions must outlive an individual fixture schema, including a
		// prior terminal test timeout that left its schema behind.
		require.NoError(t, db.Exec(`CREATE EXTENSION IF NOT EXISTS "uuid-ossp" WITH SCHEMA public`).Error)
		require.NoError(t, db.Exec(`ALTER EXTENSION "uuid-ossp" SET SCHEMA public`).Error)
		require.NoError(t, db.Exec(`CREATE FUNCTION uuid_generate_v4() RETURNS uuid LANGUAGE sql AS $$ SELECT public.uuid_generate_v4() $$`).Error)
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
	if len(before) > 0 {
		switch hook := before[0].(type) {
		case func(*gorm.DB, *repository.KnowledgeInputProvenanceStore, context.Context):
			hook(db, store, ctx)
		case func(*messageService, *gorm.DB, *repository.KnowledgeInputProvenanceStore, context.Context, *types.Session):
			hook(messages, db, store, ctx, session)
		default:
			t.Fatal("unsupported actual before-body fixture hook")
		}
	}
	path := "../../../migrations/sqlite/000071_original_body_retention.up.sql"
	if pg {
		path = "../../../migrations/versioned/000152_original_body_retention.up.sql"
	}
	ddl, e := os.ReadFile(path)
	require.NoError(t, e)
	require.NoError(t, db.Exec(string(ddl)).Error)
	installConfigurationOriginalBodyFixtureSchema(t, db, pg)
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
	inventoryKnownConfigurationOriginalBodyFixture(t, ctx, db, store)
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

func runMessageBodyNamespaceABA(t *testing.T, pg bool) {
	messages, _, store, db, ctx, session, body := messageBodyIntegrationFixture(t, pg, func(db *gorm.DB, store *repository.KnowledgeInputProvenanceStore, ctx context.Context) {
		if !db.Migrator().HasTable("knowledge_display_versions") {
			ddl, e := os.ReadFile("../../../migrations/sqlite/000059_knowledge_collection_display_inputs.up.sql")
			require.NoError(t, e)
			require.NoError(t, db.Exec(string(ddl)).Error)
		}
		require.NoError(t, db.Create(&types.KnowledgeBase{ID: "body-cross-tenant-kb", TenantID: 8, Name: "shared scope fixture", Type: types.KnowledgeBaseTypeDocument}).Error)
		target := &types.KnowledgeBase{ID: "body-new-namespace", TenantID: 7, Name: "new namespace", Type: types.KnowledgeBaseTypeDocument}
		require.NoError(t, store.CreateKnowledgeBaseWithInput(repository.WithIndependentKnowledgeBaseNameInput(repository.WithIndependentKnowledgeInput(ctx), "", target.Name), target))
		source := &types.Knowledge{ID: "body-knowledge-reference", TenantID: 7, KnowledgeBaseID: "kb", Type: types.KnowledgeTypeManual, Title: "original ordinary reference", Metadata: types.JSON(`{}`), CustomMetadata: types.JSON(`{}`)}
		_, e := store.CreateKnowledgeWithInput(repository.WithIndependentKnowledgeInput(ctx), source, "actual ordinary original")
		require.NoError(t, e)
	})
	user, e := messages.CreateMessage(ctx, &types.Message{SessionID: session.ID, RequestID: "body-namespace-request", Role: "user", Content: "actual only-knowledge-id reference", IsCompleted: true, ExecutionContext: types.MessageExecutionContext{KnowledgeIDs: []string{"body-knowledge-reference"}, KnowledgeBaseIDs: []string{"body-cross-tenant-kb"}, AgentKnowledgeBaseIDs: []string{"body-cross-tenant-kb"}}})
	require.NoError(t, e)
	var bodyID string
	require.NoError(t, db.Raw("SELECT h.body_id FROM original_body_field_heads h JOIN message_generation_origins g ON g.id=h.row_key WHERE h.source_table='message_generation_origins' AND g.message_id=?", user.ID).Scan(&bodyID).Error)
	var stored int64
	require.NoError(t, db.Table("original_body_lease_scopes").Where("body_id=? AND tenant_id=7 AND knowledge_base_id='kb'", bodyID).Count(&stored).Error)
	require.Equal(t, int64(1), stored)
	require.NoError(t, db.Table("original_body_lease_scopes").Where("body_id=? AND tenant_id=8 AND knowledge_base_id='body-cross-tenant-kb'", bodyID).Count(&stored).Error)
	require.Equal(t, int64(1), stored, "actual capture resolves shared KB owner tenant without assuming session tenant")
	require.Error(t, db.Exec("UPDATE original_body_lease_scopes SET knowledge_base_id='body-new-namespace' WHERE body_id=?", bodyID).Error)
	require.Error(t, db.Exec("DELETE FROM original_body_lease_scopes WHERE body_id=?", bodyID).Error)
	lease, e := store.ContentLeaseStore().AcquireKBRead(ctx, 7, "kb", "actual-old-namespace-reader", time.Minute)
	require.NoError(t, e)
	require.NoError(t, db.Exec("UPDATE knowledges SET knowledge_base_id='body-new-namespace' WHERE id='body-knowledge-reference'").Error)
	deleted := "datetime('now','-2 hours')"
	if pg {
		deleted = "CURRENT_TIMESTAMP-INTERVAL '2 hours'"
	}
	require.NoError(t, db.Exec("UPDATE messages SET deleted_at="+deleted+" WHERE id=?", user.ID).Error)
	_, e = body.PurgeDue(context.Background(), 100)
	require.NoError(t, e)
	var state string
	require.NoError(t, db.Raw("SELECT state FROM original_body_payloads WHERE id=?", bodyID).Scan(&state).Error)
	require.Equal(t, "live", state, "original namespace lease remains protected after the pointer moves")
	require.NoError(t, store.ContentLeaseStore().ReleaseLease(context.Background(), lease.ID))
	n, e := body.PurgeDue(context.Background(), 100)
	require.NoError(t, e)
	require.Positive(t, n)
	require.NoError(t, db.Raw("SELECT state FROM original_body_payloads WHERE id=?", bodyID).Scan(&state).Error)
	require.Equal(t, "purged", state)
}
func TestMessageBodyNamespaceABASQLite(t *testing.T)   { runMessageBodyNamespaceABA(t, false) }
func TestPostgresMessageBodyNamespaceABA(t *testing.T) { runMessageBodyNamespaceABA(t, true) }

func runMessageBodyUnknownScopeSchema(t *testing.T, pg bool) {
	messages, _, _, db, ctx, session, body := messageBodyIntegrationFixture(t, pg)
	_, assistant := produceActualMessagePair(t, messages, ctx, session)
	// A missing extension table must not pass startup on a sequence-zero pin.
	require.NoError(t, db.Exec("DROP TABLE original_body_lease_scopes").Error)
	require.ErrorIs(t, body.VerifyJournalReady(), repository.ErrOriginalBodyUnavailable)
	require.NoError(t, body.UpgradeLeaseScopeSchema())
	require.NoError(t, body.VerifyJournalReady())
	var refs int64
	require.NoError(t, db.Table("original_body_lease_scopes").Count(&refs).Error)
	require.Zero(t, refs, "forward schema upgrade must not infer original scopes from current pointers")
	deleted := "datetime('now','-2 hours')"
	if pg {
		deleted = "CURRENT_TIMESTAMP-INTERVAL '2 hours'"
	}
	require.NoError(t, db.Exec("UPDATE messages SET deleted_at="+deleted+" WHERE id=?", assistant.ID).Error)
	n, e := body.PurgeDue(context.Background(), 100)
	require.NoError(t, e)
	require.Zero(t, n, "unknown legacy scopes remain protected")
	var bodyID string
	require.NoError(t, db.Raw("SELECT h.body_id FROM original_body_field_heads h JOIN message_generation_origins g ON g.id=h.row_key WHERE h.source_table='message_generation_origins' AND g.message_id=?", assistant.ID).Scan(&bodyID).Error)
	require.NotEmpty(t, bodyID)
	require.NoError(t, db.Exec("INSERT INTO original_body_lease_scopes(body_id,tenant_id,knowledge_base_id) VALUES(?,7,'kb')", bodyID).Error)
	n, e = body.PurgeDue(context.Background(), 100)
	require.NoError(t, e)
	require.Zero(t, n, "a partial KB row without the authenticated completeness marker cannot supply old scope inventory")

}
func TestMessageBodyUnknownScopeSchemaSQLite(t *testing.T) {
	runMessageBodyUnknownScopeSchema(t, false)
}
func TestPostgresMessageBodyUnknownScopeSchema(t *testing.T) {
	runMessageBodyUnknownScopeSchema(t, true)
}

func runMessageBodyNoKBActualReader(t *testing.T, pg bool) {
	messages, _, _, db, ctx, session, body := messageBodyIntegrationFixture(t, pg)
	user, e := messages.CreateMessage(ctx, &types.Message{SessionID: session.ID, RequestID: "actual-no-kb-body-request", Role: "user", Content: "actual standalone no KB body", IsCompleted: true})
	require.NoError(t, e)
	request := types.MessageMaterialReadRequest{TenantID: 7, SessionID: session.ID, MessageIDs: []string{user.ID}}
	guard, e := messages.beginMessageMaterialRead(ctx, request)
	require.NoError(t, e)
	require.Empty(t, guard.plan.Scopes)
	require.NotEmpty(t, guard.bodyLeases)
	require.NoError(t, guard.Recheck())
	var active int64
	require.NoError(t, db.Table("original_body_leases").Where("released_at_ms IS NULL").Count(&active).Error)
	require.Positive(t, active)
	deleted := "datetime('now','-2 hours')"
	if pg {
		deleted = "CURRENT_TIMESTAMP-INTERVAL '2 hours'"
	}
	require.NoError(t, db.Exec("UPDATE messages SET deleted_at="+deleted+" WHERE id=?", user.ID).Error)
	n, e := body.PurgeDue(context.Background(), 100)
	require.NoError(t, e)
	require.Zero(t, n, "the real no-KB reader still owns body ID leases")
	require.Error(t, guard.Recheck(), "metadata revocation blocks subsequent model/HTTP use")
	guard.Close()
	n, e = body.PurgeDue(context.Background(), 100)
	require.NoError(t, e)
	require.Positive(t, n)
	require.NoError(t, db.Table("original_body_leases").Where("released_at_ms IS NULL").Count(&active).Error)
	require.Zero(t, active)
	_, e = messages.beginMessageMaterialRead(ctx, request)
	require.Error(t, e)
}
func TestMessageBodyNoKBActualReaderSQLite(t *testing.T)   { runMessageBodyNoKBActualReader(t, false) }
func TestPostgresMessageBodyNoKBActualReader(t *testing.T) { runMessageBodyNoKBActualReader(t, true) }

func runMessageBodyNoKBHTTPHolder(t *testing.T, pg bool) {
	messages, _, _, db, ctx, session, body := messageBodyIntegrationFixture(t, pg)
	user, e := messages.CreateMessage(ctx, &types.Message{SessionID: session.ID, RequestID: "actual-no-kb-http-tail", Role: "user", Content: "actual no KB input for live model", IsCompleted: true})
	require.NoError(t, e)
	assistant, e := messages.CreateMessage(ctx, &types.Message{SessionID: session.ID, RequestID: user.RequestID, Role: "assistant"})
	require.NoError(t, e)
	ctx, _, check, closeHTTP := BeginMessageMaterialHTTPBoundary(ctx)
	defer closeHTTP()
	ctx, releaseModel, external := retainMessageMaterialProducer(ctx)
	defer releaseModel()
	require.True(t, external)
	ctx = context.WithValue(ctx, agentHistoryCurrentRequestKey{}, user.RequestID)
	_, e = messages.GetMessage(ctx, session.ID, user.ID)
	require.NoError(t, e)
	_, e = messages.admittedMessagePage(ctx, session.ID, time.Time{}, 20, 1, false)
	require.NoError(t, e)
	markMessageMaterialReady(ctx)
	require.NoError(t, check())
	assistant.Content = "actual live answer with no KB"
	assistant.IsCompleted = true
	assistant.SourceLineage, _ = types.NewCompleteSourceLineage()
	require.NoError(t, messages.UpdateMessage(ctx, assistant))
	require.NoError(t, check())
	var answerLeases int64
	require.NoError(t, db.Raw("SELECT count(*) FROM original_body_leases l JOIN original_body_field_heads h ON h.body_id=l.body_id WHERE h.source_table='messages' AND h.row_key=? AND h.column_name='content' AND l.released_at_ms IS NULL", assistant.ID).Scan(&answerLeases).Error)
	require.Positive(t, answerLeases, "own completion claims the new body before live HTTP output")
	releaseModel()
	require.NoError(t, check(), "HTTP holder survives model release")
	deleted := "datetime('now','-2 hours')"
	if pg {
		deleted = "CURRENT_TIMESTAMP-INTERVAL '2 hours'"
	}
	require.NoError(t, db.Exec("UPDATE messages SET deleted_at="+deleted+" WHERE session_id=?", session.ID).Error)
	n, e := body.PurgeDue(context.Background(), 100)
	require.NoError(t, e)
	require.Zero(t, n, "real HTTP tail still owns body leases after model release")
	require.Error(t, check(), "deleted original cannot continue HTTP output")
	closeHTTP()
	n, e = body.PurgeDue(context.Background(), 100)
	require.NoError(t, e)
	require.Positive(t, n)
	var active int64
	require.NoError(t, db.Table("original_body_leases").Where("released_at_ms IS NULL").Count(&active).Error)
	require.Zero(t, active)
}
func TestMessageBodyNoKBHTTPHolderSQLite(t *testing.T)   { runMessageBodyNoKBHTTPHolder(t, false) }
func TestPostgresMessageBodyNoKBHTTPHolder(t *testing.T) { runMessageBodyNoKBHTTPHolder(t, true) }

func runMessageBodyActualGeneratedField(t *testing.T, pg bool) {
	messages, _, _, db, ctx, session, body := messageBodyIntegrationFixture(t, pg)
	user, e := messages.CreateMessage(ctx, &types.Message{SessionID: session.ID, RequestID: "actual-body-generated-field", Role: "user", Content: "actual-generated-parent-body-sentinel", IsCompleted: true})
	require.NoError(t, e)
	ctx = WithMessageMaterialOutputBoundary(ctx)
	defer CloseMessageMaterialOutputBoundary(ctx)
	turn := types.NewRAGSourceLineage(func(checkCtx context.Context, lineage *types.SourceLineage) error {
		if e := lineage.RequireComplete(); e != nil {
			return e
		}
		return CheckMessageMaterialOutputBoundary(ctx)
	})
	ctx = types.WithRAGSourceLineage(ctx, turn)
	_, e = messages.GetMessage(ctx, session.ID, user.ID)
	require.NoError(t, e)
	require.NoError(t, messages.UpdateMessageRenderedContent(ctx, session.ID, user.ID, "actual-generated-rendered-body-sentinel"))
	require.NoError(t, CheckMessageMaterialOutputBoundary(ctx))
	var parents string
	require.NoError(t, db.Raw("SELECT parent_materials FROM message_generated_field_admissions WHERE message_id=?", user.ID).Scan(&parents).Error)
	require.NotContains(t, parents, user.Content, "metadata admission must not persist a second original text copy")
	require.NotContains(t, parents, "actual-generated-rendered-body-sentinel")
	var oldAdmission map[string]any
	require.NoError(t, db.Table("message_generated_field_admissions").Where("message_id=?", user.ID).Take(&oldAdmission).Error)
	slot := ctx.Value(messageMaterialOutputKey{}).(*messageMaterialOutputSlot)
	legacyCopy := *slot.Guards[0].snapshot
	legacyCopy.Messages = append([]*types.Message{}, legacyCopy.Messages...)
	unadmitted := *legacyCopy.Messages[0]
	unadmitted.Content = "unaccounted historical duplicate bytes"
	legacyCopy.Messages[0] = &unadmitted
	legacyBytes, e := json.Marshal([]*types.MessageMaterialSnapshot{&legacyCopy})
	require.NoError(t, e)
	oldAdmission["id"] = "unknown-legacy-parent-copy"
	oldAdmission["parent_materials"] = string(legacyBytes)
	require.NoError(t, db.Table("message_generated_field_admissions").Create(&oldAdmission).Error)
	var legacyBodyID string
	require.NoError(t, db.Raw("SELECT body_id FROM original_body_field_heads WHERE source_table='message_generated_field_admissions' AND row_key='unknown-legacy-parent-copy'").Scan(&legacyBodyID).Error)
	require.NotEmpty(t, legacyBodyID, "known old Messages copies enter conservative body inventory")

	var scoped, leased int64
	require.NoError(t, db.Raw("SELECT count(*) FROM original_body_lease_scopes s JOIN original_body_field_heads h ON h.body_id=s.body_id WHERE h.source_table='messages' AND h.row_key=? AND h.column_name='rendered_content'", user.ID).Scan(&scoped).Error)
	require.Positive(t, scoped, "actual generated field captures original scope refs")
	require.NoError(t, db.Raw("SELECT count(*) FROM original_body_leases l JOIN original_body_field_heads h ON h.body_id=l.body_id WHERE h.source_table='messages' AND h.row_key=? AND h.column_name='rendered_content' AND l.released_at_ms IS NULL", user.ID).Scan(&leased).Error)
	require.Positive(t, leased, "actual guard holds newly generated body before exposure")
	deleted := "datetime('now','-2 hours')"
	if pg {
		deleted = "CURRENT_TIMESTAMP-INTERVAL '2 hours'"
	}
	require.NoError(t, db.Exec("UPDATE messages SET deleted_at="+deleted+" WHERE id=?", user.ID).Error)
	n, e := body.PurgeDue(context.Background(), 100)
	require.NoError(t, e)
	require.Zero(t, n)
	CloseMessageMaterialOutputBoundary(ctx)
	n, e = body.PurgeDue(context.Background(), 100)
	require.NoError(t, e)
	require.Positive(t, n)
	var live int64
	require.NoError(t, db.Raw("SELECT count(*) FROM original_body_payloads p JOIN original_body_field_heads h ON h.body_id=p.id WHERE h.source_table='messages' AND h.row_key=? AND h.column_name='rendered_content' AND p.state='live'", user.ID).Scan(&live).Error)
	require.Zero(t, live, "closed generated body is not left unknown by a missing producer marker")
	var legacyState string
	require.NoError(t, db.Raw("SELECT state FROM original_body_payloads WHERE id=?", legacyBodyID).Scan(&legacyState).Error)
	require.Equal(t, "live", legacyState, "unadmitted old copy bytes cannot acquire scope/closure proof from a current message")
	require.NoError(t, db.Raw("SELECT parent_materials FROM message_generated_field_admissions WHERE id='unknown-legacy-parent-copy'").Scan(&parents).Error)
	require.Contains(t, parents, "unaccounted historical duplicate bytes", "no current-body re-sign or unproved old-copy erase")

}
func TestMessageBodyActualGeneratedFieldSQLite(t *testing.T) {
	runMessageBodyActualGeneratedField(t, false)
}
func TestPostgresMessageBodyActualGeneratedField(t *testing.T) {
	runMessageBodyActualGeneratedField(t, true)
}

func runMessageBodyEmptyGeneratedABA(t *testing.T, pg bool) {
	for _, field := range []string{"images", "attachments", "rendered_content"} {
		t.Run(field, func(t *testing.T) {
			messages, _, _, db, ctx, session, _ := messageBodyIntegrationFixture(t, pg)
			user, e := messages.CreateMessage(ctx, &types.Message{SessionID: session.ID, RequestID: "actual-empty-generated-" + field, Role: "user", Content: "actual raw author survives unconsumed generated update", IsCompleted: true})
			require.NoError(t, e)
			request := types.MessageMaterialReadRequest{TenantID: 7, SessionID: session.ID, MessageIDs: []string{user.ID}, IncludeOriginalUserFields: true}
			guard, e := messages.beginMessageMaterialRead(ctx, request)
			require.NoError(t, e)
			guard.Close()
			var before any
			var row map[string]any
			require.NoError(t, db.Table("messages").Select(field).Where("id=?", user.ID).Take(&row).Error)
			before = row[field]
			changed := any("foreign generated rendered")
			if field == "images" {
				changed = `[{"url":"https://foreign.invalid/image"}]`
			} else if field == "attachments" {
				changed = `[{"name":"foreign attachment"}]`
			}
			require.NoError(t, db.Table("messages").Where("id=?", user.ID).Update(field, changed).Error)
			require.NoError(t, db.Table("messages").Where("id=?", user.ID).Update(field, before).Error)
			_, e = messages.beginMessageMaterialRead(ctx, request)
			require.Error(t, e, "selected original generated controls must reject empty ABA")
			raw := request
			raw.IncludeOriginalUserFields = false
			rawGuard, e := messages.beginMessageMaterialRead(ctx, raw)
			require.NoError(t, e, "raw-user plan did not consume those generated fields")
			rawGuard.Close()
		})
	}
}
func TestMessageBodyEmptyGeneratedABASQLite(t *testing.T)   { runMessageBodyEmptyGeneratedABA(t, false) }
func TestPostgresMessageBodyEmptyGeneratedABA(t *testing.T) { runMessageBodyEmptyGeneratedABA(t, true) }
