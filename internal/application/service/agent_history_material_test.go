package service

import (
	"context"
	"fmt"
	"github.com/Tencent/WeKnora/internal/agent"
	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/application/repository"
	chatpipeline "github.com/Tencent/WeKnora/internal/application/service/chat_pipeline"
	"github.com/Tencent/WeKnora/internal/config"
	"github.com/Tencent/WeKnora/internal/event"
	"github.com/Tencent/WeKnora/internal/models/chat"
	"os"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

func TestAgentHistoryActualOriginalReadLeaseLifetimeAndCheckpoint(t *testing.T) {
	messages, _, _, db, ctx, session := messageMaterialActualFixture(t)
	_, assistant := produceActualMessagePair(t, messages, ctx, session)
	currentUser, err := messages.CreateMessage(ctx, &types.Message{SessionID: session.ID, RequestID: "current-unconsumed-turn", Role: "user", Content: "new current question", IsCompleted: true})
	require.NoError(t, err)
	_, err = messages.CreateMessage(ctx, &types.Message{SessionID: session.ID, RequestID: currentUser.RequestID, Role: "assistant"})
	require.NoError(t, err)
	ctx = WithMessageMaterialOutputBoundary(ctx)
	ctx = context.WithValue(ctx, agentHistoryCurrentRequestKey{}, currentUser.RequestID)
	defer CloseMessageMaterialOutputBoundary(ctx)
	debugRepo, debugErr := (messageLineageHistoryAdmission{verifier: messages}).OriginalHistoryRepository(ctx, session.ID)
	require.NoError(t, debugErr, "original capture")
	require.NoError(t, CheckMessageMaterialOutputBoundary(ctx), "after original capture")
	debugRows, debugErr := debugRepo.ListMessagesBySessionBeforeCursor(ctx, session.ID, time.Time{}, "", 200)
	require.NoError(t, debugErr, "first original cursor")
	require.Len(t, debugRows, 2)
	history, _, err := LoadAgentHistory(ctx, messages.messageRepo, session.ID, 8000, false, messageLineageHistoryAdmission{verifier: messages})
	require.NoError(t, err)
	require.Len(t, history, 2)
	require.Equal(t, assistant.Content, history[1].Content)
	require.NoError(t, CheckMessageMaterialOutputBoundary(ctx))
	var live int64
	require.NoError(t, db.Table("nextcloud_content_leases").Where("kind='read' AND released_at_ms IS NULL").Count(&live).Error)
	require.Positive(t, live)
	cp := &types.ContextCheckpoint{Summary: "actual deterministic compaction of original captured answer", SourceLineage: assistant.SourceLineage}
	sink := messageCheckpointSink{repo: messages.messageRepo, sessionID: session.ID}
	require.NoError(t, sink.SaveContextCheckpoint(ctx, assistant.ID, cp))
	require.NoError(t, CheckMessageMaterialOutputBoundary(ctx))
	fresh := WithMessageMaterialOutputBoundary(ctx)
	defer CloseMessageMaterialOutputBoundary(fresh)
	replay, _, err := LoadAgentHistory(fresh, messages.messageRepo, session.ID, 8000, false, messageLineageHistoryAdmission{verifier: messages})
	require.NoError(t, err)
	require.NotEmpty(t, replay)
	require.Contains(t, replay[0].Content, cp.Summary)
	require.NoError(t, db.Model(&types.Message{}).Where("id=?", assistant.ID).Update("content", "foreign body").Error)
	require.NoError(t, db.Model(&types.Message{}).Where("id=?", assistant.ID).Update("content", assistant.Content).Error)
	require.Error(t, CheckMessageMaterialOutputBoundary(ctx))
}

type materialLeaseChat struct {
	callback func(context.Context, []chat.Message) error
}

func (m materialLeaseChat) Chat(context.Context, []chat.Message, *chat.ChatOptions) (*types.ChatResponse, error) {
	return nil, fmt.Errorf("unexpected non-stream model call")
}
func (m materialLeaseChat) GetModelName() string { return "deterministic-owned-model" }
func (m materialLeaseChat) GetModelID() string   { return "deterministic-owned-model" }
func (m materialLeaseChat) ChatStream(ctx context.Context, input []chat.Message, _ *chat.ChatOptions) (<-chan types.StreamResponse, error) {
	if err := m.callback(ctx, input); err != nil {
		return nil, err
	}
	out := make(chan types.StreamResponse, 1)
	out <- types.StreamResponse{ResponseType: types.ResponseTypeAnswer, Content: "new actual model answer", Done: true}
	close(out)
	return out, nil
}
func TestAgentHistoryActualModelConsumesWithHeldLeasesAndBodyRevocation(t *testing.T) {
	for _, revoke := range []bool{false, true} {
		t.Run(map[bool]string{false: "positive", true: "body-revoke"}[revoke], func(t *testing.T) {
			messages, _, _, db, ctx, session := messageMaterialActualFixture(t)
			_, assistant := produceActualMessagePair(t, messages, ctx, session)
			ctx = WithMessageMaterialOutputBoundary(ctx)
			defer CloseMessageMaterialOutputBoundary(ctx)
			history, _, err := LoadAgentHistory(ctx, messages.messageRepo, session.ID, 8000, false, messageLineageHistoryAdmission{verifier: messages})
			require.NoError(t, err)
			called := false
			model := materialLeaseChat{callback: func(_ context.Context, input []chat.Message) error {
				called = true
				var live int64
				require.NoError(t, db.Table("nextcloud_content_leases").Where("kind='read' AND released_at_ms IS NULL").Count(&live).Error)
				require.Positive(t, live)
				found := false
				for _, message := range input {
					if message.Content == assistant.Content {
						found = true
					}
				}
				require.True(t, found)
				if revoke {
					require.NoError(t, db.Model(&types.Message{}).Where("id=?", assistant.ID).Update("content", "foreign body").Error)
				}
				return nil
			}}
			engine := agent.NewAgentEngine(&types.AgentConfig{MaxIterations: 2, Temperature: 0.1}, model, nil, event.NewEventBus(), nil, nil, session.ID, "")
			require.NotNil(t, engine)
			engine.SetSourceLineageVerifier(func(_ context.Context, lineage *types.SourceLineage) error {
				if err := lineage.RequireComplete(); err != nil {
					return err
				}
				return CheckMessageMaterialOutputBoundary(ctx)
			})
			engine.SetRoundGuard(func(context.Context) error { return CheckMessageMaterialOutputBoundary(ctx) })
			empty, _ := types.NewCompleteSourceLineage()
			engine.SetUserInputSourceLineage(empty)
			engine.SetSystemPromptSourceLineage(empty)
			engine.SetSkillsMetadataSourceLineage(empty)
			_, err = engine.Execute(ctx, session.ID, "actual-new-output", "current human question", history)
			require.True(t, called)
			if revoke {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestAgentHistoryPostgresActualModelCheckpointAndParentRevokeSingleConnection(t *testing.T) {
	kg, store, db, ctx := faqPostgresActualFixture(t)
	require.NoError(t, db.Exec("CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\"").Error)
	require.NoError(t, db.Exec("ALTER TABLE messages ADD COLUMN artifacts JSONB").Error)
	for _, name := range []string{"000103_message_artifacts_table.up.sql", "000107_message_artifacts_deleted_at.up.sql", "000127_nextcloud_content_leases.up.sql", "000151_message_original_materials.up.sql"} {
		ddl, err := os.ReadFile("../../../migrations/versioned/" + name)
		require.NoError(t, err)
		require.NoError(t, db.Exec(string(ddl)).Error)
	}
	sqlDB, err := db.DB()
	require.NoError(t, err)
	sqlDB.SetMaxOpenConns(1)
	sqlDB.SetMaxIdleConns(1)
	ctx = context.WithValue(ctx, types.UserIDContextKey, types.CallerFromContext(ctx).UserID)
	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	kbs := &knowledgeBaseService{repo: repository.NewKnowledgeBaseRepository(db)}
	groups := NewGroupAccessService(repository.NewGroupAccessRepository(db))
	sourceGuard := access.NewNextcloudSourceLineageGuard(kg.repo.(access.NextcloudLineageSnapshotLookup), kbs, nil, nil, groups, nil, store)
	messages := &messageService{messageRepo: repository.NewMessageRepository(db), sessionRepo: repository.NewSessionRepository(db), sourceLineageGuard: sourceGuard, kbService: kbs, knowService: kg}
	ConfigureMessageMaterialProvenance(messages, store)
	session := &types.Session{TenantID: 7, UserID: types.CallerFromContext(ctx).UserID}
	require.NoError(t, db.Create(session).Error)
	user, assistant := produceActualMessagePair(t, messages, ctx, session)
	ctx = WithMessageMaterialOutputBoundary(ctx)
	defer CloseMessageMaterialOutputBoundary(ctx)
	history, _, err := LoadAgentHistory(ctx, messages.messageRepo, session.ID, 8000, false, messageLineageHistoryAdmission{verifier: messages})
	require.NoError(t, err)
	require.Len(t, history, 2)
	called := false
	model := materialLeaseChat{callback: func(context.Context, []chat.Message) error {
		called = true
		var count int64
		require.NoError(t, db.Table("nextcloud_content_leases").Where("kind='read' AND released_at_ms IS NULL").Count(&count).Error)
		require.Positive(t, count)
		return nil
	}}
	engine := agent.NewAgentEngine(&types.AgentConfig{MaxIterations: 2}, model, nil, event.NewEventBus(), nil, nil, session.ID, "")
	empty, _ := types.NewCompleteSourceLineage()
	engine.SetSystemPromptSourceLineage(empty)
	engine.SetUserInputSourceLineage(empty)
	engine.SetSkillsMetadataSourceLineage(empty)
	engine.SetSourceLineageVerifier(func(_ context.Context, lineage *types.SourceLineage) error {
		if err := lineage.RequireComplete(); err != nil {
			return err
		}
		return CheckMessageMaterialOutputBoundary(ctx)
	})
	engine.SetRoundGuard(func(context.Context) error { return CheckMessageMaterialOutputBoundary(ctx) })
	_, err = engine.Execute(ctx, session.ID, "new-output", "current question", history)
	require.NoError(t, err)
	require.True(t, called)
	cp := &types.ContextCheckpoint{Summary: "actual PostgreSQL compaction", SourceLineage: assistant.SourceLineage}
	require.NoError(t, (messageCheckpointSink{repo: messages.messageRepo, sessionID: session.ID}).SaveContextCheckpoint(ctx, assistant.ID, cp))
	require.NoError(t, CheckMessageMaterialOutputBoundary(ctx))
	require.NoError(t, db.Model(&types.Message{}).Where("id=?", user.ID).Update("content", "foreign parent").Error)
	require.NoError(t, db.Model(&types.Message{}).Where("id=?", user.ID).Update("content", user.Content).Error)
	require.Error(t, CheckMessageMaterialOutputBoundary(ctx))
	fresh := WithMessageMaterialOutputBoundary(ctx)
	defer CloseMessageMaterialOutputBoundary(fresh)
	_, _, err = LoadAgentHistory(fresh, messages.messageRepo, session.ID, 8000, false, messageLineageHistoryAdmission{verifier: messages})
	require.Error(t, err)
}

func TestAgentHistoryActualHumanAttachmentOriginalAndUnknownGeneratedFieldReject(t *testing.T) {
	messages, _, _, db, ctx, session := messageMaterialActualFixture(t)
	user, err := messages.CreateMessage(ctx, &types.Message{SessionID: session.ID, RequestID: "new-uploaded-human-input", Role: "user", Content: "actual attachment question", IsCompleted: true, Attachments: types.MessageAttachments{{FileName: "actual.txt", Content: "actual newly submitted attachment content"}}})
	require.NoError(t, err)
	assistant, err := messages.CreateMessage(ctx, &types.Message{SessionID: session.ID, RequestID: user.RequestID, Role: "assistant"})
	require.NoError(t, err)
	assistant.Content = "actual answer"
	assistant.IsCompleted = true
	assistant.SourceLineage, _ = types.NewCompleteSourceLineage()
	require.NoError(t, messages.UpdateMessage(ctx, assistant))
	ctx = WithMessageMaterialOutputBoundary(ctx)
	defer CloseMessageMaterialOutputBoundary(ctx)
	history, _, err := LoadAgentHistory(ctx, messages.messageRepo, session.ID, 8000, false, messageLineageHistoryAdmission{verifier: messages})
	require.NoError(t, err)
	require.Len(t, history, 2)
	require.Contains(t, history[0].Content, user.Attachments[0].Content)
	require.NoError(t, messages.UpdateMessageRenderedContent(ctx, session.ID, user.ID, "uncertified generated rendering"))
	fresh := WithMessageMaterialOutputBoundary(ctx)
	defer CloseMessageMaterialOutputBoundary(fresh)
	_, _, err = LoadAgentHistory(fresh, messages.messageRepo, session.ID, 8000, false, messageLineageHistoryAdmission{verifier: messages})
	require.Error(t, err)
	var exists int64
	require.NoError(t, db.Table("message_generated_field_admissions").Where("message_id=? AND field='rendered_content'", user.ID).Count(&exists).Error)
	require.Zero(t, exists)
}

func TestMessageActualRAGPartialRendererImageThenFinalProducer(t *testing.T) {
	messages, _, _, _, ctx, session := messageMaterialActualFixture(t)
	user, err := messages.CreateMessage(ctx, &types.Message{SessionID: session.ID, RequestID: "actual-rag-partials", Role: "user", Content: "actual human query", IsCompleted: true, Images: types.MessageImages{{URL: "https://owned.invalid/new-human-image"}}, ExecutionContext: types.MessageExecutionContext{KnowledgeBaseIDs: []string{"kb"}}})
	require.NoError(t, err)
	assistant, err := messages.CreateMessage(ctx, &types.Message{SessionID: session.ID, RequestID: user.RequestID, Role: "assistant", ExecutionContext: user.ExecutionContext})
	require.NoError(t, err)
	ctx = WithMessageMaterialOutputBoundary(ctx)
	ctx = context.WithValue(ctx, agentHistoryCurrentRequestKey{}, user.RequestID)
	defer CloseMessageMaterialOutputBoundary(ctx)
	source, _ := types.NewCompleteSourceLineage()
	turn := types.NewRAGSourceLineage(func(checkCtx context.Context, lineage *types.SourceLineage) error {
		if err := lineage.RequireComplete(); err != nil {
			return err
		}
		return CheckMessageMaterialOutputBoundary(ctx)
	})
	ctx = types.WithRAGSourceLineage(ctx, turn)
	original, err := messages.GetMessage(ctx, session.ID, user.ID)
	require.NoError(t, err)
	require.Len(t, original.Images, 1)
	// Actual pipeline setters use the checked turn and original read snapshots.
	require.NoError(t, messages.UpdateMessageRenderedContent(ctx, session.ID, user.ID, "actual deterministic rendered query"))
	images := append(types.MessageImages{}, original.Images...)
	images[0].Caption = "actual deterministic generated image caption"
	require.NoError(t, messages.UpdateMessageImages(ctx, session.ID, user.ID, images))
	require.NoError(t, CheckMessageMaterialOutputBoundary(ctx))
	assistant.Content = "actual final answer after legitimate partial producers"
	assistant.SourceLineage = source
	assistant.IsCompleted = true
	require.NoError(t, messages.UpdateMessage(ctx, assistant))
	// A new original reader gets the exact independently admitted user fields.
	fresh := WithMessageMaterialOutputBoundary(ctx)
	defer CloseMessageMaterialOutputBoundary(fresh)
	rows, err := messages.admittedMessages(fresh, types.MessageMaterialReadRequest{TenantID: 7, SessionID: session.ID, MessageIDs: []string{user.ID}, IncludeOriginalUserFields: true})
	require.NoError(t, err)
	require.Equal(t, images[0].Caption, rows[0].Images[0].Caption)
	require.Equal(t, "actual deterministic rendered query", rows[0].RenderedContent)
}

func TestMessageActualFirstQAPersistedPlaceholderHistoryAndFinalStream(t *testing.T) {
	messages, _, _, db, ctx, session := messageMaterialActualFixture(t)
	user, err := messages.CreateMessage(ctx, &types.Message{SessionID: session.ID, RequestID: "first-actual-qa-turn", Role: "user", Content: "first actual human question", IsCompleted: true})
	require.NoError(t, err)
	assistant, err := messages.CreateMessage(ctx, &types.Message{SessionID: session.ID, RequestID: user.RequestID, Role: "assistant"})
	require.NoError(t, err)
	cfg := &config.Config{Conversation: &config.ConversationConfig{MaxRounds: 3, Summary: &config.SummaryConfig{Prompt: "actual configured code prompt", ContextTemplate: "{{.Query}}"}}}
	called := false
	model := materialLeaseChat{callback: func(modelCtx context.Context, input []chat.Message) error {
		called = true
		require.NoError(t, types.CheckHistoryMaterialLifetime(modelCtx))
		for _, item := range input {
			require.NotContains(t, item.Content, "unknown placeholder")
		}
		var count int64
		require.NoError(t, db.Table("message_material_receipts").Count(&count).Error)
		require.GreaterOrEqual(t, count, int64(2))
		return nil
	}}
	models := &stubModelService{chatModel: model, modelsByID: map[string]*types.Model{"actual-first-qa-model": {ID: "actual-first-qa-model", TenantID: 7, Type: types.ModelTypeKnowledgeQA}}}
	manager := chatpipeline.NewEventManager()
	chatpipeline.ConfigureSourceLineage(manager, messages.sourceLineageGuard.(*access.NextcloudSourceLineageGuard))
	chatpipeline.NewPluginLoadHistory(manager, messages, cfg)
	chatpipeline.NewPluginMemoryRecall(manager, nil)
	chatpipeline.NewPluginChatCompletionStream(manager, models)
	service := &sessionService{cfg: cfg, modelService: models, eventManager: manager, lineageVerifier: messages, sourceLineageGuard: messages.sourceLineageGuard}
	bus := event.NewEventBus()
	require.NoError(t, repository.UpgradeMessageProducerAdvanceSchema(db))
	ctx, _, finalCheck, closeHTTP := BeginMessageMaterialHTTPBoundary(ctx)
	defer closeHTTP()
	done := make(chan error, 1)
	bus.On(event.EventAgentFinalAnswer, func(eventCtx context.Context, evt event.Event) error {
		data, ok := evt.Data.(event.AgentFinalAnswerData)
		if !ok {
			return nil
		}
		assistant.Content += data.Content
		if data.Done {
			assistant.SourceLineage, _ = evt.SourceLineage.(*types.SourceLineage)
			assistant.IsCompleted = true
			// The production handler persists with its shared outer async context.
			err := messages.UpdateMessage(ctx, assistant)
			done <- err
		}
		return nil
	})
	req := &types.QARequest{Session: session, Query: user.Content, UserMessageID: user.ID, AssistantMessageID: assistant.ID, SummaryModelID: "actual-first-qa-model", TurnLeaseHeld: true}
	require.NoError(t, service.KnowledgeQA(ctx, req, bus))
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("actual first QA stream did not finish")
	}
	require.True(t, called)
	require.NoError(t, finalCheck())
	var actual types.Message
	require.NoError(t, db.Where("id=?", assistant.ID).Take(&actual).Error)
	require.True(t, actual.IsCompleted)
	require.Contains(t, actual.Content, "new actual model answer")
}

func TestCommittedHistoryParentRequestReadsOnlyMetadataBeforeLease(t *testing.T) {
	messages, _, store, db, ctx, session := messageMaterialActualFixture(t)
	_, assistant := produceActualMessagePair(t, messages, ctx, session)
	observed := false
	name := "only-original-parent-control-metadata"
	require.NoError(t, db.Callback().Query().Before("gorm:query").Register(name, func(tx *gorm.DB) {
		if tx.Statement.Table != "message_generation_origins" {
			return
		}
		observed = true
		require.Equal(t, []string{"request_id"}, tx.Statement.Selects)
		for _, field := range tx.Statement.Selects {
			require.NotEqual(t, "initial_input", field)
		}
	}))
	defer db.Callback().Query().Remove(name)
	req, err := store.CommittedMessageParentRequest(ctx, 7, session.ID, assistant.ID)
	require.NoError(t, err)
	require.Len(t, req.MessageIDs, 2)
	require.True(t, observed)
}

func TestMessageMaterialModelBoundaryCancelExpiryAndCurrentRequestControlABA(t *testing.T) {
	for _, change := range []string{"cancel", "expiry", "current-request-control-ABA"} {
		t.Run(change, func(t *testing.T) {
			messages, _, _, db, ctx, session := messageMaterialActualFixture(t)
			_, assistant := produceActualMessagePair(t, messages, ctx, session)
			current, err := messages.CreateMessage(ctx, &types.Message{SessionID: session.ID, RequestID: "current-model-request", Role: "assistant"})
			require.NoError(t, err)
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()
			ctx = WithMessageMaterialOutputBoundary(ctx)
			ctx = context.WithValue(ctx, agentHistoryCurrentRequestKey{}, current.RequestID)
			defer CloseMessageMaterialOutputBoundary(ctx)
			_, _, err = LoadAgentHistory(ctx, messages.messageRepo, session.ID, 8000, false, messageLineageHistoryAdmission{verifier: messages})
			require.NoError(t, err)
			switch change {
			case "cancel":
				cancel()
			case "expiry":
				require.NoError(t, db.Table("nextcloud_content_leases").Where("kind='read' AND released_at_ms IS NULL").Update("expires_at_ms", 1).Error)
			case "current-request-control-ABA":
				require.NoError(t, db.Model(&types.Message{}).Where("id=?", current.ID).Update("request_id", "foreign-selection-control").Error)
				require.NoError(t, db.Model(&types.Message{}).Where("id=?", current.ID).Update("request_id", current.RequestID).Error)
			}
			require.Error(t, CheckMessageMaterialOutputBoundary(ctx), assistant.ID)
		})
	}
}

func TestMessageGeneratedFieldForwardUpgradeFromInstalledCore151(t *testing.T) {
	_, _, store, db, _, _ := messageMaterialActualFixture(t)
	// This fixture currently installed fresh70; remove only new owned schema to
	// emulate the already-installed old core migration, retaining originals.
	for _, name := range []string{"message_generated_field_insert", "message_generated_field_images", "message_generated_field_attachments", "message_generated_field_rendered_content", "message_generated_field_context_checkpoint", "message_fork_parent_origins_immutable_update", "message_fork_parent_origins_immutable_delete", "message_generated_field_admissions_immutable_update", "message_generated_field_admissions_immutable_delete"} {
		require.NoError(t, db.Exec("DROP TRIGGER IF EXISTS "+name).Error)
	}
	for _, table := range []string{"message_fork_parent_origins", "message_generated_field_receipts", "message_generated_field_admissions", "message_generated_field_versions"} {
		require.NoError(t, db.Exec("DROP TABLE "+table).Error)
	}
	require.NoError(t, repository.UpgradeMessageGeneratedFieldSchema(db))
	require.NoError(t, repository.UpgradeMessageGeneratedFieldSchema(db))
	var receipts int64
	require.NoError(t, db.Table("message_generated_field_admissions").Count(&receipts).Error)
	require.Zero(t, receipts, "upgrade counters never certify legacy fields")
	_ = store
}
