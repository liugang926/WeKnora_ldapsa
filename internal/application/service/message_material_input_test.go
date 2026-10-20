package service

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/hibiken/asynq"

	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

func messageMaterialActualFixture(
	t *testing.T,
) (
	*messageService,
	*knowledgeService,
	*repository.KnowledgeInputProvenanceStore,
	*gorm.DB,
	context.Context,
	*types.Session,
) {
	t.Helper()
	kg, store, db, ctx, _, _, _ := historyActorIntegrationFixture(t, "web")
	closeHistoryMaterialGuard(ctx)
	ctx = types.WithExecutionTenant(
		types.WithPrincipal(
			types.WithCaller(context.Background(), types.CallerFromContext(ctx)),
			types.Principal{Type: types.PrincipalWebUser, ID: types.CallerFromContext(ctx).UserID},
		),
		7,
	)
	if !db.Migrator().HasColumn(&types.Message{}, "artifacts") {
		require.NoError(t, db.Exec("ALTER TABLE messages ADD COLUMN artifacts TEXT").Error)
	}
	for _, name := range []string{
		"000023_message_artifacts_table.up.sql",
		"000026_message_artifacts_deleted_at.up.sql",
		"000046_nextcloud_content_leases.up.sql",
		"000070_message_original_materials.up.sql",
	} {
		table := map[string]string{
			"000023_message_artifacts_table.up.sql":      "message_artifacts",
			"000026_message_artifacts_deleted_at.up.sql": "message_artifacts",
			"000046_nextcloud_content_leases.up.sql":     "nextcloud_content_leases",
			"000070_message_original_materials.up.sql":   "message_generation_origins",
		}[name]
		if db.Migrator().HasTable(table) {
			continue
		}
		ddl, err := os.ReadFile("../../../migrations/sqlite/" + name)
		require.NoError(t, err)
		require.NoError(t, db.Exec(string(ddl)).Error)
	}
	groups := NewGroupAccessService(repository.NewGroupAccessRepository(db))
	factory := func(tx *gorm.DB) *repository.KnowledgeInputProvenanceStore {
		authority := repository.NewKnowledgeInputProvenanceStore(tx)
		repository.ConfigureKnowledgeInputActorGroupAccess(
			authority,
			NewGroupAccessService(repository.NewGroupAccessRepository(tx)),
		)
		return authority
	}
	repository.ConfigureKnowledgeDisplayInputWriters(store, repository.NewKnowledgeTagRepository(db), kg.repo, factory)
	kbs := &knowledgeBaseService{repo: repository.NewKnowledgeBaseRepository(db)}
	guard := access.NewNextcloudSourceLineageGuard(
		kg.repo.(access.NextcloudLineageSnapshotLookup),
		kbs,
		nil,
		nil,
		groups,
		nil,
		store,
	)
	messages := &messageService{
		messageRepo:        repository.NewMessageRepository(db),
		sessionRepo:        repository.NewSessionRepository(db),
		sourceLineageGuard: guard,
		kbService:          kbs,
		knowService:        kg,
	}
	ConfigureMessageMaterialProvenance(messages, store)
	ctx = context.WithValue(ctx, types.UserIDContextKey, types.CallerFromContext(ctx).UserID)
	session := &types.Session{
		ID:       "actual-new-message-session",
		TenantID: 7,
		UserID:   types.CallerFromContext(ctx).UserID,
	}
	require.NoError(t, db.Create(session).Error)
	sqlDB, err := db.DB()
	require.NoError(t, err)
	sqlDB.SetMaxOpenConns(1)
	sqlDB.SetMaxIdleConns(1)
	var kb types.KnowledgeBase
	require.NoError(t, db.Where("id='kb'").Take(&kb).Error)
	grant, err := access.ResolveKB(
		ctx,
		access.KBRequest{Caller: types.CallerFromContext(ctx)},
		&kb,
		types.OrgRoleEditor,
		nil,
		nil,
	)
	require.NoError(t, err)
	ctx = grant.Context(ctx)
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	t.Cleanup(cancel)
	return messages, kg, store, db, ctx, session
}

func produceActualMessagePair(
	ctx context.Context, t *testing.T,
	messages *messageService,

	session *types.Session,
) (*types.Message, *types.Message) {
	t.Helper()
	user, err := messages.CreateMessage(
		ctx,
		&types.Message{
			SessionID:        session.ID,
			RequestID:        "actual-material-request",
			Role:             "user",
			Content:          "new actual original question",
			IsCompleted:      true,
			ExecutionContext: types.MessageExecutionContext{KnowledgeBaseIDs: []string{"kb"}},
		},
	)
	require.NoError(t, err)
	assistant, err := messages.CreateMessage(
		ctx,
		&types.Message{
			SessionID:        session.ID,
			RequestID:        user.RequestID,
			Role:             "assistant",
			IsCompleted:      false,
			ExecutionContext: user.ExecutionContext,
		},
	)
	require.NoError(t, err)
	assistant.Content = "new actual original generated answer"
	assistant.IsCompleted = true
	assistant.SourceLineage, _ = types.NewCompleteSourceLineage()
	require.NoError(t, messages.UpdateMessage(ctx, assistant))
	return user, assistant
}

func TestMessageMaterialActualProducerReadAndDerivedKnowledgeSingleConnection(t *testing.T) {
	messages, kg, store, db, ctx, session := messageMaterialActualFixture(t)
	_, assistant := produceActualMessagePair(ctx, t, messages, session)
	request, err := store.CommittedMessageParentRequest(ctx, 7, session.ID, assistant.ID)
	require.NoError(t, err)
	guard, err := messages.beginMessageMaterialRead(ctx, request)
	require.NoError(t, err)
	require.NoError(t, guard.Recheck())
	guard.Close()
	tenant, _ := types.TenantInfoFromContext(ctx)
	if tenant == nil {
		tenant = kg.tenantRepo.(knowledgeInputTenantRepo).tenant
	}
	tenant.ChatHistoryConfig = &types.ChatHistoryConfig{Enabled: true, KnowledgeBaseID: "kb", EmbeddingModelID: "embed"}
	ctx = context.WithValue(ctx, types.TenantInfoContextKey, tenant)
	messages.IndexMessageToKB(ctx, "foreign cached question", "foreign cached answer", assistant.ID, session.ID)
	queue := kg.task.(*knowledgeInputQueue)
	require.Len(t, queue.tasks, 1)
	var payload types.DocumentProcessPayload
	require.NoError(t, json.Unmarshal(queue.tasks[0].Payload(), &payload))
	require.Contains(t, payload.Passages[0], assistant.Content)
	require.NotContains(t, payload.Passages[0], "foreign cached")
	result := &types.CommittedHistoryKnowledgeInput{
		Knowledge: &types.Knowledge{ID: payload.KnowledgeID},
		Passages:  payload.Passages,
		Proof:     payload.OriginalInput,
	}
	require.NoError(
		t,
		kg.ProcessDocument(context.Background(), asynq.NewTask(types.TypeDocumentProcess, queue.tasks[0].Payload())),
	)
	var persisted types.Knowledge
	require.NoError(t, db.Where("id=?", result.Knowledge.ID).Take(&persisted).Error)
	t.Logf("postworker status=%s error=%s", persisted.ParseStatus, persisted.ErrorMessage)
	debugCtx, debugErr := kg.resumeKnowledgeInput(context.Background(), result.Proof, result.Passages)
	require.NoError(t, debugErr)
	require.NoError(t, requireActualKnowledgeBuildInput(debugCtx), "after actual chunks")
	closeHistoryMaterialGuard(debugCtx)
	var count int64
	require.NoError(t, db.Model(&types.Chunk{}).Where("knowledge_id=?", result.Knowledge.ID).Count(&count).Error)
	require.Positive(t, count)
	originalRead, err := messages.beginMessageMaterialRead(ctx, request)
	require.NoError(t, err)
	var ids []string
	require.NoError(t, db.Model(&types.Chunk{}).Where("knowledge_id=?", result.Knowledge.ID).Pluck("id", &ids).Error)
	snapshot, err := store.CaptureKnowledgeBuildInputs(
		originalRead.Context(),
		types.KnowledgeBuildInputCaptureRequest{
			TenantID:        7,
			KnowledgeBaseID: "kb",
			KnowledgeID:     result.Knowledge.ID,
			ChunkIDs:        ids,
			Expected:        result.Proof,
		},
	)
	require.NoError(t, err)
	require.NoError(t, snapshot.Proof.SourceLineage.RequireComplete())
	require.NotEmpty(t, snapshot.Chunks)
	require.Equal(t, "enabled", snapshot.Knowledge.EnableStatus)
	require.NoError(
		t,
		messages.sourceLineageGuard.Check(
			originalRead.Context(),
			snapshot.Proof.SourceLineage,
			access.LineageReadScope{
				Targets: []*types.SearchTarget{
					{Type: types.SearchTargetTypeKnowledgeBase, KnowledgeBaseID: "kb", TenantID: 7},
				},
			},
		),
	)
	originalRead.Close()
	require.NoError(
		t,
		db.Table("nextcloud_content_leases").Where("kind='read' AND released_at_ms IS NULL").Count(&count).Error,
	)
	require.Zero(t, count)
}

func TestMessageMaterialActualProducerLegacyAndBodyOwnerABAReject(t *testing.T) {
	for _, change := range []string{
		"legacy",
		"content-ABA",
		"owner-ABA",
		"caller",
		"user-revoked",
		"member-revoked",
		"execution-ABA",
		"membership-ABA",
	} {
		t.Run(change, func(t *testing.T) {
			messages, _, store, db, ctx, session := messageMaterialActualFixture(t)
			_, assistant := produceActualMessagePair(ctx, t, messages, session)
			request, err := store.CommittedMessageParentRequest(ctx, 7, session.ID, assistant.ID)
			require.NoError(t, err)
			guard, err := messages.beginMessageMaterialRead(ctx, request)
			require.NoError(t, err)
			defer guard.Close()
			switch change {
			case "legacy":
				legacy := *assistant
				legacy.ID = "old-unadmitted-message"
				legacy.RequestID = "old-request"
				require.NoError(t, db.Create(&legacy).Error)
				_, err = messages.beginMessageMaterialRead(
					ctx,
					types.MessageMaterialReadRequest{
						TenantID:   7,
						SessionID:  session.ID,
						MessageIDs: []string{legacy.ID},
					},
				)
				require.Error(t, err)
				return
			case "execution-ABA":
				require.NoError(
					t,
					db.Model(&types.Message{}).
						Where("id=?", assistant.ID).
						Update("execution_context", types.MessageExecutionContext{KnowledgeBaseIDs: []string{
							"foreign-kb",
						}}).
						Error,
				)
				require.NoError(
					t,
					db.Model(&types.Message{}).
						Where("id=?", assistant.ID).
						Update("execution_context", assistant.ExecutionContext).
						Error,
				)
			case "membership-ABA":
				other, err := messages.CreateMessage(
					ctx,
					&types.Message{
						SessionID:   session.ID,
						RequestID:   "new-foreign-candidate",
						Role:        "user",
						Content:     "new human candidate",
						IsCompleted: true,
					},
				)
				require.NoError(t, err)
				require.NoError(t, db.Delete(other).Error)
			case "content-ABA":
				require.NoError(
					t,
					db.Model(&types.Message{}).Where("id=?", assistant.ID).Update("content", "foreign transient").Error,
				)
				require.NoError(
					t,
					db.Model(&types.Message{}).Where("id=?", assistant.ID).Update("content", assistant.Content).Error,
				)
			case "owner-ABA":
				owner := session.UserID
				require.NoError(t, db.Model(session).Update("user_id", "other-user").Error)
				require.NoError(t, db.Model(session).Update("user_id", owner).Error)
			case "caller":
				user := &types.User{
					ID:       "same-role-other",
					TenantID: 7,
					Username: "other",
					Email:    "other@isolated.test",
					IsActive: true,
				}
				require.NoError(t, db.Create(user).Error)
				require.NoError(
					t,
					db.Create(
						&types.TenantMember{
							UserID:   user.ID,
							TenantID: 7,
							Role:     types.TenantRoleContributor,
							Status:   types.TenantMemberStatusActive,
						},
					).Error,
				)
				other := types.WithPrincipal(
					types.WithCaller(
						ctx,
						types.Caller{TenantID: 7, UserID: user.ID, Role: types.TenantRoleContributor},
					),
					types.Principal{Type: types.PrincipalWebUser, ID: user.ID},
				)
				_, err = messages.beginMessageMaterialRead(other, request)
				require.Error(t, err)
				return
			case "user-revoked":
				require.NoError(t, db.Model(&types.User{}).Where("id='actual-user'").Update("is_active", false).Error)
			case "member-revoked":
				require.NoError(
					t,
					db.Model(&types.TenantMember{}).
						Where("user_id='actual-user' AND tenant_id=7").
						Update("status", types.TenantMemberStatusSuspended).
						Error,
				)
			}
			require.Error(t, guard.Recheck())
		})
	}
}

func TestMessageMaterialActualSavedReadBoundarySingleConnection(t *testing.T) {
	messages, _, _, db, ctx, session := messageMaterialActualFixture(t)
	_, assistant := produceActualMessagePair(ctx, t, messages, session)
	ctx = WithMessageMaterialOutputBoundary(ctx)
	defer CloseMessageMaterialOutputBoundary(ctx)
	rows, err := messages.GetRecentMessagesBySession(ctx, session.ID, 20)
	require.NoError(t, err)
	require.Len(t, rows, 2)
	require.Equal(t, assistant.Content, rows[1].Content)
	require.NoError(t, CheckMessageMaterialOutputBoundary(ctx))
	var count int64
	require.NoError(
		t,
		db.Table("nextcloud_content_leases").Where("kind='read' AND released_at_ms IS NULL").Count(&count).Error,
	)
	require.Positive(t, count)
	rows[1].Content = "foreign output"
	require.Error(t, CheckMessageMaterialOutputBoundary(ctx))
	rows[1].Content = assistant.Content
	require.NoError(
		t,
		db.Model(&types.Message{}).Where("id=?", assistant.ID).Update("content", "foreign transient").Error,
	)
	require.NoError(
		t,
		db.Model(&types.Message{}).Where("id=?", assistant.ID).Update("content", assistant.Content).Error,
	)
	require.Error(t, CheckMessageMaterialOutputBoundary(ctx))
	require.Error(t, messages.UpdateMessage(ctx, assistant), "a full producer cannot re-seal a foreign partial ABA")
}

func TestMessageMaterialRawUserFieldsSurviveUnconsumedGeneratedUpdates(t *testing.T) {
	messages, _, store, db, ctx, session := messageMaterialActualFixture(t)
	user, assistant := produceActualMessagePair(ctx, t, messages, session)
	request, err := store.CommittedMessageParentRequest(ctx, 7, session.ID, assistant.ID)
	require.NoError(t, err)
	guard, err := messages.beginMessageMaterialRead(ctx, request)
	require.NoError(t, err)
	defer guard.Close()
	require.NoError(
		t,
		messages.UpdateMessageRenderedContent(ctx, session.ID, user.ID, "unadmitted generated controlled context"),
	)
	require.NoError(
		t,
		messages.UpdateMessageImages(
			ctx,
			session.ID,
			user.ID,
			types.MessageImages{{Caption: "unadmitted generated caption"}},
		),
	)
	require.NoError(t, guard.Recheck())
	var raw types.Message
	require.NoError(t, db.Where("id=?", user.ID).Take(&raw).Error)
	require.NotEmpty(t, raw.RenderedContent)
	rows, err := messages.GetMessage(ctx, session.ID, user.ID)
	require.NoError(t, err)
	require.Equal(t, user.Content, rows.Content)
	require.Empty(t, rows.RenderedContent)
	require.Empty(t, rows.Images)
}

func TestMessageMaterialPostgresActualProducerReadDerivedKnowledgeSingleConnection(t *testing.T) {
	kg, store, db, ctx := faqPostgresActualFixture(t)
	kg.graphEngine = parentChildGraphRepo{}
	require.NoError(t, db.Exec("CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\"").Error)
	require.NoError(t, db.Exec("ALTER TABLE messages ADD COLUMN artifacts JSONB").Error)
	for _, name := range []string{
		"000103_message_artifacts_table.up.sql",
		"000107_message_artifacts_deleted_at.up.sql",
		"000127_nextcloud_content_leases.up.sql",
		"000151_message_original_materials.up.sql",
	} {
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
	guard := access.NewNextcloudSourceLineageGuard(
		kg.repo.(access.NextcloudLineageSnapshotLookup),
		kbs,
		nil,
		nil,
		groups,
		nil,
		store,
	)
	messages := &messageService{
		messageRepo:        repository.NewMessageRepository(db),
		sessionRepo:        repository.NewSessionRepository(db),
		sourceLineageGuard: guard,
		kbService:          kbs,
		knowService:        kg,
	}
	ConfigureMessageMaterialProvenance(messages, store)
	session := &types.Session{TenantID: 7, UserID: types.CallerFromContext(ctx).UserID}
	require.NoError(t, db.Create(session).Error)
	_, assistant := produceActualMessagePair(ctx, t, messages, session)
	request, err := store.CommittedMessageParentRequest(ctx, 7, session.ID, assistant.ID)
	require.NoError(t, err)
	original, err := messages.beginMessageMaterialRead(ctx, request)
	require.NoError(t, err)
	require.NoError(t, original.Recheck())
	original.Close()
	target := &types.KnowledgeBase{
		ID:               "history-pg-target",
		TenantID:         7,
		Type:             types.KnowledgeBaseTypeDocument,
		Name:             "actual new history target",
		ChunkingConfig:   types.ChunkingConfig{ChunkSize: 512},
		IndexingStrategy: types.IndexingStrategy{VectorEnabled: true},
		EmbeddingModelID: "embed",
	}
	require.NoError(
		t,
		store.CreateKnowledgeBaseWithInput(
			repository.WithIndependentKnowledgeBaseNameInput(
				repository.WithIndependentKnowledgeInput(ctx),
				"",
				target.Name,
			),
			target,
		),
	)
	kg.kbService = &createKnowledgeFileKBServiceStub{kb: target}
	grant, err := access.ResolveKB(
		ctx,
		access.KBRequest{Caller: types.CallerFromContext(ctx)},
		target,
		types.OrgRoleEditor,
		nil,
		nil,
	)
	require.NoError(t, err)
	ctx = grant.Context(ctx)
	result, err := kg.CreateCommittedHistoryKnowledgeInput(
		ctx,
		types.CommittedHistoryKnowledgeInputRequest{
			TenantID:           7,
			KnowledgeBaseID:    target.ID,
			SessionID:          session.ID,
			AssistantMessageID: assistant.ID,
		},
	)
	require.NoError(t, err)
	queue := kg.task.(*knowledgeInputQueue)
	require.Len(t, queue.tasks, 1)
	require.NoError(
		t,
		kg.ProcessDocument(context.Background(), asynq.NewTask(types.TypeDocumentProcess, queue.tasks[0].Payload())),
	)
	var chunks []*types.Chunk
	require.NoError(t, db.Where("knowledge_id=?", result.Knowledge.ID).Find(&chunks).Error)
	require.NotEmpty(t, chunks)
	plan, err := store.PeekFAQImportOutputReadSetPlan(ctx, nil, []string{result.Knowledge.ID})
	require.NoError(t, err)
	require.Len(t, plan.MessagePlans, 1)
	require.Len(t, plan.Scopes, 2)
	leases := []repository.NextcloudContentLease{}
	for _, scope := range plan.Scopes {
		lease, err := store.ContentLeaseStore().
			AcquireKBRead(ctx, scope.TenantID, scope.KBID, "actual-PG-history-output", time.Minute)
		require.NoError(t, err)
		leases = append(leases, lease)
	}
	covered, err := repository.WithFAQReadLeaseCoverage(ctx, leases)
	require.NoError(t, err)
	covered, err = repository.WithFAQImportOutputReadSetPlan(covered, plan)
	require.NoError(t, err)
	output, err := store.CaptureKnowledgeOutputInputs(
		covered,
		types.KnowledgeBuildInputCaptureRequest{
			TenantID:        7,
			KnowledgeBaseID: target.ID,
			KnowledgeID:     result.Knowledge.ID,
			ChunkIDs:        []string{chunks[0].ID},
		},
	)
	require.NoError(t, err)
	require.NoError(t, output.Proof.SourceLineage.RequireComplete())
	require.NoError(
		t,
		guard.Check(
			covered,
			output.Proof.SourceLineage,
			access.LineageReadScope{
				Targets: []*types.SearchTarget{
					{Type: types.SearchTargetTypeKnowledgeBase, TenantID: 7, KnowledgeBaseID: target.ID},
				},
			},
		),
	)
	require.NoError(t, db.Exec("UPDATE messages SET content=? WHERE id=?", "foreign transient", assistant.ID).Error)
	require.NoError(t, db.Exec("UPDATE messages SET content=? WHERE id=?", assistant.Content, assistant.ID).Error)
	_, err = store.CaptureKnowledgeOutputInputs(
		covered,
		types.KnowledgeBuildInputCaptureRequest{
			TenantID:        7,
			KnowledgeBaseID: target.ID,
			KnowledgeID:     result.Knowledge.ID,
		},
	)
	require.Error(t, err)
	for _, lease := range leases {
		require.NoError(t, store.ContentLeaseStore().ReleaseLease(context.Background(), lease.ID))
	}
	var live int64
	require.NoError(
		t,
		db.Table("nextcloud_content_leases").Where("kind='read' AND released_at_ms IS NULL").Count(&live).Error,
	)
	require.Zero(t, live, "actual PostgreSQL material leases cleaned")
}
