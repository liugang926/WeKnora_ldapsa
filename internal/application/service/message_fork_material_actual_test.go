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
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

type actualForkMaterialSessions struct {
	interfaces.SessionRepository
	writer      forkMaterialSessionStore
	t           *testing.T
	db          *gorm.DB
	beforeWrite func()
	writes      int
}

func (s *actualForkMaterialSessions) CreateForkedFromMaterial(
	ctx context.Context,
	session *types.Session,
	copies []*types.Message,
	snapshot *types.MessageMaterialSnapshot,
) error {
	s.writes++
	assertForkMaterialReadLeases(s.t, s.db, true)
	require.NotEmpty(s.t, snapshot.Proofs)
	if s.beforeWrite != nil {
		s.beforeWrite()
	}
	return s.writer.CreateForkedFromMaterial(ctx, session, copies, snapshot)
}

type actualForkMaterialSandbox struct {
	*fakeForkSandboxPort
	t             *testing.T
	db            *gorm.DB
	beforeControl func()
}

func (s *actualForkMaterialSandbox) HasActiveTurn(ctx context.Context, sessionID string) (bool, error) {
	assertForkMaterialReadLeases(s.t, s.db, true)
	if s.beforeControl != nil {
		before := s.beforeControl
		s.beforeControl = nil
		before()
	}
	return s.fakeForkSandboxPort.HasActiveTurn(ctx, sessionID)
}

func assertForkMaterialReadLeases(t *testing.T, db *gorm.DB, held bool) {
	t.Helper()
	var live int64
	require.NoError(
		t,
		db.Table("nextcloud_content_leases").Where("kind='read' AND released_at_ms IS NULL").Count(&live).Error,
	)
	if held {
		require.Positive(
			t,
			live,
			"actual fork material leases must cover system control checks and final atomic clone write",
		)
	} else {
		require.Zero(t, live)
	}
}

func actualForkMaterialFixture(
	t *testing.T,
) (
	*SessionForkService,
	*messageService,
	*repository.KnowledgeInputProvenanceStore,
	*gorm.DB,
	context.Context,
	*types.Session,
	*types.Message,
	*types.Message,
	*actualForkMaterialSessions,
	*actualForkMaterialSandbox,
) {
	t.Helper()
	messages, _, store, db, ctx, session := messageMaterialActualFixture(t)
	require.NoError(t, db.AutoMigrate(&types.ForkSnapshotLease{}))
	base := time.Now().UTC().Add(-time.Minute)
	user, err := messages.CreateMessage(ctx, &types.Message{
		SessionID:        session.ID,
		RequestID:        "actual-fork-request",
		Role:             "user",
		Content:          "actual original fork question",
		IsCompleted:      true,
		CreatedAt:        base,
		AgentTenantID:    7,
		ExecutionContext: types.MessageExecutionContext{KnowledgeBaseIDs: []string{"kb"}},
		Images:           types.MessageImages{{URL: "https://owned.example.test/human-image.png"}},
		Attachments: types.MessageAttachments{
			{ID: "actual-original-upload", FileName: "actual.txt", Content: "actual authored attachment body"},
		},
	})
	require.NoError(t, err)
	assistant, err := messages.CreateMessage(
		ctx,
		&types.Message{
			SessionID:        session.ID,
			RequestID:        user.RequestID,
			Role:             "assistant",
			CreatedAt:        base.Add(time.Second),
			AgentTenantID:    7,
			ExecutionContext: user.ExecutionContext,
		},
	)
	require.NoError(t, err)
	assistant.Content = "actual original fork answer"
	assistant.IsCompleted = true
	assistant.SourceLineage, _ = types.NewCompleteSourceLineage()
	assistant.AgentSteps = types.AgentSteps{{Thought: "actual original fork thought"}}
	require.NoError(t, messages.UpdateMessage(ctx, assistant))
	repo := repository.NewSessionRepository(db)
	writer, ok := repo.(forkMaterialSessionStore)
	require.True(t, ok, "production repository must supply the atomic original-material fork writer")
	sessions := &actualForkMaterialSessions{SessionRepository: repo, writer: writer, t: t, db: db}
	port := &actualForkMaterialSandbox{
		fakeForkSandboxPort: &fakeForkSandboxPort{
			boundID:    "actual-fork-sandbox",
			bound:      true,
			snapshotID: "actual-original-fork-snapshot",
		},
		t:  t,
		db: db,
	}
	fork := NewSessionForkService(sessions, messages.messageRepo, port)
	fork.lineageVerifier = messages
	return fork, messages, store, db, ctx, session, user, assistant, sessions, port
}

func TestMessageForkActualOriginalCloneAndSavedReadSingleConnection(t *testing.T) {
	fork, messages, _, db, ctx, source, user, assistant, sessions, port := actualForkMaterialFixture(t)
	result, err := fork.Fork(ctx, 7, source.UserID, source.ID, assistant.ID, "actual new human branch title")
	require.NoError(t, err)
	require.True(t, result.Degraded)
	require.Equal(t, ForkDegradeNoCheckpoint, result.Reason)
	require.Equal(t, 1, sessions.writes)
	require.Zero(t, port.snapshotCalls)
	assertForkMaterialReadLeases(t, db, false)
	guard, err := messages.beginMessageMaterialRead(
		ctx,
		types.MessageMaterialReadRequest{
			TenantID:                  7,
			SessionID:                 result.SessionID,
			AllSession:                true,
			IncludeOriginalUserFields: true,
		},
	)
	require.NoError(t, err, "a legitimate new fork must remain readable through actual saved-original receipts")
	require.Len(t, guard.snapshot.Messages, 2)
	for index, original := range []*types.Message{user, assistant} {
		copied2 := guard.snapshot.Messages[index]
		require.NotEqual(t, original.ID, copied2.ID)
		require.Equal(t, result.SessionID, copied2.SessionID)
		require.Equal(t, original.Content, copied2.Content)
		require.True(t, original.CreatedAt.Equal(copied2.CreatedAt))
		require.Equal(t, original.ExecutionContext, copied2.ExecutionContext)
		require.Equal(t, original.AgentTenantID, copied2.AgentTenantID)
		require.Empty(t, copied2.KnowledgeID)
		require.Empty(t, copied2.Artifacts)
		require.NoError(t, copied2.SourceLineage.RequireComplete())
	}
	require.Equal(t, user.Images, guard.snapshot.Messages[0].Images)
	require.Equal(t, user.Attachments, guard.snapshot.Messages[0].Attachments)
	require.Equal(t, assistant.AgentSteps, guard.snapshot.Messages[1].AgentSteps)
	require.Nil(t, guard.snapshot.Messages[1].SandboxCheckpoint)
	require.NotEqual(t, user.RequestID, guard.snapshot.Messages[0].RequestID)
	require.Equal(t, guard.snapshot.Messages[0].RequestID, guard.snapshot.Messages[1].RequestID)
	require.NoError(t, guard.Recheck())
	guard.Close()
	var copiedArtifacts int64
	require.NoError(
		t,
		db.Model(&types.MessageArtifactRecord{}).Where("session_id=?", result.SessionID).Count(&copiedArtifacts).Error,
	)
	require.Zero(t, copiedArtifacts)
	assertForkMaterialReadLeases(t, db, false)
}

func TestMessageForkActualFirstUserAuthenticatesOriginalWithoutCopy(t *testing.T) {
	fork, _, _, db, ctx, source, user, _, sessions, port := actualForkMaterialFixture(t)
	result, err := fork.Fork(ctx, 7, source.UserID, source.ID, user.ID, "")
	require.NoError(t, err)
	require.False(t, result.Degraded)
	require.Equal(t, 1, sessions.writes)
	require.Zero(t, port.snapshotCalls)
	var count int64
	require.NoError(t, db.Model(&types.Message{}).Where("session_id=?", result.SessionID).Count(&count).Error)
	require.Zero(t, count)
	assertForkMaterialReadLeases(t, db, false)
}

func TestMessageForkActualLegacyAndUnknownGeneratedFieldRejectBeforeSnapshot(t *testing.T) {
	for _, mode := range []string{
		"legacy",
		"legacy_unknown_artifact",
		"unknown_user_rendering",
		"unknown_assistant_checkpoint",
	} {
		t.Run(mode, func(t *testing.T) {
			fork, _, _, db, ctx, source, user, assistant, sessions, port := actualForkMaterialFixture(t)
			point := assistant.ID
			switch mode {
			case "legacy", "legacy_unknown_artifact":
				lineage, _ := types.NewCompleteSourceLineage()
				legacy := &types.Message{
					SessionID:     source.ID,
					RequestID:     "legacy-fork",
					Role:          "assistant",
					Content:       "legacy currentbody cannot be newly signed",
					SourceLineage: lineage,
					IsCompleted:   true,
					CreatedAt:     assistant.CreatedAt.Add(time.Second),
				}
				require.NoError(t, db.Create(legacy).Error)
				point = legacy.ID
				if mode == "legacy_unknown_artifact" {
					records := types.NewMessageArtifactRecords(
						source.ID,
						legacy.ID,
						types.MessageArtifacts{
							{
								URL:        "local://7/unknown-artifact",
								FileName:   "unknown-cached-artifact.txt",
								SourcePath: "/workspace/unknown.txt",
							},
						},
					)
					require.NoError(t, db.Create(&records).Error)
				}
			case "unknown_user_rendering":
				require.NoError(
					t,
					db.Model(&types.Message{}).
						Where("id=?", user.ID).
						Update("rendered_content", "unknown generated current rendering").
						Error,
				)
			case "unknown_assistant_checkpoint":
				checkpoint, err := json.Marshal(
					&types.SandboxCheckpoint{
						SandboxID: "unknown-current-sandbox",
						CommitSHA: "1234567890abcdef1234567890abcdef12345678",
					},
				)
				require.NoError(t, err)
				require.NoError(
					t,
					db.Model(&types.Message{}).
						Where("id=?", assistant.ID).
						Update("sandbox_checkpoint", string(checkpoint)).
						Error,
				)
			}
			_, err := fork.Fork(ctx, 7, source.UserID, source.ID, point, "")
			require.Error(t, err)
			require.Zero(t, sessions.writes)
			require.Zero(t, port.snapshotCalls)
			var children int64
			require.NoError(
				t,
				db.Model(&types.Session{}).Where("parent_session_id=?", source.ID).Count(&children).Error,
			)
			require.Zero(t, children)
			assertForkMaterialReadLeases(t, db, false)
		})
	}
}

func TestMessageForkActualSystemControlAndAtomicWriterRecheckOriginalAndCurrentActor(t *testing.T) {
	for _, stage := range []string{"session_control", "atomic_writer"} {
		for _, change := range []string{"body_aba", "owner_aba", "actor_revoke"} {
			t.Run(stage+"/"+change, func(t *testing.T) {
				fork, _, _, db, ctx, source, _, assistant, sessions, port := actualForkMaterialFixture(t)
				changed := false
				mutate := func() {
					assertForkMaterialReadLeases(t, db, true)
					changed = true
					switch change {
					case "body_aba":
						require.NoError(
							t,
							db.Model(&types.Message{}).
								Where("id=?", assistant.ID).
								Update("content", "foreign transient fork body").
								Error,
						)
						require.NoError(
							t,
							db.Model(&types.Message{}).
								Where("id=?", assistant.ID).
								Update("content", assistant.Content).
								Error,
						)
					case "owner_aba":
						require.NoError(
							t,
							db.Model(&types.Session{}).
								Where("id=?", source.ID).
								Update("user_id", "foreign transient fork owner").
								Error,
						)
						require.NoError(
							t,
							db.Model(&types.Session{}).Where("id=?", source.ID).Update("user_id", source.UserID).Error,
						)
					case "actor_revoke":
						require.NoError(
							t,
							db.Model(&types.User{}).Where("id=?", source.UserID).Update("is_active", false).Error,
						)
					}
				}
				if stage == "session_control" {
					port.beforeControl = mutate
				} else {
					sessions.beforeWrite = mutate
				}
				_, err := fork.Fork(ctx, 7, source.UserID, source.ID, assistant.ID, "")
				require.Error(t, err)
				require.True(t, changed)
				require.Empty(t, port.deleted)
				require.Zero(t, port.snapshotCalls)
				var children int64
				require.NoError(
					t,
					db.Model(&types.Session{}).Where("parent_session_id=?", source.ID).Count(&children).Error,
				)
				require.Zero(t, children, "the final original-material denial must leave no partial destination")
				assertForkMaterialReadLeases(t, db, false)
			})
		}
	}
}

func TestMessageForkNativeCopyPreservesHiddenMaterialAndDoesNotAliasOriginal(t *testing.T) {
	lineage, _ := types.NewCompleteSourceLineage()
	original := &types.Message{
		ID:                "original",
		SessionID:         "source",
		Content:           "original body",
		SourceLineage:     lineage,
		AgentTenantID:     7,
		RenderedContent:   "original generated rendering",
		ExecutionContext:  types.MessageExecutionContext{KnowledgeBaseIDs: []string{"original-scope"}},
		ContextCheckpoint: &types.ContextCheckpoint{Summary: "original summary", SourceLineage: lineage},
	}
	copied3, err := cloneForkOriginalMessage(original)
	require.NoError(t, err)
	require.Equal(t, original.SourceLineage, copied3.SourceLineage)
	require.Equal(t, original.AgentTenantID, copied3.AgentTenantID)
	require.Equal(t, original.RenderedContent, copied3.RenderedContent)
	require.Equal(t, original.ExecutionContext, copied3.ExecutionContext)
	require.Equal(t, original.ContextCheckpoint, copied3.ContextCheckpoint)
	copied3.ExecutionContext.KnowledgeBaseIDs[0] = "changed"
	copied3.ContextCheckpoint.Summary = "changed"
	copied3.SourceLineage.State = types.SourceLineageUnknown
	require.Equal(t, "original-scope", original.ExecutionContext.KnowledgeBaseIDs[0])
	require.Equal(t, "original summary", original.ContextCheckpoint.Summary)
	require.NoError(t, original.SourceLineage.RequireComplete())
}

func TestMessageForkActualSignedSourceCloneAndFinalWriterRevoke(t *testing.T) {
	f, _, _, allow, scope := sourceBuildFixture(t, false)
	// Publish the actual parsed original; this fixture does not invoke a model.
	require.NoError(t, f.db.Where("id=?", f.k.ID).Take(f.k).Error)
	f.k.ParseStatus = types.ParseStatusCompleted
	f.k.EnableStatus = "enabled"
	require.NoError(t, f.s.repo.UpdateKnowledge(f.build, f.k))
	require.NoError(t, f.db.Exec("ALTER TABLE messages ADD COLUMN artifacts TEXT").Error)
	for _, name := range []string{
		"000023_message_artifacts_table.up.sql",
		"000026_message_artifacts_deleted_at.up.sql",
		"000070_message_original_materials.up.sql",
	} {
		ddl, err := os.ReadFile("../../../migrations/sqlite/" + name)
		require.NoError(t, err)
		require.NoError(t, f.db.Exec(string(ddl)).Error)
	}
	require.NoError(t, f.db.AutoMigrate(&types.ForkSnapshotLease{}))
	sqlDB, err := f.db.DB()
	require.NoError(t, err)
	sqlDB.SetMaxOpenConns(1)
	sqlDB.SetMaxIdleConns(1)
	ctx := context.WithValue(f.ctx, types.UserIDContextKey, "enrich-human")
	guard := access.NewNextcloudSourceLineageGuard(
		f.s.repo.(access.NextcloudLineageSnapshotLookup),
		f.s.kbService,
		nil,
		nil,
		NewGroupAccessService(repository.NewGroupAccessRepository(f.db)),
		access.NewNextcloudPublicationGuard(
			repository.NewDirectoryRepository(f.db),
			repository.NewDataSourceRepository(f.db),
		),
		f.store,
	)
	messages := &messageService{
		messageRepo:        repository.NewMessageRepository(f.db),
		sessionRepo:        repository.NewSessionRepository(f.db),
		sourceLineageGuard: guard,
		kbService:          f.s.kbService,
		knowService:        f.s,
	}
	ConfigureMessageMaterialProvenance(messages, f.store)
	lease, err := f.store.ContentLeaseStore().
		AcquireKnowledge(ctx, scope, repository.NextcloudContentReadLease, "actual-fork-original-read", time.Minute)
	require.NoError(t, err)
	readCtx, err := repository.WithFAQReadLeaseCoverage(ctx, []repository.NextcloudContentLease{lease})
	require.NoError(t, err)
	originals, err := repository.NewRAGChunkSnapshotReader(f.db, f.store).
		HydrateRAGChunkSnapshots(readCtx, []types.RAGChunkReadRequest{{
			TenantID:        7,
			KnowledgeBaseID: "kb",
			KnowledgeID:     f.k.ID,
			ChunkIDs: []string{
				f.chunks[0].ID,
			},
			Lease: &types.RAGExactReadLease{
				ID:              lease.ID,
				TenantID:        7,
				KnowledgeBaseID: "kb",
				KnowledgeID:     f.k.ID,
				Kind:            string(lease.Kind),
				Epoch:           lease.Epoch,
				OwnerID:         lease.OwnerID,
				ExpiresAt:       lease.ExpiresAt,
			},
		}})
	require.NoError(t, err)
	require.Len(t, originals, 1)
	session := &types.Session{TenantID: 7, UserID: "enrich-human"}
	require.NoError(t, f.db.Create(session).Error)
	base := time.Now().UTC().Add(-time.Minute)
	user, err := messages.CreateMessage(
		readCtx,
		&types.Message{
			SessionID:        session.ID,
			RequestID:        "actual-controlled-fork",
			Role:             "user",
			Content:          "actual controlled-source question",
			IsCompleted:      true,
			CreatedAt:        base,
			ExecutionContext: types.MessageExecutionContext{KnowledgeBaseIDs: []string{"kb"}},
		},
	)
	require.NoError(t, err)
	assistant, err := messages.CreateMessage(
		readCtx,
		&types.Message{
			SessionID:        session.ID,
			RequestID:        user.RequestID,
			Role:             "assistant",
			CreatedAt:        base.Add(time.Second),
			ExecutionContext: user.ExecutionContext,
		},
	)
	require.NoError(t, err)
	assistant.Content = "CONTROLLED FORK ORIGINAL: " + originals[0].Chunk.Content
	assistant.SourceLineage = originals[0].SourceLineage
	assistant.IsCompleted = true
	require.NoError(t, messages.UpdateMessage(readCtx, assistant))
	require.NoError(t, f.store.ContentLeaseStore().ReleaseLease(context.Background(), lease.ID))
	repo := repository.NewSessionRepository(f.db)
	writer, ok := repo.(forkMaterialSessionStore)
	require.True(t, ok)
	sessions := &actualForkMaterialSessions{SessionRepository: repo, writer: writer, t: t, db: f.db}
	port := &actualForkMaterialSandbox{
		fakeForkSandboxPort: &fakeForkSandboxPort{
			boundID:    "actual-fork-sandbox",
			bound:      true,
			snapshotID: "actual-controlled-fork-snapshot",
		},
		t:  t,
		db: f.db,
	}
	fork := NewSessionForkService(sessions, messages.messageRepo, port)
	fork.lineageVerifier = messages
	result, err := fork.Fork(ctx, 7, session.UserID, session.ID, assistant.ID, "")
	require.NoError(t, err)
	var copied types.Message
	require.NoError(t, f.db.Where("session_id=? AND role='assistant'", result.SessionID).Take(&copied).Error)
	replayed, check, closeRead, err := messages.BeginCompletedMessageMaterialReplay(ctx, result.SessionID, copied.ID)
	require.NoError(t, err)
	require.Equal(t, assistant.Content, replayed.Content)
	require.Equal(t, assistant.SourceLineage, replayed.SourceLineage)
	require.NoError(t, check())
	closeRead()
	assertForkMaterialReadLeases(t, f.db, false)
	for _, stage := range []string{"session_control", "atomic_writer"} {
		allow.Store(true)
		port.beforeControl = nil
		sessions.beforeWrite = nil
		observed := false
		revoke := func() {
			var exact int64
			require.NoError(
				t,
				f.db.Table("nextcloud_content_leases").
					Where("kind='read' AND released_at_ms IS NULL AND knowledge_id=?", f.k.ID).
					Count(&exact).
					Error,
			)
			require.Positive(t, exact)
			observed = true
			allow.Store(false)
		}
		if stage == "session_control" {
			port.beforeControl = revoke
		} else {
			sessions.beforeWrite = revoke
		}
		_, err = fork.Fork(ctx, 7, session.UserID, session.ID, assistant.ID, "")
		require.Error(t, err, stage)
		require.True(t, observed, "signed source revocation must occur while the exact original lease is held")
		require.Empty(t, port.deleted)
		require.Zero(t, port.snapshotCalls)
		var children int64
		require.NoError(t, f.db.Model(&types.Session{}).Where("parent_session_id=?", session.ID).Count(&children).Error)
		require.EqualValues(t, 1, children, "revocation at "+stage+" cannot create a second branch")
		assertForkMaterialReadLeases(t, f.db, false)
	}
	require.Zero(t, f.model.calls, "source transfer cannot invoke a paid model")
}
