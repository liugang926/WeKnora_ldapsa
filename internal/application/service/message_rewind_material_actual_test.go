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

type actualRewindMaterialMessages struct {
	interfaces.MessageRepository
	writer       rewindMaterialMessageStore
	t            *testing.T
	db           *gorm.DB
	beforeDelete func()
	deletes      int
}

func (m *actualRewindMaterialMessages) DeleteMessagesFromMaterial(
	ctx context.Context,
	session, point string,
	snapshot *types.MessageMaterialSnapshot,
) ([]*types.Message, error) {
	m.deletes++
	assertForkMaterialReadLeases(m.t, m.db, true)
	require.NotEmpty(m.t, snapshot.Proofs)
	if m.beforeDelete != nil {
		m.beforeDelete()
	}
	deleted, err := m.writer.DeleteMessagesFromMaterial(ctx, session, point, snapshot)
	for _, metadata := range deleted {
		require.Empty(m.t, metadata.Content, "actual truncate cleanup cannot load deleted message bodies")
		require.Empty(m.t, metadata.AgentSteps)
		require.Empty(m.t, metadata.KnowledgeReferences)
		require.Empty(m.t, metadata.Images)
		require.Empty(m.t, metadata.Attachments)
		require.Nil(m.t, metadata.SourceLineage)
	}
	return deleted, err
}

type actualRewindMaterialSandbox struct {
	*fakeRewindSandboxPort
	t             *testing.T
	db            *gorm.DB
	beforeControl func()
}

func (s *actualRewindMaterialSandbox) HasActiveTurn(ctx context.Context, session string) (bool, error) {
	assertForkMaterialReadLeases(s.t, s.db, true)
	if s.beforeControl != nil {
		before := s.beforeControl
		s.beforeControl = nil
		before()
	}
	return s.fakeRewindSandboxPort.HasActiveTurn(ctx, session)
}

func produceActualRewindLaterTurn(
	t *testing.T,
	messages *messageService,
	ctx context.Context,
	session string,
	after time.Time,
) (*types.Message, *types.Message) {
	t.Helper()
	user, err := messages.CreateMessage(
		ctx,
		&types.Message{
			SessionID:        session,
			RequestID:        "actual-rewind-later",
			Role:             "user",
			Content:          "actual later original question",
			IsCompleted:      true,
			CreatedAt:        after.Add(time.Second),
			ExecutionContext: types.MessageExecutionContext{KnowledgeBaseIDs: []string{"kb"}},
		},
	)
	require.NoError(t, err)
	assistant, err := messages.CreateMessage(
		ctx,
		&types.Message{
			SessionID:        session,
			RequestID:        user.RequestID,
			Role:             "assistant",
			CreatedAt:        after.Add(2 * time.Second),
			ExecutionContext: user.ExecutionContext,
		},
	)
	require.NoError(t, err)
	assistant.Content = "actual later original answer"
	assistant.IsCompleted = true
	assistant.SourceLineage, _ = types.NewCompleteSourceLineage()
	require.NoError(t, messages.UpdateMessage(ctx, assistant))
	return user, assistant
}

func actualRewindMaterialService(
	t *testing.T,
	messages *messageService,
	db *gorm.DB,
) (*SessionRewindService, *actualRewindMaterialMessages, *actualRewindMaterialSandbox) {
	t.Helper()
	writer, ok := messages.messageRepo.(rewindMaterialMessageStore)
	require.True(t, ok, "production message repository must supply the actual atomic truncate writer")
	rows := &actualRewindMaterialMessages{MessageRepository: messages.messageRepo, writer: writer, t: t, db: db}
	port := &actualRewindMaterialSandbox{fakeRewindSandboxPort: newFakeRewindPort(), t: t, db: db}
	port.bound = false
	return NewSessionRewindService(messages.sessionRepo, rows, port, messages, nil), rows, port
}

func TestMessageRewindActualOriginalUserAndAssistantCutsSingleConnection(t *testing.T) {
	for _, role := range []string{"user", "assistant"} {
		t.Run(role, func(t *testing.T) {
			_, messages, _, db, ctx, session, firstUser, firstAnswer, _, _ := actualForkMaterialFixture(t)
			laterUser, laterAnswer := produceActualRewindLaterTurn(t, messages, ctx, session.ID, firstAnswer.CreatedAt)
			rewind, rows, port := actualRewindMaterialService(t, messages, db)
			point := firstAnswer.ID
			if role == "user" {
				point = laterUser.ID
			}
			result, err := rewind.Rewind(ctx, 7, session.UserID, session.ID, point)
			require.NoError(t, err)
			require.Equal(t, 2, result.DeletedMessages)
			require.False(t, result.WorkspaceReset)
			require.Equal(t, RewindSkipNoSandbox, result.Reason)
			require.Equal(t, 1, rows.deletes)
			require.Empty(t, port.runner.calls)
			require.False(t, port.rewindHeld)
			var deleted int64
			require.NoError(
				t,
				db.Unscoped().
					Model(&types.Message{}).
					Where("session_id=? AND id IN ? AND deleted_at IS NOT NULL", session.ID, []string{laterUser.ID, laterAnswer.ID}).
					Count(&deleted).
					Error,
			)
			require.EqualValues(t, 2, deleted)
			guard, err := messages.beginMessageMaterialRead(
				ctx,
				types.MessageMaterialReadRequest{
					TenantID:                  7,
					SessionID:                 session.ID,
					AllSession:                true,
					IncludeOriginalUserFields: true,
				},
			)
			require.NoError(t, err)
			require.Equal(t, []string{firstUser.ID, firstAnswer.ID}, messageIDs(guard.snapshot.Messages))
			require.NoError(t, guard.Recheck())
			guard.Close()
			assertForkMaterialReadLeases(t, db, false)
		})
	}
}

func TestMessageRewindActualLegacyCheckpointAndBootstrapRejectWithoutEffects(t *testing.T) {
	for _, mode := range []string{"legacy_boundary", "unknown_checkpoint", "unknown_pending_bootstrap"} {
		t.Run(mode, func(t *testing.T) {
			_, messages, _, db, ctx, session, _, firstAnswer, _, _ := actualForkMaterialFixture(t)
			laterUser, _ := produceActualRewindLaterTurn(t, messages, ctx, session.ID, firstAnswer.CreatedAt)
			rewind, rows, port := actualRewindMaterialService(t, messages, db)
			point := laterUser.ID
			switch mode {
			case "legacy_boundary":
				lineage, _ := types.NewCompleteSourceLineage()
				legacy := &types.Message{
					SessionID:     session.ID,
					RequestID:     "legacy-rewind-boundary",
					Role:          "assistant",
					IsCompleted:   true,
					Content:       "legacy currentbody cannot authorize rewind",
					SourceLineage: lineage,
					CreatedAt:     firstAnswer.CreatedAt.Add(3 * time.Second),
				}
				require.NoError(t, db.Create(legacy).Error)
				point = legacy.ID
			case "unknown_checkpoint":
				checkpoint, err := json.Marshal(&types.SandboxCheckpoint{SandboxID: "sbx-1", CommitSHA: rewindSHA1})
				require.NoError(t, err)
				require.NoError(
					t,
					db.Model(&types.Message{}).
						Where("id=?", firstAnswer.ID).
						Update("sandbox_checkpoint", string(checkpoint)).
						Error,
				)
				port.bound = true
			case "unknown_pending_bootstrap":
				bootstrap, err := json.Marshal(
					&types.ForkBootstrap{
						SnapshotID:      "unknown-cached-snapshot",
						CommitSHA:       rewindSHA1,
						SourceSandboxID: "sbx-1",
					},
				)
				require.NoError(t, err)
				require.NoError(
					t,
					db.Model(&types.Session{}).
						Where("id=?", session.ID).
						Update("fork_bootstrap", string(bootstrap)).
						Error,
				)
			}
			_, err := rewind.Rewind(ctx, 7, session.UserID, session.ID, point)
			require.Error(t, err)
			require.Zero(t, rows.deletes)
			require.Empty(t, port.runner.calls)
			require.False(t, port.rewindHeld)
			var deleted int64
			require.NoError(
				t,
				db.Unscoped().
					Model(&types.Message{}).
					Where("session_id=? AND deleted_at IS NOT NULL", session.ID).
					Count(&deleted).
					Error,
			)
			require.Zero(t, deleted)
			assertForkMaterialReadLeases(t, db, false)
		})
	}
}

func TestMessageRewindActualSystemControlAndAtomicDeleteRejectABAAndRevocation(t *testing.T) {
	for _, stage := range []string{"session_control", "atomic_delete"} {
		for _, mode := range []string{"body_aba", "owner_aba", "control_aba", "actor_revoke", "scope_revoke"} {
			t.Run(stage+"/"+mode, func(t *testing.T) {
				_, messages, _, db, ctx, session, _, firstAnswer, _, _ := actualForkMaterialFixture(t)
				point, _ := produceActualRewindLaterTurn(t, messages, ctx, session.ID, firstAnswer.CreatedAt)
				rewind, rows, port := actualRewindMaterialService(t, messages, db)
				changed := false
				mutate := func() {
					assertForkMaterialReadLeases(t, db, true)
					changed = true
					switch mode {
					case "body_aba":
						require.NoError(
							t,
							db.Model(&types.Message{}).
								Where("id=?", point.ID).
								Update("content", "foreign transient rewind body").
								Error,
						)
						require.NoError(
							t,
							db.Model(&types.Message{}).Where("id=?", point.ID).Update("content", point.Content).Error,
						)
					case "owner_aba":
						require.NoError(
							t,
							db.Model(&types.Session{}).
								Where("id=?", session.ID).
								Update("user_id", "foreign rewind owner").
								Error,
						)
						require.NoError(
							t,
							db.Model(&types.Session{}).
								Where("id=?", session.ID).
								Update("user_id", session.UserID).
								Error,
						)
					case "control_aba":
						require.NoError(
							t,
							db.Model(&types.Message{}).
								Where("id=?", point.ID).
								Update("created_at", point.CreatedAt.Add(time.Second)).
								Error,
						)
						require.NoError(
							t,
							db.Model(&types.Message{}).
								Where("id=?", point.ID).
								Update("created_at", point.CreatedAt).
								Error,
						)
					case "actor_revoke":
						require.NoError(
							t,
							db.Model(&types.User{}).Where("id=?", session.UserID).Update("is_active", false).Error,
						)
					case "scope_revoke":
						require.NoError(
							t,
							db.Model(&types.KnowledgeBase{}).Where("id='kb'").Update("deleted_at", time.Now()).Error,
						)
					}
				}
				if stage == "session_control" {
					port.beforeControl = mutate
				} else {
					rows.beforeDelete = mutate
				}
				_, err := rewind.Rewind(ctx, 7, session.UserID, session.ID, point.ID)
				require.Error(t, err)
				require.True(t, changed)
				require.Empty(t, port.runner.calls)
				require.False(t, port.rewindHeld)
				var deleted int64
				require.NoError(
					t,
					db.Unscoped().
						Model(&types.Message{}).
						Where("session_id=? AND deleted_at IS NOT NULL", session.ID).
						Count(&deleted).
						Error,
				)
				require.Zero(t, deleted, "final material denial cannot delete any selected row")
				assertForkMaterialReadLeases(t, db, false)
			})
		}
	}
}

func actualRewindSignedSourceFixture(
	t *testing.T,
) (*messageService, *gorm.DB, context.Context, *types.Session, *types.Message, func(bool), string, func() int) {
	t.Helper()
	f, _, _, allow, scope := sourceBuildFixture(t, false)
	require.NoError(t, f.db.Where("id=?", f.k.ID).Take(f.k).Error)
	f.k.ParseStatus = types.ParseStatusCompleted
	f.k.EnableStatus = "enabled"
	require.NoError(t, f.s.repo.UpdateKnowledge(f.build, f.k))
	require.NoError(t, f.db.Exec("ALTER TABLE messages ADD COLUMN artifacts TEXT").Error)
	for _, name := range []string{"000023_message_artifacts_table.up.sql", "000026_message_artifacts_deleted_at.up.sql", "000070_message_original_materials.up.sql"} {
		ddl, err := os.ReadFile("../../../migrations/sqlite/" + name)
		require.NoError(t, err)
		require.NoError(t, f.db.Exec(string(ddl)).Error)
	}
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
		AcquireKnowledge(ctx, scope, repository.NextcloudContentReadLease, "actual-rewind-original-read", time.Minute)
	require.NoError(t, err)
	readCtx, err := repository.WithFAQReadLeaseCoverage(ctx, []repository.NextcloudContentLease{lease})
	require.NoError(t, err)
	originals, err := repository.NewRAGChunkSnapshotReader(f.db, f.store).
		HydrateRAGChunkSnapshots(readCtx, []types.RAGChunkReadRequest{{TenantID: 7, KnowledgeBaseID: "kb", KnowledgeID: f.k.ID, ChunkIDs: []string{f.chunks[0].ID}, Lease: &types.RAGExactReadLease{ID: lease.ID, TenantID: 7, KnowledgeBaseID: "kb", KnowledgeID: f.k.ID, Kind: string(lease.Kind), Epoch: lease.Epoch, OwnerID: lease.OwnerID, ExpiresAt: lease.ExpiresAt}}})
	require.NoError(t, err)
	require.Len(t, originals, 1)
	session := &types.Session{TenantID: 7, UserID: "enrich-human"}
	require.NoError(t, f.db.Create(session).Error)
	base := time.Now().UTC().Add(-time.Minute)
	user, err := messages.CreateMessage(
		readCtx,
		&types.Message{
			SessionID:        session.ID,
			RequestID:        "actual-controlled-rewind",
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
	assistant.Content = "CONTROLLED REWIND ORIGINAL: " + originals[0].Chunk.Content
	assistant.SourceLineage = originals[0].SourceLineage
	assistant.IsCompleted = true
	require.NoError(t, messages.UpdateMessage(readCtx, assistant))
	require.NoError(t, f.store.ContentLeaseStore().ReleaseLease(context.Background(), lease.ID))
	return messages, f.db, ctx, session, assistant, allow.Store, f.k.ID, func() int { return f.model.calls }
}

func TestMessageRewindActualSignedSourceDeletesAndFinalWriterRevoke(t *testing.T) {
	for _, revoke := range []bool{false, true} {
		name := "allowed"
		if revoke {
			name = "revoked_before_actual_delete"
		}
		t.Run(name, func(t *testing.T) {
			messages, db, ctx, session, firstAnswer, setAllow, sourceID, modelCalls := actualRewindSignedSourceFixture(
				t,
			)
			point, _ := produceActualRewindLaterTurn(t, messages, ctx, session.ID, firstAnswer.CreatedAt)
			rewind, rows, port := actualRewindMaterialService(t, messages, db)
			observed := false
			rows.beforeDelete = func() {
				var exact int64
				require.NoError(
					t,
					db.Table("nextcloud_content_leases").
						Where("kind='read' AND released_at_ms IS NULL AND knowledge_id=?", sourceID).
						Count(&exact).
						Error,
				)
				require.Positive(t, exact)
				observed = true
				if revoke {
					setAllow(false)
				}
			}
			result, err := rewind.Rewind(ctx, 7, session.UserID, session.ID, point.ID)
			require.True(t, observed)
			var deleted int64
			require.NoError(
				t,
				db.Unscoped().
					Model(&types.Message{}).
					Where("session_id=? AND deleted_at IS NOT NULL", session.ID).
					Count(&deleted).
					Error,
			)
			if revoke {
				require.Error(t, err)
				require.Zero(t, deleted)
			} else {
				require.NoError(t, err)
				require.Equal(t, 2, result.DeletedMessages)
				require.EqualValues(t, 2, deleted)
			}
			require.Empty(t, port.runner.calls)
			require.False(t, port.rewindHeld)
			assertForkMaterialReadLeases(t, db, false)
			require.Zero(t, modelCalls())
		})
	}
}

func TestMessageRewindPostgresActualOriginalCutAndOutputCurrentCaller(t *testing.T) {
	messages, db, ctx, session := messageNativePostgresFixture(t)
	_, first := produceActualMessagePair(t, messages, ctx, session)
	laterUser, _ := produceActualRewindLaterTurn(t, messages, ctx, session.ID, first.CreatedAt)
	rewind, _, _ := actualRewindMaterialService(t, messages, db)
	result, err := rewind.Rewind(WithRewindOriginalHTTPOutput(ctx), 7, session.UserID, session.ID, laterUser.ID)
	require.NoError(t, err)
	require.Equal(t, 2, result.DeletedMessages)
	require.NotNil(t, result.CheckOriginalOutput)
	defer result.CloseOriginalOutput()
	require.NoError(t, result.CheckOriginalOutput())
	require.NoError(t, db.Model(&types.User{}).Where("id=?", session.UserID).Update("is_active", false).Error)
	require.Error(t, result.CheckOriginalOutput())
}

func TestMessageRewindActualForeignKnowledgeCleanupLinkRejects(t *testing.T) {
	_, messages, _, db, ctx, session, _, first, _, _ := actualForkMaterialFixture(t)
	_, later := produceActualRewindLaterTurn(t, messages, ctx, session.ID, first.CreatedAt)
	require.NoError(
		t,
		db.Model(&types.Message{}).Where("id=?", later.ID).Update("knowledge_id", "unrelated-current-knowledge").Error,
	)
	rewind, _, _ := actualRewindMaterialService(t, messages, db)
	_, err := rewind.Rewind(ctx, 7, session.UserID, session.ID, first.ID)
	require.Error(t, err)
	var count int64
	require.NoError(
		t,
		db.Unscoped().
			Model(&types.Message{}).
			Where("session_id=? AND deleted_at IS NOT NULL", session.ID).
			Count(&count).
			Error,
	)
	require.Zero(t, count)
}
