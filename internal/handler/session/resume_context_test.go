package session

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/agent"
	"github.com/Tencent/WeKnora/internal/agent/hitl"
	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/application/service"
	chatpipeline "github.com/Tencent/WeKnora/internal/application/service/chat_pipeline"
	"github.com/Tencent/WeKnora/internal/stream"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

type resumeMessages struct {
	interfaces.MessageService
	rows map[string]*types.Message
}

type resumeKBService struct {
	interfaces.KnowledgeBaseService
	*wikiFixerKBLookupStub
}

func (s *resumeKBService) GetKnowledgeBaseByIDOnly(ctx context.Context, id string) (*types.KnowledgeBase, error) {
	return s.wikiFixerKBLookupStub.GetKnowledgeBaseByIDOnly(ctx, id)
}

type resumeKBShareService struct {
	interfaces.KBShareService
	*wikiFixerKBShareStub
}

func (s *resumeKBShareService) CheckTenantKBPermission(
	ctx context.Context, kbID string, tenantID uint64, role types.TenantRole,
) (types.OrgMemberRole, bool, error) {
	return s.wikiFixerKBShareStub.CheckTenantKBPermission(ctx, kbID, tenantID, role)
}

func TestBuildResumeRequestRevalidatesSharedKBBuiltin(t *testing.T) {
	require.NoError(t, types.LoadBuiltinAgentsConfig(filepath.Join("..", "..", "..", "config")))
	for _, agentID := range []string{types.BuiltinWeKnoraAssistantID, types.BuiltinWikiFixerID} {
		for _, allowed := range []bool{true, false} {
			name := agentID + "/editor"
			if !allowed {
				name = agentID + "/revoked"
			}
			t.Run(name, func(t *testing.T) {
				c, ctx := newResolveAgentTestContext(1)
				ctx = context.WithValue(ctx, types.TenantRoleContextKey, types.TenantRoleContributor)
				messages := &resumeMessages{rows: map[string]*types.Message{
					"m": {ID: "m", SessionID: "s", RunState: types.MessageRunStateAwaitingInput},
				}}
				b := hitl.NewBroker(nil, nil)
				request, err := json.Marshal(types.ResumableAgentRequest{
					AgentID: agentID, AgentTenantID: 2, KnowledgeBaseIDs: []string{"shared-kb"},
					PageContext: &types.PageContext{KnowledgeBaseID: "shared-kb"},
				})
				require.NoError(t, err)
				snapshot, err := json.Marshal(agent.RunSnapshot{Version: agent.RunSnapshotVersion, Request: request})
				require.NoError(t, err)
				require.NoError(t, b.Suspensions().Save(ctx, &types.AgentRunSuspension{
					AssistantMessageID: "m", PromptID: "p", Snapshot: types.JSON(snapshot),
				}))
				permission := types.OrgRoleViewer
				if allowed {
					permission = types.OrgRoleEditor
				}
				share := &wikiFixerKBShareStub{permission: permission, isShared: true}
				own := types.GetBuiltinAgent(agentID, 1)
				h := &Handler{
					promptBroker: b, messageService: messages, sessionService: &resumeSessionService{},
					streamManager:      stream.NewMemoryStreamManager(),
					customAgentService: &resolveOwnAgentStub{agent: own},
					agentShareService:  &resolveAgentShareStub{err: service.ErrAgentShareNotFound},
					knowledgebaseService: &resumeKBService{wikiFixerKBLookupStub: &wikiFixerKBLookupStub{
						kb: &types.KnowledgeBase{ID: "shared-kb", TenantID: 2},
					}},
					kbShareService: &resumeKBShareService{wikiFixerKBShareStub: share},
				}
				req, _, err := h.buildResumeRequest(ctx, c, &types.AgentPrompt{
					ID: "p", SessionID: "s", AssistantMessageID: "m", Kind: types.AgentPromptKindToolApproval,
				})
				if !allowed {
					require.Error(t, err)
					require.Nil(t, req)
					require.Equal(t, types.MessageRunStateAwaitingInput, messages.rows["m"].RunState)
				} else {
					require.NoError(t, err)
					require.Equal(t, uint64(2), req.effectiveTenantID)
					require.Equal(t, uint64(2), req.customAgent.TenantID)
					require.Equal(t, []string{"shared-kb"}, req.customAgent.Config.KnowledgeBases)
					require.False(t, req.sharedAgentReadOnly)
					require.Equal(t, "shared-kb", req.buildQARequest().PageContext.KnowledgeBaseID)
				}
				require.Equal(t, "shared-kb", share.checkedKBID)
				require.Equal(t, uint64(1), share.checkedTenantID)
				require.Equal(t, uint64(1), own.TenantID)
			})
		}
	}
}

func (m *resumeMessages) GetMessage(_ context.Context, sessionID, id string) (*types.Message, error) {
	row := m.rows[id]
	if row == nil || row.SessionID != sessionID {
		return nil, nil
	}
	cloned := *row
	return &cloned, nil
}

func (m *resumeMessages) UpdateMessage(_ context.Context, row *types.Message) error {
	cloned := *row
	m.rows[row.ID] = &cloned
	return nil
}

type resumeSessionService struct{ interfaces.SessionService }

func (*resumeSessionService) GetOwnedSession(_ context.Context, id string) (*types.Session, error) {
	return &types.Session{ID: id, TenantID: 1}, nil
}

func TestBuildResumeRequestKeepsTurnAndOriginalSteeringSnapshot(t *testing.T) {
	c, ctx := newResolveAgentTestContext(1)
	messages := &resumeMessages{rows: map[string]*types.Message{
		"m": {ID: "m", SessionID: "sess-1", RequestID: "original-turn", RunState: types.MessageRunStateAwaitingInput},
		"u": {ID: "u", SessionID: "sess-1", Role: "user", Content: "use staging"},
	}}
	b := hitl.NewBroker(nil, nil)
	request, err := json.Marshal(types.ResumableAgentRequest{AgentID: "a", UserMessageID: "first"})
	require.NoError(t, err)
	snapshot, err := json.Marshal(agent.RunSnapshot{
		Version: agent.RunSnapshotVersion, Query: "original question", Request: request,
		PendingRound: agent.PendingRound{Step: types.AgentStep{UserMessagesBefore: []string{"u"}}},
	})
	require.NoError(t, err)
	require.NoError(t, b.Suspensions().Save(ctx, &types.AgentRunSuspension{
		AssistantMessageID: "m", PromptID: "p", Snapshot: types.JSON(snapshot),
	}))
	h := &Handler{
		promptBroker: b, messageService: messages, streamManager: stream.NewMemoryStreamManager(),
		sessionService:     &resumeSessionService{},
		customAgentService: &resolveOwnAgentStub{agent: &types.CustomAgent{ID: "a"}},
	}
	req, _, err := h.buildResumeRequest(ctx, c, &types.AgentPrompt{
		ID: "p", SessionID: "sess-1", AssistantMessageID: "m", RequestID: "transport",
	})
	require.NoError(t, err)
	require.Equal(t, "original question", req.query)
	require.Equal(t, req.query, req.userInput)
	require.Equal(t, "original-turn", req.requestID)
	require.Equal(t, "original question", req.buildQARequest().Query)
	require.Equal(t, snapshot, req.resume.Snapshot)
}

func recoveryMaterialStore(db *gorm.DB) *repository.KnowledgeInputProvenanceStore {
	newAuthority := func(tx *gorm.DB) *repository.KnowledgeInputProvenanceStore {
		store := repository.NewKnowledgeInputProvenanceStore(tx)
		groups := service.NewGroupAccessService(repository.NewGroupAccessRepository(tx))
		repository.ConfigureKnowledgeInputActorGroupAccess(store, groups)
		return store
	}
	store := newAuthority(db)
	repository.ConfigureKnowledgeDisplayInputWriters(store, nil, nil, newAuthority)
	return store
}

func newCrashRecoveryFixture(t *testing.T) (*Handler, context.Context, *types.AgentPrompt, *gorm.DB) {
	t.Helper()
	db, err := gorm.Open(sqlite.Open(filepath.Join(t.TempDir(), "recovery.db")), &gorm.Config{})
	require.NoError(t, err)
	sqlDB, err := db.DB()
	require.NoError(t, err)
	t.Cleanup(func() { _ = sqlDB.Close() })
	require.NoError(t, db.AutoMigrate(
		&types.User{}, &types.TenantMember{}, &types.Session{}, &types.Message{}, &types.MessageArtifactRecord{},
		&types.AgentPrompt{}, &types.AgentRunSuspension{},
	))
	for _, migration := range []string{"000028_directory_group_access", "000070_message_original_materials"} {
		ddl, err := os.ReadFile(filepath.Join("../../../migrations/sqlite", migration+".up.sql"))
		require.NoError(t, err)
		require.NoError(t, db.Exec(string(ddl)).Error)
	}
	session := &types.Session{TenantID: 1, UserID: "owner", OwnerAuthSource: types.AuthSourceWeb}
	require.NoError(t, db.Create(session).Error)
	require.NoError(t, db.Create(&types.User{ID: session.UserID, IsActive: true}).Error)
	role := types.TenantRoleContributor
	require.NoError(t, db.Create(&types.TenantMember{
		TenantID: session.TenantID, UserID: session.UserID, Role: role,
	}).Error)
	principal := types.Principal{Type: types.PrincipalWebUser, ID: session.UserID}
	ctx := types.WithExecutionTenant(types.WithPrincipal(t.Context(), principal), session.TenantID)
	ctx = types.WithCaller(ctx, types.Caller{TenantID: session.TenantID, UserID: session.UserID, Role: role})
	ctx = context.WithValue(ctx, types.UserIDContextKey, session.UserID)

	groups := service.NewGroupAccessService(repository.NewGroupAccessRepository(db))
	users := service.NewUserService(nil, repository.NewUserRepository(db), nil, nil, nil, nil, nil, groups, nil, nil)
	store := recoveryMaterialStore(db)
	msgRepo := repository.NewMessageRepository(db)
	messages := service.NewMessageService(
		msgRepo, repository.NewSessionRepository(db), nil, nil, nil, nil, nil, nil, nil,
	)
	guard := access.NewNextcloudSourceLineageGuard(nil, nil, nil, nil, groups, nil, store)
	service.ConfigureMessageSourceLineage(
		messages, nil, &service.SessionForkService{}, &chatpipeline.EventManager{}, guard,
	)
	service.ConfigureMessageMaterialProvenance(messages, store)
	broker := hitl.NewBroker(repository.NewAgentPromptRepository(db), nil,
		hitl.WithSuspensionStore(repository.NewAgentRunSuspensionRepository(db)))
	h := &Handler{messageService: messages, streamManager: stream.NewMemoryStreamManager()}
	h.SetSuspendedRuns(broker, service.NewAuthorizedSuspendedRunService(broker, msgRepo, db, messages, groups, users))

	userMessage, err := messages.CreateMessage(ctx, &types.Message{
		SessionID: session.ID, RequestID: "turn", Role: "user", Content: "question", IsCompleted: true,
	})
	require.NoError(t, err)
	message, err := messages.CreateMessage(ctx, &types.Message{
		SessionID: session.ID, RequestID: userMessage.RequestID, Role: "assistant", AgentID: "agent",
		AgentTenantID: session.TenantID,
	})
	require.NoError(t, err)
	prompt := &types.AgentPrompt{
		ID: "prompt", UserID: principal.StorageID(), TenantID: session.TenantID, SessionID: session.ID,
		AssistantMessageID: message.ID, RequestID: message.RequestID, Kind: types.AgentPromptKindToolApproval,
		Status: types.AgentPromptAnswered, Suspendable: true,
	}
	require.NoError(t, broker.Store().Create(ctx, prompt))
	request, err := json.Marshal(types.ResumableAgentRequest{
		AgentID: message.AgentID, AgentTenantID: session.TenantID, UserMessageID: userMessage.ID,
	})
	require.NoError(t, err)
	lineage, err := types.NewCompleteSourceLineage()
	require.NoError(t, err)
	snapshot, err := json.Marshal(agent.RunSnapshot{
		Version: agent.RunSnapshotVersion, Request: request, WeKnoraACL: true, SourceLineage: lineage,
		CompletedSteps: []types.AgentStep{{Thought: "saved work"}},
	})
	require.NoError(t, err)
	require.NoError(t, broker.Suspensions().Save(ctx, &types.AgentRunSuspension{
		TenantID: session.TenantID, SessionID: session.ID, AssistantMessageID: message.ID, PromptID: prompt.ID,
		SnapshotVersion: agent.RunSnapshotVersion, Snapshot: snapshot,
	}))
	require.NoError(t, h.streamManager.SetLiveRun(ctx, session.ID, message.ID, "old"))
	return h, ctx, prompt, db
}

func TestResumeRecoversOwnerThatDiedBeforeSavingMessage(t *testing.T) {
	for _, tc := range []struct {
		name        string
		ownerGone   bool
		transferred bool
	}{
		{name: "reaped", ownerGone: true},
		{name: "self-suspended"},
		{name: "session-transferred", ownerGone: true, transferred: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			shortResumeSettle(t, 20*time.Millisecond)
			h, ctx, prompt, db := newCrashRecoveryFixture(t)
			if tc.transferred {
				require.NoError(t, db.Model(&types.Session{}).
					Where("id = ?", prompt.SessionID).Update("user_id", "new-owner").Error)
			}
			past := time.Now().Add(-time.Second)
			err := h.awaitSuspendedRunSettled(ctx, prompt, tc.ownerGone, &past)
			var message types.Message
			require.NoError(t, db.First(&message, "id = ?", prompt.AssistantMessageID).Error)
			if tc.transferred {
				require.Error(t, err)
				require.Empty(t, message.AgentSteps)
				return
			}
			require.NoError(t, err)
			require.Equal(t, types.MessageRunStateAwaitingInput, message.RunState)
			require.NotEmpty(t, message.AgentSteps)
			require.Equal(t, "saved work", message.AgentSteps[0].Thought)
			live, _, err := h.streamManager.GetLiveRun(ctx, prompt.SessionID)
			require.NoError(t, err)
			require.Empty(t, live)
		})
	}
}
