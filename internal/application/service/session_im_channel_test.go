package service

import (
	"context"
	"errors"
	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/application/repository"
	chatpipeline "github.com/Tencent/WeKnora/internal/application/service/chat_pipeline"
	"github.com/Tencent/WeKnora/internal/logger"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestIMOriginalSyntheticIdentityFailure(t *testing.T) {
	ctx := context.WithValue(context.Background(), types.TenantIDContextKey, uint64(10000))
	ctx = context.WithValue(ctx, types.UserIDContextKey, "system-10000")
	ctx = types.WithPrincipal(ctx, types.Principal{Type: types.PrincipalIMUser, ID: "10000:channel:dingtalk:sender"})
	groups := &groupAccessService{now: time.Now, userEligibility: func(context.Context, string) error { return errors.New("load executing user: user not found") }}
	svc := &sessionService{groupAccess: groups}
	_, err := svc.CreateSession(ctx, &types.Session{TenantID: 10000})
	require.EqualError(t, err, "load executing user: user not found")
}

type imSessionRepo struct {
	interfaces.SessionRepository
	created int
}

func (r *imSessionRepo) Create(_ context.Context, s *types.Session) (*types.Session, error) {
	r.created++
	return s, nil
}
func imScopeContext() context.Context {
	ctx := context.WithValue(context.Background(), types.TenantIDContextKey, uint64(10000))
	ctx = context.WithValue(ctx, types.UserIDContextKey, "system-10000")
	ctx = types.WithPrincipal(ctx, types.Principal{Type: types.PrincipalIMUser, ID: "10000:channel:dingtalk:sender"})
	return types.WithIMChannelScope(ctx, types.IMChannelScope{TenantID: 10000, AgentID: "agent", KnowledgeBases: map[string]uint64{"kb": 10000}, Verify: func(context.Context, uint64, types.ResourceType, string, types.ResourceAction) error { return nil }})
}
func TestIMTrustedChannelCreatesSessionAndConstrainedRead(t *testing.T) {
	ctx := imScopeContext()
	users := 0
	groups := &groupAccessService{now: time.Now, userEligibility: func(context.Context, string) error { users++; return errors.New("load executing user: user not found") }}
	repo := &imSessionRepo{}
	svc := &sessionService{groupAccess: groups, sessionRepo: repo}
	_, err := svc.CreateSession(ctx, &types.Session{ID: "session", TenantID: 10000})
	require.NoError(t, err)
	require.Equal(t, 1, repo.created)
	require.Zero(t, users)
	for _, clone := range []context.Context{ctx, logger.CloneContext(ctx)} {
		require.NoError(t, groups.Authorize(clone, 10000, types.GroupResourceTypeKnowledgeBase, "kb", types.ResourceActionRead))
		require.Error(t, groups.Authorize(clone, 10000, types.GroupResourceTypeKnowledgeBase, "other", types.ResourceActionRead))
		require.Error(t, groups.Authorize(clone, 10001, types.GroupResourceTypeKnowledgeBase, "kb", types.ResourceActionRead))
		require.Error(t, groups.Authorize(clone, 10000, types.GroupResourceTypeKnowledgeBase, "kb", types.ResourceActionEdit))
		require.Error(t, groups.Authorize(clone, 10000, types.GroupResourceTypeAgent, "other", types.ResourceActionUse))
		require.Error(t, types.AuthorizeTenantAPIKeyKnowledgeBases(clone, "other"))
	}
	_, err = svc.CreateSession(ctx, &types.Session{TenantID: 10001})
	require.Error(t, err)
	changed := types.WithPrincipal(ctx, types.Principal{Type: types.PrincipalWebUser, ID: "system-10000"})
	_, err = svc.CreateSession(changed, &types.Session{TenantID: 10000})
	require.Error(t, err)
}

type imScopeKBs struct {
	interfaces.KnowledgeBaseService
	kbs []*types.KnowledgeBase
}

func (f imScopeKBs) ListKnowledgeBases(ctx context.Context) ([]*types.KnowledgeBase, error) {
	var rows []*types.KnowledgeBase
	for _, k := range f.kbs {
		if k.TenantID == types.CallerFromContext(ctx).TenantID {
			rows = append(rows, k)
		}
	}
	return rows, nil
}
func (f imScopeKBs) GetKnowledgeBaseByIDOnly(_ context.Context, id string) (*types.KnowledgeBase, error) {
	for _, k := range f.kbs {
		if k.ID == id {
			return k, nil
		}
	}
	return nil, errors.New("missing")
}
func TestIMAgentKnowledgeScopeModesAndDeletedResource(t *testing.T) {
	ctx := context.WithValue(context.Background(), types.TenantIDContextKey, uint64(10000))
	ctx = context.WithValue(ctx, types.UserIDContextKey, "system-10000")
	svc := &sessionService{knowledgeBaseService: imScopeKBs{kbs: []*types.KnowledgeBase{{ID: "kb", TenantID: 10000}}}}
	for _, mode := range []string{"none", "selected", ""} {
		ids, err := svc.ResolveIMChannelKnowledgeBases(ctx, &types.CustomAgent{ID: "agent", TenantID: 10000, Config: types.CustomAgentConfig{KBSelectionMode: mode}})
		require.NoError(t, err)
		require.Empty(t, ids)
	}
	agent := &types.CustomAgent{ID: "agent", TenantID: 10000, Config: types.CustomAgentConfig{KBSelectionMode: "selected", KnowledgeBases: []string{"kb"}}}
	ids, err := svc.ResolveIMChannelKnowledgeBases(ctx, agent)
	require.NoError(t, err)
	require.Equal(t, map[string]uint64{"kb": 10000}, ids)
	agent.Config.KnowledgeBases = []string{"deleted"}
	_, err = svc.ResolveIMChannelKnowledgeBases(ctx, agent)
	require.Error(t, err)
	require.Error(t, svc.CheckIMChannelKnowledgeBase(ctx, 10001, "kb"))
}

type imDisabledDirectory struct {
	interfaces.DirectoryRuntimeService
}

func (imDisabledDirectory) GetStatus(context.Context) (*types.DirectoryHealth, error) {
	return &types.DirectoryHealth{Enabled: false}, nil
}

type imManagedKB struct{ NextcloudScopedGrantReader }

func (imManagedKB) ManagedKB(context.Context, uint64, string) (bool, error) { return true, nil }
func TestIMChannelDoesNotBypassDisabledNextcloudDirectory(t *testing.T) {
	group := &groupAccessService{now: time.Now, directoryRuntime: imDisabledDirectory{}, nextcloudScopes: imManagedKB{}}
	require.Error(t, group.Authorize(imScopeContext(), 10000, types.GroupResourceTypeKnowledgeBase, "kb", types.ResourceActionRead))
}

// Runs the real session repository, user eligibility, retrieval and generation
// pipeline; only external model/vector services use the existing test fixture.
func TestIMChannelActualSessionAndRAGPipeline(t *testing.T) {
	model := &ragEntryModel{}
	f := NewNormalReferencesHTTPFixtureForTest(t, false, model)
	svc := f.Sessions.(normalReferencesSessionForTest).SessionService.(*sessionService)
	ConfigureGroupAccessUserEligibility(svc.groupAccess, &userService{userRepo: repository.NewUserRepository(f.DB)})
	chatpipeline.NewPluginSearch(svc.eventManager, f.KBs, svc.knowledgeService, svc.chunkService, f.Config, nil, svc.tenantService, nil, nil, nil)
	chatpipeline.NewPluginChatCompletion(svc.eventManager, f.Models)
	ctx := types.WithCaller(f.Ctx, types.Caller{TenantID: 7, UserID: "system-7", Role: types.TenantRoleViewer})
	ctx = context.WithValue(ctx, types.UserIDContextKey, "system-7")
	ctx = types.WithPrincipal(ctx, types.Principal{Type: types.PrincipalIMUser, ID: "7:channel:dingtalk:sender"})
	_, err := svc.CreateSession(ctx, &types.Session{TenantID: 7})
	require.ErrorContains(t, err, "load executing user")
	ctx = types.WithIMChannelScope(ctx, types.IMChannelScope{TenantID: 7, AgentID: "agent", OwnerID: "im_session:fixture", KnowledgeBases: map[string]uint64{"kb": 7}, Verify: func(context.Context, uint64, types.ResourceType, string, types.ResourceAction) error { return nil }})
	session, err := svc.CreateSession(ctx, &types.Session{TenantID: 7})
	require.NoError(t, err)
	require.NotEmpty(t, session.ID)
	var users int64
	require.NoError(t, f.DB.Model(&types.User{}).Where("id = ?", "system-7").Count(&users).Error)
	require.Zero(t, users)
	require.NoError(t, f.DB.Model(&types.Knowledge{}).Where("knowledge_base_id = ?", "kb").Updates(map[string]any{"parse_status": types.ParseStatusCompleted, "pending_subtasks_count": 0, "enable_status": "enabled"}).Error)
	cm := &types.ChatManage{}
	cm.Query = "What is the current employee policy?"
	cm.RewriteQuery = cm.Query
	cm.ChatModelID = "normal-reference-model"
	cm.KnowledgeBaseIDs = []string{"kb"}
	cm.SearchTargets = types.SearchTargets{{Type: types.SearchTargetTypeKnowledgeBase, TenantID: 7, KnowledgeBaseID: "kb"}}
	cm.EmbeddingTopK = 2
	cm.RerankTopK = 2
	cm.SummaryConfig.ContextTemplate = "{{query}}\n{{contexts}}"
	ctx, err = svc.eventManager.BeginSourceLineage(ctx, cm, access.LineageReadScope{Targets: cm.SearchTargets}, svc.knowledgeQAAuthorizer(session))
	require.NoError(t, err)
	require.NoError(t, svc.KnowledgeQAByEvent(ctx, cm, types.Pipline["rag"]))
	require.Empty(t, cm.SearchResult)
	require.Zero(t, model.calls())
	// Existing legacy source refusal is preserved; an ordinary admitted receipt
	// exercises the same real QA authorization, prompt and model path below.
	cm.MergeResult = []*types.SearchResult{{ID: "ordinary", KnowledgeBaseID: "kb", KnowledgeID: "ordinary-doc", Content: "Synthetic ordinary document", SourceLineage: &types.SourceLineage{Version: types.SourceLineageVersion, State: types.SourceLineageComplete}}}
	require.NoError(t, svc.KnowledgeQAByEvent(ctx, cm, []types.EventType{types.INTO_CHAT_MESSAGE, types.CHAT_COMPLETION}))
	require.NotEmpty(t, cm.ChatResponse.Content)
	require.Positive(t, model.calls())
}

type imScopeShares struct {
	*fakeKBShareService
	shared *types.KnowledgeBase
}

func (f imScopeShares) ListSharedKnowledgeBases(context.Context, uint64, types.TenantRole) ([]*types.SharedKnowledgeBaseInfo, error) {
	if !f.allowedKBs[f.shared.ID] {
		return nil, nil
	}
	return []*types.SharedKnowledgeBaseInfo{{KnowledgeBase: f.shared}}, nil
}
func TestIMAllPreservesOnlyLiveShares(t *testing.T) {
	ctx := context.WithValue(context.Background(), types.TenantIDContextKey, uint64(10000))
	ctx = context.WithValue(ctx, types.UserIDContextKey, "system-10000")
	shared := &types.KnowledgeBase{ID: "shared", TenantID: 10001}
	unshared := &types.KnowledgeBase{ID: "unshared", TenantID: 10002}
	shares := imScopeShares{&fakeKBShareService{allowedKBs: map[string]bool{"shared": true}}, shared}
	svc := &sessionService{kbShareService: shares, knowledgeBaseService: imScopeKBs{kbs: []*types.KnowledgeBase{{ID: "own", TenantID: 10000}, shared, unshared}}}
	agent := &types.CustomAgent{ID: "agent", TenantID: 10000, Config: types.CustomAgentConfig{KBSelectionMode: "all"}}
	ids, err := svc.ResolveIMChannelKnowledgeBases(ctx, agent)
	require.NoError(t, err)
	require.Equal(t, map[string]uint64{"own": 10000, "shared": 10001}, ids)
	require.Error(t, svc.CheckIMChannelKnowledgeBase(ctx, 10002, "unshared"))
	shares.allowedKBs["shared"] = false
	require.Error(t, svc.CheckIMChannelKnowledgeBase(ctx, 10001, "shared"))
	ids, err = svc.ResolveIMChannelKnowledgeBases(ctx, agent)
	require.NoError(t, err)
	require.Equal(t, map[string]uint64{"own": 10000}, ids)
}
