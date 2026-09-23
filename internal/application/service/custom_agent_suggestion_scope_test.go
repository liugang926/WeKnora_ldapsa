package service

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type suggestionTagRepo struct {
	interfaces.KnowledgeTagRepository
	tagsByTenant map[uint64][]*types.KnowledgeTag
}

func (r *suggestionTagRepo) GetByIDs(_ context.Context, tenantID uint64, ids []string) ([]*types.KnowledgeTag, error) {
	wanted := make(map[string]bool, len(ids))
	for _, id := range ids {
		wanted[id] = true
	}
	var result []*types.KnowledgeTag
	for _, tag := range r.tagsByTenant[tenantID] {
		if tag != nil && wanted[tag.ID] {
			result = append(result, tag)
		}
	}
	return result, nil
}

type suggestionKnowledgeRepo struct {
	interfaces.KnowledgeRepository
	idsByTenantAndKB map[uint64]map[string][]string
	byID             map[string]*types.Knowledge
}

func (r *suggestionKnowledgeRepo) ListIDsByTagIDs(
	_ context.Context,
	tenantID uint64,
	kbID string,
	_ []string,
) ([]string, error) {
	return append([]string(nil), r.idsByTenantAndKB[tenantID][kbID]...), nil
}

func (r *suggestionKnowledgeRepo) GetKnowledgeByIDOnly(_ context.Context, id string) (*types.Knowledge, error) {
	if knowledge := r.byID[id]; knowledge != nil {
		return knowledge, nil
	}
	return nil, errors.New("not found")
}

type suggestionKBService struct {
	interfaces.KnowledgeBaseService
	kbs map[string]*types.KnowledgeBase
}

func (s *suggestionKBService) GetKnowledgeBasesByIDsOnly(
	_ context.Context,
	ids []string,
) ([]*types.KnowledgeBase, error) {
	result := make([]*types.KnowledgeBase, 0, len(ids))
	for _, id := range ids {
		if kb := s.kbs[id]; kb != nil {
			result = append(result, kb)
		}
	}
	return result, nil
}

type suggestionKBShareService struct {
	interfaces.KBShareService
	allowed map[string]bool
}

type suggestionGroupAccess struct {
	interfaces.GroupAccessService
	allowed map[string]bool
	calls   []string
}

func (s *suggestionGroupAccess) EffectivePermission(
	_ context.Context,
	_ uint64,
	resourceType types.ResourceType,
	resourceID string,
	action types.ResourceAction,
	_ time.Time,
) (types.EffectiveResourcePermission, error) {
	s.calls = append(s.calls, string(resourceType)+":"+resourceID+":"+string(action))
	return types.EffectiveResourcePermission{Allowed: s.allowed[resourceID]}, nil
}

func (s *suggestionKBShareService) CheckTenantKBPermission(
	_ context.Context,
	kbID string,
	_ uint64,
	_ types.TenantRole,
) (types.OrgMemberRole, bool, error) {
	return types.OrgRoleViewer, s.allowed[kbID], nil
}

func TestResolveSuggestionTagScopes_UsesSourceTenantForSharedKB(t *testing.T) {
	const (
		callerTenant = uint64(1)
		sourceTenant = uint64(2)
		kbID         = "shared-kb"
		tagID        = "shared-tag"
	)
	svc := &customAgentService{
		tagRepo: &suggestionTagRepo{tagsByTenant: map[uint64][]*types.KnowledgeTag{
			sourceTenant: {{ID: tagID, TenantID: sourceTenant, KnowledgeBaseID: kbID}},
		}},
		knowledgeRepo: &suggestionKnowledgeRepo{idsByTenantAndKB: map[uint64]map[string][]string{
			sourceTenant: {kbID: {"doc-in-tag"}},
		}},
		kbService: &suggestionKBService{kbs: map[string]*types.KnowledgeBase{
			kbID: {ID: kbID, TenantID: sourceTenant},
		}},
		kbShareService: &suggestionKBShareService{allowed: map[string]bool{kbID: true}},
	}

	resolved, err := svc.resolveSuggestionTagScopes(
		context.WithValue(context.Background(), types.TenantIDContextKey, callerTenant),
		[]types.TagScope{{KnowledgeBaseID: kbID, TagIDs: []string{tagID}}},
	)
	require.NoError(t, err)
	assert.Equal(t, []string{kbID}, resolved.KnowledgeBaseIDs)
	assert.Equal(t, []string{"doc-in-tag"}, resolved.KnowledgeIDs)
	assert.Equal(t, []string{tagID}, resolved.TagIDsByTenant[sourceTenant])
	assert.Empty(t, resolved.TagIDsByTenant[callerTenant])
}

func TestSuggestionScopesApplyKnowledgeBaseGroupPolicy(t *testing.T) {
	access := &suggestionGroupAccess{allowed: map[string]bool{"allowed": true}}
	svc := &customAgentService{
		kbService: &suggestionKBService{kbs: map[string]*types.KnowledgeBase{
			"allowed": {ID: "allowed", TenantID: 1},
			"denied":  {ID: "denied", TenantID: 1},
		}},
		groupAccess: access,
	}
	ctx := context.WithValue(context.Background(), types.TenantIDContextKey, uint64(1))

	grouped := svc.groupKBIDsByEffectiveTenant(ctx, []string{"allowed", "denied"})
	require.Equal(t, []string{"allowed"}, grouped[1])
	require.ElementsMatch(t, []string{
		"knowledge_base:allowed:read",
		"knowledge_base:denied:read",
	}, access.calls)
}

func TestSuggestionKnowledgeIDsApplyKnowledgeBaseGroupPolicy(t *testing.T) {
	access := &suggestionGroupAccess{allowed: map[string]bool{"kb-allowed": true}}
	svc := &customAgentService{
		knowledgeRepo: &suggestionKnowledgeRepo{byID: map[string]*types.Knowledge{
			"doc-allowed": {ID: "doc-allowed", TenantID: 1, KnowledgeBaseID: "kb-allowed"},
			"doc-denied":  {ID: "doc-denied", TenantID: 1, KnowledgeBaseID: "kb-denied"},
		}},
		groupAccess: access,
	}
	ctx := context.WithValue(context.Background(), types.TenantIDContextKey, uint64(1))

	readable := svc.readableSuggestionKnowledgeIDs(ctx, []string{"doc-allowed", "doc-denied"})
	require.Equal(t, []string{"doc-allowed"}, readable)
}

func TestMergeHybridStarterSuggestions_ReservesKnowledgeSlots(t *testing.T) {
	curated := []types.SuggestedQuestion{
		{Question: "curated 1", Source: "agent_config"},
		{Question: "curated 2", Source: "agent_config"},
		{Question: "curated 3", Source: "agent_config"},
		{Question: "curated 4", Source: "agent_config"},
		{Question: "curated 5", Source: "agent_config"},
		{Question: "curated 6", Source: "agent_config"},
	}
	knowledge := []types.SuggestedQuestion{
		{Question: "knowledge 1", Source: "document"},
		{Question: "knowledge 2", Source: "faq"},
		{Question: "knowledge 3", Source: "document"},
	}

	got := mergeHybridStarterSuggestions(curated, knowledge, 6)
	require.Len(t, got, 6)
	assert.Equal(t, []string{
		"curated 1", "curated 2", "curated 3", "curated 4", "knowledge 1", "knowledge 2",
	}, []string{got[0].Question, got[1].Question, got[2].Question, got[3].Question, got[4].Question, got[5].Question})
}

func TestMergeHybridStarterSuggestions_BackfillsWhenKnowledgeIsEmpty(t *testing.T) {
	curated := []types.SuggestedQuestion{
		{Question: "curated 1"}, {Question: "curated 2"}, {Question: "curated 3"},
	}
	got := mergeHybridStarterSuggestions(curated, nil, 3)
	require.Len(t, got, 3)
	assert.Equal(t, []string{"curated 1", "curated 2", "curated 3"}, []string{
		got[0].Question, got[1].Question, got[2].Question,
	})
}

func TestExcludeSuggestionStrings_TagScopeOverridesSameKnowledgeBase(t *testing.T) {
	got := excludeSuggestionStrings([]string{"kb-with-tag", "kb-explicit"}, []string{"kb-with-tag"})
	assert.Equal(t, []string{"kb-explicit"}, got)
}

type suggestionDocRepo struct {
	interfaces.KnowledgeRepository
	docs map[string]*types.Knowledge
}

func (r *suggestionDocRepo) GetKnowledgeByIDOnly(_ context.Context, id string) (*types.Knowledge, error) {
	if doc, ok := r.docs[id]; ok {
		return doc, nil
	}
	return nil, errors.New("not found")
}

// Document IDs in a suggestion scope reach a tenant's chunks directly, so each
// must be readable by the caller, as KB IDs already are: a shared agent's
// message snapshot records them before the agent's scope is applied.
func TestReadableSuggestionKnowledgeIDs(t *testing.T) {
	svc := &customAgentService{knowledgeRepo: &suggestionDocRepo{docs: map[string]*types.Knowledge{
		"own-doc":      {ID: "own-doc", TenantID: 7, KnowledgeBaseID: "own-kb"},
		"scoped-doc":   {ID: "scoped-doc", TenantID: 84, KnowledgeBaseID: "agent-kb"},
		"unscoped-doc": {ID: "unscoped-doc", TenantID: 84, KnowledgeBaseID: "other-kb"},
	}}}
	caller := types.WithCaller(context.Background(),
		types.Caller{TenantID: 7, UserID: "u", Role: types.TenantRoleAdmin})
	ids := []string{"own-doc", "scoped-doc", "unscoped-doc", "missing-doc"}

	assert.Equal(t, []string{"own-doc"}, svc.readableSuggestionKnowledgeIDs(caller, ids))

	sharedRun := access.WithSharedAgent(caller, &types.CustomAgent{
		ID: "agent", TenantID: 84,
		Config: types.CustomAgentConfig{KBSelectionMode: "selected", KnowledgeBases: []string{"agent-kb"}},
	})
	assert.Equal(t, []string{"own-doc", "scoped-doc"}, svc.readableSuggestionKnowledgeIDs(sharedRun, ids))

	// Follow-up generation moves execution into the agent's workspace; with
	// the caller pinned, its documents still need the agent's grant.
	followUp := types.WithExecutionTenant(sharedRun, 84)
	assert.Equal(t, []string{"own-doc", "scoped-doc"}, svc.readableSuggestionKnowledgeIDs(followUp, ids))
	uncaptured := context.WithValue(context.Background(), types.TenantIDContextKey, uint64(7))
	assert.Equal(t, []string{"own-doc"},
		svc.readableSuggestionKnowledgeIDs(types.WithExecutionTenant(uncaptured, 84), ids),
		"execution in the agent's workspace does not make its documents the caller's")
}
