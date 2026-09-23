package handler

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

type organizationGroupAccessStub struct {
	interfaces.GroupAccessService
	allowed map[string]bool
	fail    map[string]error
	calls   []organizationGroupAccessCall
}

type organizationGroupAccessCall struct {
	tenantID     uint64
	resourceType types.ResourceType
	resourceID   string
	action       types.ResourceAction
}

func (s *organizationGroupAccessStub) EffectivePermission(
	_ context.Context,
	tenantID uint64,
	resourceType types.ResourceType,
	resourceID string,
	action types.ResourceAction,
	_ time.Time,
) (types.EffectiveResourcePermission, error) {
	s.calls = append(s.calls, organizationGroupAccessCall{
		tenantID: tenantID, resourceType: resourceType, resourceID: resourceID, action: action,
	})
	if err := s.fail[resourceID]; err != nil {
		return types.EffectiveResourcePermission{}, err
	}
	return types.EffectiveResourcePermission{Allowed: s.allowed[resourceID]}, nil
}

type organizationGroupOrgStub struct{ interfaces.OrganizationService }

func (organizationGroupOrgStub) GetTenantMember(
	context.Context, string, uint64,
) (*types.OrganizationTenantMember, error) {
	return &types.OrganizationTenantMember{Role: types.OrgRoleViewer}, nil
}

func (organizationGroupOrgStub) GetOrganization(
	context.Context,
	string,
) (*types.Organization, error) {
	return &types.Organization{ID: "org-1", Name: "Organization"}, nil
}

type organizationGroupShareStub struct {
	interfaces.KBShareService
	shares []*types.KnowledgeBaseShare
	shared []*types.SharedKnowledgeBaseInfo
	inOrg  []*types.OrganizationSharedKnowledgeBaseItem
}

func (s organizationGroupShareStub) ListSharesByOrganization(
	context.Context,
	string,
) ([]*types.KnowledgeBaseShare, error) {
	return s.shares, nil
}

func (s organizationGroupShareStub) ListSharedKnowledgeBases(
	context.Context, uint64, types.TenantRole,
) ([]*types.SharedKnowledgeBaseInfo, error) {
	return s.shared, nil
}

func (s organizationGroupShareStub) ListSharedKnowledgeBasesInOrganization(
	context.Context, string, uint64, types.TenantRole,
) ([]*types.OrganizationSharedKnowledgeBaseItem, error) {
	return s.inOrg, nil
}

type organizationGroupAgentShareStub struct {
	interfaces.AgentShareService
	shares []*types.AgentShare
	shared []*types.SharedAgentInfo
	inOrg  []*types.OrganizationSharedAgentItem
}

func (s organizationGroupAgentShareStub) ListSharesByOrganization(
	context.Context,
	string,
) ([]*types.AgentShare, error) {
	return s.shares, nil
}

func (s organizationGroupAgentShareStub) ListSharedAgents(
	context.Context, uint64, types.TenantRole,
) ([]*types.SharedAgentInfo, error) {
	return s.shared, nil
}

func (s organizationGroupAgentShareStub) ListSharedAgentsInOrganization(
	context.Context, string, uint64, types.TenantRole,
) ([]*types.OrganizationSharedAgentItem, error) {
	return s.inOrg, nil
}

type organizationGroupUserStub struct{ interfaces.UserService }

func (organizationGroupUserStub) GetUserByID(context.Context, string) (*types.User, error) {
	return nil, errors.New("not found")
}

type organizationGroupKBStub struct {
	interfaces.KnowledgeBaseService
	byID map[string]*types.KnowledgeBase
}

func (s organizationGroupKBStub) GetKnowledgeBaseByIDOnly(
	_ context.Context,
	id string,
) (*types.KnowledgeBase, error) {
	kb := s.byID[id]
	if kb == nil {
		return nil, errors.New("not found")
	}
	return kb, nil
}

func newOrganizationGroupContext(t *testing.T) (*gin.Context, *httptest.ResponseRecorder) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Params = gin.Params{{Key: "id", Value: "org-1"}}
	ctx := context.WithValue(context.Background(), types.TenantIDContextKey, uint64(7))
	ctx = context.WithValue(ctx, types.TenantRoleContextKey, types.TenantRoleViewer)
	ctx = context.WithValue(ctx, types.UserIDContextKey, "user-7")
	c.Request = httptest.NewRequest(http.MethodGet, "/", nil).WithContext(ctx)
	c.Set(types.TenantIDContextKey.String(), uint64(7))
	c.Set(types.UserIDContextKey.String(), "user-7")
	return c, recorder
}

func TestOrganizationSharedListsApplyGroupResourceAccess(t *testing.T) {
	kb := func(id, name string) *types.KnowledgeBase {
		return &types.KnowledgeBase{ID: id, Name: name, TenantID: 42}
	}
	agent := func(id, name string) *types.CustomAgent {
		return &types.CustomAgent{ID: id, Name: name, TenantID: 42}
	}

	access := &organizationGroupAccessStub{
		allowed: map[string]bool{"kb-allowed": true, "agent-allowed": true},
		fail: map[string]error{
			"kb-error":    errors.New("directory unavailable"),
			"agent-error": errors.New("directory unavailable"),
		},
	}
	shareService := organizationGroupShareStub{
		shares: []*types.KnowledgeBaseShare{
			{
				ID:              "share-allowed",
				KnowledgeBaseID: "kb-allowed",
				SourceTenantID:  42,
				Permission:      types.OrgRoleViewer,
			},
			{
				ID:              "share-denied",
				KnowledgeBaseID: "kb-denied",
				SourceTenantID:  42,
				Permission:      types.OrgRoleViewer,
			},
			{
				ID:              "share-error",
				KnowledgeBaseID: "kb-error",
				SourceTenantID:  42,
				Permission:      types.OrgRoleViewer,
			},
		},
		shared: []*types.SharedKnowledgeBaseInfo{
			{KnowledgeBase: kb("kb-allowed", "Allowed KB"), SourceTenantID: 42},
			{KnowledgeBase: kb("kb-denied", "Denied KB"), SourceTenantID: 42},
			{KnowledgeBase: kb("kb-error", "Error KB"), SourceTenantID: 42},
		},
		inOrg: []*types.OrganizationSharedKnowledgeBaseItem{
			{
				SharedKnowledgeBaseInfo: types.SharedKnowledgeBaseInfo{
					KnowledgeBase:  kb("kb-allowed", "Allowed KB"),
					SourceTenantID: 42,
				},
			},
			{
				SharedKnowledgeBaseInfo: types.SharedKnowledgeBaseInfo{
					KnowledgeBase:  kb("kb-denied", "Denied KB"),
					SourceTenantID: 42,
				},
			},
			{
				SharedKnowledgeBaseInfo: types.SharedKnowledgeBaseInfo{
					KnowledgeBase:  kb("kb-error", "Error KB"),
					SourceTenantID: 42,
				},
			},
		},
	}
	agentShareService := organizationGroupAgentShareStub{
		shares: []*types.AgentShare{
			{
				ID:             "agent-share-allowed",
				AgentID:        "agent-allowed",
				SourceTenantID: 42,
				Agent:          agent("agent-allowed", "Allowed Agent"),
			},
			{
				ID:             "agent-share-denied",
				AgentID:        "agent-denied",
				SourceTenantID: 42,
				Agent:          agent("agent-denied", "Denied Agent"),
			},
			{
				ID:             "agent-share-error",
				AgentID:        "agent-error",
				SourceTenantID: 42,
				Agent:          agent("agent-error", "Error Agent"),
			},
		},
		shared: []*types.SharedAgentInfo{
			{Agent: agent("agent-allowed", "Allowed Agent"), SourceTenantID: 42},
			{Agent: agent("agent-denied", "Denied Agent"), SourceTenantID: 42},
			{Agent: agent("agent-error", "Error Agent"), SourceTenantID: 42},
		},
		inOrg: []*types.OrganizationSharedAgentItem{
			{
				SharedAgentInfo: types.SharedAgentInfo{
					Agent:          agent("agent-allowed", "Allowed Agent"),
					SourceTenantID: 42,
				},
			},
			{
				SharedAgentInfo: types.SharedAgentInfo{
					Agent:          agent("agent-denied", "Denied Agent"),
					SourceTenantID: 42,
				},
			},
			{
				SharedAgentInfo: types.SharedAgentInfo{
					Agent:          agent("agent-error", "Error Agent"),
					SourceTenantID: 42,
				},
			},
		},
	}
	h := &OrganizationHandler{
		orgService: organizationGroupOrgStub{}, shareService: shareService, agentShareService: agentShareService,
		userService: organizationGroupUserStub{},
	}
	ConfigureOrganizationGroupAccess(h, access)

	tests := []struct {
		name    string
		invoke  func(*gin.Context)
		allowed string
		denied  []string
	}{
		{
			name:    "ListOrgShares",
			invoke:  h.ListOrgShares,
			allowed: "share-allowed",
			denied:  []string{"share-denied", "share-error"},
		},
		{
			name:    "ListSharedKnowledgeBases",
			invoke:  h.ListSharedKnowledgeBases,
			allowed: "Allowed KB",
			denied:  []string{"Denied KB", "Error KB"},
		},
		{
			name:    "ListOrganizationSharedKnowledgeBases",
			invoke:  h.ListOrganizationSharedKnowledgeBases,
			allowed: "Allowed KB",
			denied:  []string{"Denied KB", "Error KB"},
		},
		{
			name:    "ListOrgAgentShares",
			invoke:  h.ListOrgAgentShares,
			allowed: "Allowed Agent",
			denied:  []string{"Denied Agent", "Error Agent"},
		},
		{
			name:    "ListSharedAgents",
			invoke:  h.ListSharedAgents,
			allowed: "Allowed Agent",
			denied:  []string{"Denied Agent", "Error Agent"},
		},
		{
			name:    "ListOrganizationSharedAgents",
			invoke:  h.ListOrganizationSharedAgents,
			allowed: "Allowed Agent",
			denied:  []string{"Denied Agent", "Error Agent"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c, recorder := newOrganizationGroupContext(t)
			test.invoke(c)
			require.Empty(t, c.Errors)
			require.Contains(t, recorder.Body.String(), test.allowed)
			for _, denied := range test.denied {
				require.NotContains(t, recorder.Body.String(), denied)
			}
		})
	}

	for _, call := range access.calls {
		require.Equal(t, uint64(42), call.tenantID)
		switch call.resourceType {
		case types.GroupResourceTypeKnowledgeBase:
			require.Equal(t, types.ResourceActionRead, call.action)
		case types.GroupResourceTypeAgent:
			require.Equal(t, types.ResourceActionUse, call.action)
		default:
			t.Fatalf("unexpected resource type %q", call.resourceType)
		}
	}
}

func TestOrganizationAgentCarriedKnowledgeBasesRequireAgentAndKBAccess(t *testing.T) {
	agent := func(id, kbID string) *types.OrganizationSharedAgentItem {
		return &types.OrganizationSharedAgentItem{SharedAgentInfo: types.SharedAgentInfo{
			Agent: &types.CustomAgent{
				ID: id, Name: id, TenantID: 42,
				Config: types.CustomAgentConfig{
					KBSelectionMode: "selected",
					KnowledgeBases:  []string{kbID},
				},
			},
			SourceTenantID: 42,
		}}
	}
	access := &organizationGroupAccessStub{allowed: map[string]bool{
		"agent-allowed": true,
		"agent-denied":  false,
		"kb-allowed":    true,
		"kb-denied":     false,
	}}
	h := &OrganizationHandler{
		orgService:   organizationGroupOrgStub{},
		shareService: organizationGroupShareStub{},
		agentShareService: organizationGroupAgentShareStub{
			inOrg: []*types.OrganizationSharedAgentItem{
				agent("agent-allowed", "kb-allowed"),
				agent("agent-denied", "kb-behind-denied-agent"),
				agent("agent-allowed", "kb-denied"),
			},
		},
		kbService: organizationGroupKBStub{byID: map[string]*types.KnowledgeBase{
			"kb-allowed": {ID: "kb-allowed", Name: "Visible Carried KB", TenantID: 42},
			"kb-behind-denied-agent": {
				ID:       "kb-behind-denied-agent",
				Name:     "Denied Agent KB",
				TenantID: 42,
			},
			"kb-denied": {ID: "kb-denied", Name: "Denied KB", TenantID: 42},
		}},
	}
	ConfigureOrganizationGroupAccess(h, access)

	c, recorder := newOrganizationGroupContext(t)
	h.ListOrganizationSharedKnowledgeBases(c)
	require.Empty(t, c.Errors)
	require.Contains(t, recorder.Body.String(), "Visible Carried KB")
	require.NotContains(t, recorder.Body.String(), "Denied Agent KB")
	require.NotContains(t, recorder.Body.String(), "Denied KB")
}
