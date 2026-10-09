package service

import (
	"context"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/stretchr/testify/require"
)

func TestRevalidateAgentExecutionChecksAgentAndKnowledgeBases(t *testing.T) {
	access := &tagTargetGroupAccessService{allowed: map[string]bool{
		"agent-1": true,
		"kb-1":    false,
	}}
	svc := &sessionService{groupAccess: access}
	agent := &types.CustomAgent{ID: "agent-1", TenantID: 7}
	targets := types.SearchTargets{{
		Type:            types.SearchTargetTypeKnowledgeBase,
		KnowledgeBaseID: "kb-1",
		TenantID:        7,
	}}

	err := svc.revalidateAgentExecution(context.Background(), agent, targets)

	require.ErrorIs(t, err, ErrResourceAccessDenied)
	require.Equal(t, []string{"agent-1", "kb-1"}, access.calls)
}

func TestRevalidateAgentExecutionStopsWhenAgentGrantRevoked(t *testing.T) {
	access := &tagTargetGroupAccessService{allowed: map[string]bool{"kb-1": true}}
	svc := &sessionService{groupAccess: access}

	err := svc.revalidateAgentExecution(
		context.Background(),
		&types.CustomAgent{ID: "agent-1", TenantID: 7},
		types.SearchTargets{{KnowledgeBaseID: "kb-1", TenantID: 7}},
	)

	require.ErrorIs(t, err, ErrResourceAccessDenied)
	require.Equal(t, []string{"agent-1"}, access.calls)
}

type viewerAgentUseRepo struct {
	interfaces.GroupAccessRepository
}

func (viewerAgentUseRepo) GetResourceAccessPolicy(
	context.Context, uint64, types.ResourceType, string,
) (*types.ResourceAccessPolicy, error) {
	return &types.ResourceAccessPolicy{Mode: types.ResourceAccessRestricted}, nil
}

func (viewerAgentUseRepo) GetDirectTenantRole(context.Context, string, uint64) (*types.TenantRole, error) {
	role := types.TenantRoleViewer
	return &role, nil
}

func (viewerAgentUseRepo) ListGroupRoleMatches(context.Context, string, uint64) ([]types.GroupRoleMatch, error) {
	return nil, nil
}

func (viewerAgentUseRepo) ListResourceGroupMatches(
	_ context.Context, _ string, _ uint64, resourceType types.ResourceType, _ string,
) ([]types.ResourceGroupMatch, error) {
	if resourceType != types.GroupResourceTypeAgent {
		return nil, nil
	}
	now := time.Now().UTC()
	return []types.ResourceGroupMatch{{
		Permission: types.ResourcePermissionUse, DirectoryEnabled: true, LastSuccessfulSyncAt: &now,
	}}, nil
}

func TestRestrictedAgentGroupUseAllowsViewerWithoutGrantingKnowledgeBase(t *testing.T) {
	svc := &sessionService{groupAccess: NewGroupAccessService(viewerAgentUseRepo{})}
	ctx := types.WithPrincipal(context.Background(), types.Principal{Type: types.PrincipalWebUser, ID: "viewer"})
	ctx = types.WithCaller(ctx, types.Caller{TenantID: 7, UserID: "viewer", Role: types.TenantRoleViewer})
	agent := &types.CustomAgent{ID: "agent", TenantID: 7}
	require.NoError(t, svc.revalidateAgentExecution(ctx, agent, nil))
	err := svc.revalidateAgentExecution(ctx, agent, types.SearchTargets{{
		Type: types.SearchTargetTypeKnowledgeBase, KnowledgeBaseID: "kb", TenantID: 7,
	}})
	require.ErrorIs(t, err, ErrResourceAccessDenied)
}
