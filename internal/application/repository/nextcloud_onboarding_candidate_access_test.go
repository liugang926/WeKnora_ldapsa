package repository

import (
	"context"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/stretchr/testify/require"
)

type onboardingCandidateAccessGroups struct {
	interfaces.GroupAccessService
	allowed       map[string]bool
	seenPrincipal types.Principal
	seenCaller    types.Caller
	seenTenantID  uint64
	seenType      types.ResourceType
	seenAction    types.ResourceAction
}

func (g *onboardingCandidateAccessGroups) EffectivePermission(
	ctx context.Context,
	tenantID uint64,
	resourceType types.ResourceType,
	resourceID string,
	action types.ResourceAction,
	_ time.Time,
) (types.EffectiveResourcePermission, error) {
	g.seenPrincipal, _ = types.PrincipalFromContext(ctx)
	g.seenCaller = types.CallerFromContext(ctx)
	g.seenTenantID = tenantID
	g.seenType = resourceType
	g.seenAction = action
	return types.EffectiveResourcePermission{
		Allowed: g.allowed[resourceID],
		Mode:    types.ResourceAccessRestricted,
		Action:  action,
	}, nil
}

func TestAccessibleCandidateInventoryUsesActorResourceGrants(t *testing.T) {
	groups := &onboardingCandidateAccessGroups{allowed: map[string]bool{
		"visible-kb": true,
		"restricted": false,
	}}
	store := &NextcloudOnboardingStore{groups: groups}
	ctx := nextcloudCandidateContext(context.Background(), 10002, "T0001919")
	inventory := []types.KnowledgeBase{
		{ID: "visible-kb", Name: "真实知识库"},
		{ID: "restricted", Name: "不可见测试知识库"},
	}

	visible, err := store.accessibleCandidateInventory(ctx, 10002, inventory)
	require.NoError(t, err)
	require.Len(t, visible, 1)
	require.Equal(t, "visible-kb", visible[0].ID)
	require.Equal(t, types.PrincipalWebUser, groups.seenPrincipal.Type)
	require.Equal(t, "T0001919", groups.seenPrincipal.ID)
	require.Equal(t, uint64(10002), groups.seenCaller.TenantID)
	require.Equal(t, "T0001919", groups.seenCaller.UserID)
	require.Equal(t, uint64(10002), groups.seenTenantID)
	require.Equal(t, types.GroupResourceTypeKnowledgeBase, groups.seenType)
	require.Equal(t, types.ResourceActionRead, groups.seenAction)
}
