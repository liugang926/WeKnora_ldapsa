package repository

import (
	"context"
	"testing"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
)

func TestIMActorRequiresLiveExactScope(t *testing.T) {
	ctx := types.WithCaller(context.Background(), types.Caller{
		TenantID: 7, UserID: "system-7", Role: types.TenantRoleViewer,
	})
	ctx = types.WithPrincipal(ctx, types.Principal{Type: types.PrincipalIMUser, ID: "7:channel:dingtalk:sender"})
	ctx = types.WithIMChannelScope(ctx, types.IMChannelScope{
		TenantID: 7, ChannelID: "channel", OwnerID: "owner", AgentID: "agent",
		Verify: func(context.Context, uint64, types.ResourceType, string, types.ResourceAction) error {
			return nil
		},
	})
	actor, err := captureKnowledgeActorSelector(ctx, "")
	require.NoError(t, err)
	store := &KnowledgeInputProvenanceStore{}
	_, err = store.restoreCurrentKnowledgeActor(context.Background(), actor, 7)
	require.Error(t, err)
	for _, change := range []func(*knowledgeBuildActorSelector){
		func(a *knowledgeBuildActorSelector) { a.UserID = "other" },
		func(a *knowledgeBuildActorSelector) { a.CallerTenantID = 8 },
		func(a *knowledgeBuildActorSelector) { a.PrincipalID = "other" },
	} {
		bad := actor
		change(&bad)
		_, err = store.restoreCurrentKnowledgeActor(ctx, bad, 7)
		require.Error(t, err)
	}
	ctx = context.WithValue(ctx, types.SystemAdminContextKey, true)
	fresh, err := store.restoreCurrentKnowledgeActor(ctx, actor, 7)
	require.NoError(t, err)
	require.Equal(t, false, fresh.Value(types.SystemAdminContextKey))
	_, err = store.authorizeKnowledgeActorWrite(fresh, &types.KnowledgeBase{ID: "kb", TenantID: 7})
	require.Error(t, err)
	_, err = captureKnowledgeActorSelector(context.WithValue(ctx, types.IMChannelScopeContextKey, nil), "")
	require.Error(t, err)
}
