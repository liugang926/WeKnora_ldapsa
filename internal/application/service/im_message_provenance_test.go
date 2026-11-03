package service

import (
	"context"
	"errors"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestIMMessageProvenanceRealPersistence(t *testing.T) {
	messages, _, store, db, ctx, session := messageMaterialActualFixture(t)
	ctx = types.WithCaller(ctx, types.Caller{TenantID: 7, UserID: "system-7", Role: types.TenantRoleViewer})
	ctx = types.WithPrincipal(ctx, types.Principal{Type: types.PrincipalIMUser, ID: "7:channel:dingtalk:sender"})
	valid := true
	ctx = types.WithIMChannelScope(ctx, types.IMChannelScope{TenantID: 7, ChannelID: "channel", AgentID: "agent", OwnerID: "im_session:test", KnowledgeBases: map[string]uint64{"kb": 7}, Verify: func(context.Context, uint64, types.ResourceType, string, types.ResourceAction) error {
		if !valid {
			return errors.New("revoked")
		}
		return nil
	}})
	require.NoError(t, db.Model(session).Update("user_id", "im_session:test").Error)
	user, err := messages.CreateMessage(ctx, &types.Message{SessionID: session.ID, Role: "user", RequestID: "im-request", Content: "test question", IsCompleted: true, Channel: "im"})
	require.NoError(t, err)
	require.NotEmpty(t, user.ID)
	answer, err := messages.CreateMessage(ctx, &types.Message{SessionID: session.ID, Role: "assistant", RequestID: "im-request", Channel: "im"})
	require.NoError(t, err)
	answer.Content = "test answer"
	answer.IsCompleted = true
	answer.SourceLineage, _ = types.NewCompleteSourceLineage()
	require.NoError(t, messages.UpdateMessage(ctx, answer))
	_, err = store.PeekMessageMaterialReadPlan(ctx, types.MessageMaterialReadRequest{TenantID: 7, SessionID: session.ID, MessageIDs: []string{user.ID, answer.ID}, IncludeOriginalUserFields: true})
	require.NoError(t, err)
	valid = false
	_, err = store.RefreshCurrentReadActor(ctx)
	require.Error(t, err)
	noScope := context.WithValue(ctx, types.IMChannelScopeContextKey, nil)
	_, err = store.RefreshCurrentReadActor(noScope)
	require.Error(t, err)
}
