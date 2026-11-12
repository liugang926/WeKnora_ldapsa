package service

import (
	"context"
	"testing"

	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
)

// readContractMessageInput exercises the admitted material-consumer path used
// by history pages and derived-input readers. Ordinary GetMessage is an
// owner-scoped projection and intentionally does not register such a guard.
func readContractMessageInput(
	ctx context.Context,
	messages *messageService,
	sessionID, messageID string,
) (*types.Message, error) {
	tenant, ok := sessionTenantIDForLookup(ctx)
	if !ok {
		return nil, access.ErrNextcloudPublicationDenied
	}
	rows, err := messages.admittedMessages(ctx, types.MessageMaterialReadRequest{
		TenantID: tenant, SessionID: sessionID, MessageIDs: []string{messageID},
	})
	if err != nil {
		return nil, err
	}
	if len(rows) != 1 {
		return nil, access.ErrNextcloudPublicationDenied
	}
	return rows[0], nil
}

func TestMessageContractOwnerLookupAndMaterialAdmission(t *testing.T) {
	messages, _, _, _, ctx, session, _ := messageBodyIntegrationFixture(t, false)
	message, err := messages.CreateMessage(ctx, &types.Message{
		SessionID: session.ID, RequestID: "owner-material-contract", Role: "user",
		Content: "owner-only original question", IsCompleted: true,
	})
	require.NoError(t, err)
	consumer := WithMessageMaterialOutputBoundary(ctx)
	defer CloseMessageMaterialOutputBoundary(consumer)
	row, err := messages.GetMessage(consumer, session.ID, message.ID)
	require.NoError(t, err)
	require.Equal(t, message.Content, row.Content)
	slot := consumer.Value(messageMaterialOutputKey{}).(*messageMaterialOutputSlot)
	require.Empty(t, slot.Guards, "ordinary owner lookup is not a material-consumer admission")
	row, err = readContractMessageInput(consumer, messages, session.ID, message.ID)
	require.NoError(t, err)
	require.Equal(t, message.Content, row.Content)
	require.Len(t, slot.Guards, 1)
	require.NotEmpty(t, slot.Guards[0].bodyLeases, "explicit consumer retains original body bytes")
	require.NoError(t, slot.Guards[0].Recheck())

	for _, tc := range []struct {
		name   string
		tenant uint64
		user   string
	}{
		{name: "other session owner", tenant: 7, user: "another-session-owner"},
		{name: "other tenant", tenant: 8, user: types.CallerFromContext(ctx).UserID},
	} {
		t.Run(tc.name, func(t *testing.T) {
			other := types.WithCaller(context.Background(), types.Caller{
				TenantID: tc.tenant, UserID: tc.user, Role: types.TenantRoleViewer,
			})
			other = types.WithExecutionTenant(other, tc.tenant)
			other = context.WithValue(other, types.UserIDContextKey, tc.user)
			other = types.WithPrincipal(other, types.Principal{Type: types.PrincipalWebUser, ID: tc.user})
			denied, err := messages.GetMessage(other, session.ID, message.ID)
			require.Error(t, err)
			require.Nil(t, denied)
			denied, err = readContractMessageInput(other, messages, session.ID, message.ID)
			require.Error(t, err)
			require.Nil(t, denied)
		})
	}
}
