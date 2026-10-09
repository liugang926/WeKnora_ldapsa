package types

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTaskAuthorizationDoesNotPromoteMachineOrAmbientUser(t *testing.T) {
	ambient := context.WithValue(context.Background(), UserIDContextKey, "user-1")
	ambient = WithPrincipal(ambient, Principal{Type: PrincipalWebUser, ID: "user-1"})
	for _, initiator := range []TaskInitiator{
		{}, {UserID: "user-1", APIKeyID: 3}, {UserID: "system-7"},
	} {
		ctx := WithTaskAuthorization(ambient, 7, initiator)
		principal, ok := PrincipalFromContext(ctx)
		require.True(t, ok)
		require.NotEqual(t, PrincipalWebUser, principal.Type)
		require.Empty(t, TaskInitiatorFromContext(ctx).UserID)
	}
	ctx := WithTaskAuthorization(context.Background(), 7, TaskInitiator{UserID: "user-1"})
	principal, _ := PrincipalFromContext(ctx)
	require.Equal(t, Principal{Type: PrincipalWebUser, ID: "user-1"}, principal)
	caller := CallerFromContext(ctx)
	require.Equal(t, uint64(7), caller.TenantID)
}
