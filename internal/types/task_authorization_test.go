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
		require.Zero(t, CallerFromContext(ctx).TenantID)
	}
	ctx := WithTaskAuthorization(context.Background(), 7, TaskInitiator{UserID: "user-1"})
	principal, _ := PrincipalFromContext(ctx)
	require.Equal(t, Principal{Type: PrincipalWebUser, ID: "user-1"}, principal)
	caller := CallerFromContext(ctx)
	require.Equal(t, uint64(7), caller.TenantID)
}

func TestTaskAuthorizationPreservesAdmittedWorkspaceAcrossSharedExecution(t *testing.T) {
	ctx := context.WithValue(context.Background(), UserIDContextKey, "user-1")
	ctx = context.WithValue(ctx, TenantRoleContextKey, TenantRoleAdmin)
	ctx = WithCaller(ctx, Caller{TenantID: 7, UserID: "user-1", Role: TenantRoleContributor})
	ctx = WithExecutionTenant(ctx, 8)
	initiator := TaskInitiatorFromContext(ctx)
	require.Equal(t, uint64(7), initiator.CallerTenantID)
	require.Equal(t, TenantRoleContributor, initiator.Role)
	worker := WithTaskAuthorization(context.Background(), 8, initiator)
	require.Equal(t, uint64(7), CallerFromContext(worker).TenantID)
	execution, ok := TenantIDFromContext(worker)
	require.True(t, ok)
	require.Equal(t, uint64(8), execution)
	// Child tasks keep the original caller even after another scope rewrite.
	child := TaskInitiatorFromContext(WithExecutionTenant(worker, 9))
	require.Equal(t, initiator, child)
}
