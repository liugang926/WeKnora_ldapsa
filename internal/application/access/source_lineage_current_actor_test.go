package access

import (
	"context"
	"testing"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
)

type explicitCurrentReadActorFake func(context.Context) (context.Context, error)

func (f explicitCurrentReadActorFake) RefreshCurrentReadActor(ctx context.Context) (context.Context, error) {
	return f(ctx)
}

func TestRetiredLineageDoesNotRefreshOrReplaceCapturedActor(t *testing.T) {
	ctx := types.WithCaller(
		context.Background(),
		types.Caller{TenantID: 7, UserID: "actual-selector", Role: types.TenantRoleViewer},
	)
	ctx = types.WithPrincipal(ctx, types.Principal{Type: types.PrincipalWebUser, ID: "actual-selector"})
	ctx = types.WithExecutionTenant(ctx, 9)
	empty, _ := types.NewCompleteSourceLineage()
	guard := NewNextcloudSourceLineageGuard(nil, nil, nil, nil, nil, nil)
	require.NoError(t, guard.Check(ctx, empty, LineageReadScope{}))
	for _, change := range []string{"caller-tenant", "principal", "execution-tenant"} {
		t.Run(change, func(t *testing.T) {
			calls := 0
			fake := explicitCurrentReadActorFake(func(current context.Context) (context.Context, error) {
				calls++
				switch change {
				case "caller-tenant":
					return types.WithCaller(
						current,
						types.Caller{TenantID: 9, UserID: "actual-selector", Role: types.TenantRoleOwner},
					), nil
				case "principal":
					return types.WithPrincipal(
						current,
						types.Principal{Type: types.PrincipalWebUser, ID: "resource-owner"},
					), nil
				default:
					return types.WithExecutionTenant(current, 7), nil
				}
			})
			guard := NewNextcloudSourceLineageGuard(nil, nil, nil, nil, nil, nil, fake)
			beforeCaller := types.CallerFromContext(ctx)
			beforePrincipal, ok := types.PrincipalFromContext(ctx)
			require.True(t, ok)
			beforeTenant, ok := types.TenantIDFromContext(ctx)
			require.True(t, ok)
			assertRetiredReadUnchanged(t, empty, func() error { return guard.Check(ctx, empty, LineageReadScope{}) })
			require.Zero(t, calls)
			require.Equal(t, beforeCaller, types.CallerFromContext(ctx))
			afterPrincipal, ok := types.PrincipalFromContext(ctx)
			require.True(t, ok)
			require.Equal(t, beforePrincipal, afterPrincipal)
			afterTenant, ok := types.TenantIDFromContext(ctx)
			require.True(t, ok)
			require.Equal(t, beforeTenant, afterTenant)
		})
	}
}
