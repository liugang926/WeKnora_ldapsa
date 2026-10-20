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

func TestSourceLineageCurrentActorCapabilityMandatoryAndOriginalSelectorBound(t *testing.T) {
	ctx := types.WithCaller(
		context.Background(),
		types.Caller{TenantID: 7, UserID: "actual-selector", Role: types.TenantRoleViewer},
	)
	ctx = types.WithPrincipal(ctx, types.Principal{Type: types.PrincipalWebUser, ID: "actual-selector"})
	ctx = types.WithExecutionTenant(ctx, 9)
	empty, _ := types.NewCompleteSourceLineage()
	guard := NewNextcloudSourceLineageGuard(nil, nil, nil, nil, nil, nil)
	require.ErrorIs(t, guard.Check(ctx, empty, LineageReadScope{}), ErrNextcloudPublicationUnavailable)
	for _, change := range []string{"caller-tenant", "principal", "execution-tenant"} {
		t.Run(change, func(t *testing.T) {
			fake := explicitCurrentReadActorFake(func(current context.Context) (context.Context, error) {
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
			require.ErrorIs(t, guard.Check(ctx, empty, LineageReadScope{}), ErrNextcloudPublicationDenied)
		})
	}
}
