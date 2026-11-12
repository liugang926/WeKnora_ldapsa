package access

import (
	"context"

	"github.com/Tencent/WeKnora/internal/types"
)

// CheckWorkspaceRead reloads the WeKnora caller and its access to the named
// workspace targets before an HTTP handler publishes a result.
func (g *NextcloudSourceLineageGuard) CheckWorkspaceRead(ctx context.Context, scope LineageReadScope) error {
	if g == nil || g.groups == nil || ctx == nil {
		return ErrNextcloudPublicationUnavailable
	}
	// API-key middleware supplies a compatibility user for legacy handlers.
	// SQL restores a machine principal from its tenant and key without that user.
	principal, _ := types.PrincipalFromContext(ctx)
	if principal.Type == types.PrincipalAPITenant || principal.Type == types.PrincipalAPIExternalUser {
		caller, ok := ctx.Value(types.CallerContextKey).(types.Caller)
		if !ok {
			return ErrNextcloudPublicationDenied
		}
		caller.UserID = ""
		ctx = types.WithCaller(ctx, caller)
	}
	fresh, err := g.refreshCurrentReadActor(ctx)
	if err != nil {
		return err
	}
	return g.checkCurrentKBReads(fresh, types.CallerFromContext(fresh), scope.Targets, nil, scope)
}
