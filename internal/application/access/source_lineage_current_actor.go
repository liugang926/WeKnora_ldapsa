package access

import (
	"context"
	"reflect"

	"github.com/Tencent/WeKnora/internal/types"
)

func sameReadActorSelectors(before, after context.Context) bool {
	callerBefore, capturedBefore := before.Value(types.CallerContextKey).(types.Caller)
	callerAfter, capturedAfter := after.Value(types.CallerContextKey).(types.Caller)
	principalBefore, principalCapturedBefore := before.Value(types.PrincipalContextKey).(types.Principal)
	principalAfter, principalCapturedAfter := after.Value(types.PrincipalContextKey).(types.Principal)
	if !capturedBefore || !capturedAfter || !principalCapturedBefore || !principalCapturedAfter ||
		callerBefore.TenantID != callerAfter.TenantID ||
		callerBefore.UserID != callerAfter.UserID ||
		principalBefore != principalAfter {
		return false
	}
	executionBefore, executionCaptured := types.TenantIDFromContext(before)
	executionAfter, _ := types.TenantIDFromContext(after)
	if executionCaptured && executionBefore != 0 && executionBefore != executionAfter {
		return false
	}
	keyBefore, keyCapturedBefore := types.TenantAPIKeyScopeFromContext(before)
	keyAfter, keyCapturedAfter := types.TenantAPIKeyScopeFromContext(after)
	return keyCapturedBefore == keyCapturedAfter &&
		(!keyCapturedBefore || keyBefore.KeyID == keyAfter.KeyID && keyBefore.ScopeType == keyAfter.ScopeType)
}

func (g *NextcloudSourceLineageGuard) refreshCurrentReadActor(ctx context.Context) (context.Context, error) {
	if ctx == nil || g.currentReadActor == nil {
		return ctx, ErrNextcloudPublicationUnavailable
	}
	fresh, err := g.currentReadActor.RefreshCurrentReadActor(ctx)
	if err != nil || fresh == nil || !sameReadActorSelectors(ctx, fresh) {
		return ctx, ErrNextcloudPublicationDenied
	}
	return fresh, nil
}

func (g *NextcloudSourceLineageGuard) recheckCurrentReadActor(ctx context.Context) error {
	fresh, err := g.refreshCurrentReadActor(ctx)
	if err != nil {
		return err
	}
	if types.CallerFromContext(ctx) != types.CallerFromContext(fresh) {
		return ErrNextcloudPublicationDenied
	}
	oldKey, oldOK := types.TenantAPIKeyScopeFromContext(ctx)
	newKey, newOK := types.TenantAPIKeyScopeFromContext(fresh)
	// A credential label is diagnostic metadata, not current authorization.
	oldKey.Name, newKey.Name = "", ""
	if oldOK != newOK || !reflect.DeepEqual(oldKey, newKey) {
		return ErrNextcloudPublicationDenied
	}
	return nil
}
