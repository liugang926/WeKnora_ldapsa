package types

import (
	"context"
	"strings"
)

// WithTaskAuthorization restores an admitted task's actor for live permission
// checks. API key attribution never becomes a web-user identity. Empty legacy
// payloads remain unverified, even if the worker was given an ambient user.
func WithTaskAuthorization(ctx context.Context, tenantID uint64, initiator TaskInitiator) context.Context {
	ctx = WithBackgroundTask(ctx)
	ctx = initiator.Apply(ctx)
	caller := Caller{}
	principal := Principal{Type: "background_unverified", ID: "task"}
	if initiator.APIKeyID > 0 || initiator.APIKeyName != "" {
		principal.Type = PrincipalAPITenant
	} else if userID := strings.TrimSpace(initiator.UserID); userID != "" && !IsSyntheticUserID(userID) {
		principal = Principal{Type: PrincipalWebUser, ID: userID}
		caller = Caller{TenantID: tenantID, UserID: userID, Role: initiator.Role}
	}
	ctx = WithCaller(ctx, caller)
	ctx = WithPrincipal(ctx, principal)
	return WithExecutionTenant(ctx, tenantID)
}
