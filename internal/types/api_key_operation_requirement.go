package types

import "context"

type apiKeyOperationRequirementKey struct{}

// APIKeyOperationRequirement is set by the trusted route gate. It preserves
// the exact any-of capability policy to reauthorize a key after authentication.
type APIKeyOperationRequirement struct {
	PlatformOnly      bool
	RequireFullAccess bool
	Capabilities      []APIKeyCapability
}

// WithAPIKeyOperationRequirement attaches the trusted route capability policy.
func WithAPIKeyOperationRequirement(ctx context.Context, requirement APIKeyOperationRequirement) context.Context {
	requirement.Capabilities = append([]APIKeyCapability(nil), requirement.Capabilities...)
	return context.WithValue(ctx, apiKeyOperationRequirementKey{}, requirement)
}

// APIKeyOperationRequirementFromContext returns a copy of the trusted route capability policy.
func APIKeyOperationRequirementFromContext(ctx context.Context) (APIKeyOperationRequirement, bool) {
	if ctx == nil {
		return APIKeyOperationRequirement{}, false
	}
	r, ok := ctx.Value(apiKeyOperationRequirementKey{}).(APIKeyOperationRequirement)
	r.Capabilities = append([]APIKeyCapability(nil), r.Capabilities...)
	return r, ok
}

// Allows rechecks the exact route policy against the current key scope.
func (r APIKeyOperationRequirement) Allows(scope TenantAPIKeyScope) bool {
	if r.PlatformOnly && (!scope.IsPlatform() || len(r.Capabilities) == 0) {
		return false
	}
	if scope.FullAccess {
		return true
	}
	for _, capability := range r.Capabilities {
		if scope.HasCapability(capability) {
			return true
		}
	}
	return !r.RequireFullAccess && len(r.Capabilities) == 0
}
