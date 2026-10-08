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

func WithAPIKeyOperationRequirement(ctx context.Context, requirement APIKeyOperationRequirement) context.Context {
	requirement.Capabilities = append([]APIKeyCapability(nil), requirement.Capabilities...)
	return context.WithValue(ctx, apiKeyOperationRequirementKey{}, requirement)
}

func APIKeyOperationRequirementFromContext(ctx context.Context) (APIKeyOperationRequirement, bool) {
	if ctx == nil {
		return APIKeyOperationRequirement{}, false
	}
	r, ok := ctx.Value(apiKeyOperationRequirementKey{}).(APIKeyOperationRequirement)
	r.Capabilities = append([]APIKeyCapability(nil), r.Capabilities...)
	return r, ok
}

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
