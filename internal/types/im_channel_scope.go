package types

import (
	"context"
	"errors"
)

const IMChannelScopeContextKey ContextKey = "IMChannelScope"

// IMChannelScope is minted only by the verified IM service. It is not serialized
// or accepted from HTTP/message fields. The live verifier carries the channel
// binding; copying an IM principal or a synthetic user name conveys no grant.
type IMChannelScope struct {
	TenantID       uint64
	AgentID        string
	OwnerID        string
	ChannelID      string
	KnowledgeBases map[string]uint64
	Verify         func(context.Context, uint64, ResourceType, string, ResourceAction) error
	caller         Caller
	principal      Principal
}

func WithIMChannelScope(ctx context.Context, scope IMChannelScope) context.Context {
	scope.caller = CallerFromContext(ctx)
	scope.principal, _ = PrincipalFromContext(ctx)
	copyIDs := make(map[string]uint64, len(scope.KnowledgeBases))
	for id, tenant := range scope.KnowledgeBases {
		copyIDs[id] = tenant
	}
	scope.KnowledgeBases = copyIDs
	return context.WithValue(WithCaller(ctx, scope.caller), IMChannelScopeContextKey, scope)
}
func IMChannelScopeFromContext(ctx context.Context) (IMChannelScope, bool) {
	scope, ok := ctx.Value(IMChannelScopeContextKey).(IMChannelScope)
	return scope, ok
}
func (s IMChannelScope) Check(ctx context.Context, tenant uint64, kind ResourceType, id string, action ResourceAction) error {
	denied := errors.New("IM channel scope does not allow this operation")
	principal, ok := PrincipalFromContext(ctx)
	if !ok || principal.Type != PrincipalIMUser || principal != s.principal || CallerFromContext(ctx) != s.caller || s.TenantID == 0 || s.TenantID != s.caller.TenantID || s.Verify == nil {
		return denied
	}
	allowed := kind == GroupResourceTypeAgent && action == ResourceActionUse && tenant == s.TenantID && id == s.AgentID && id != ""
	if kind == GroupResourceTypeKnowledgeBase && action == ResourceActionRead {
		owner, exists := s.KnowledgeBases[id]
		allowed = exists && owner == tenant && tenant != 0
	}
	if !allowed {
		return denied
	}
	return s.Verify(ctx, tenant, kind, id, action)
}
func AuthorizeIMKnowledgeBases(ctx context.Context, ids ...string) error {
	scope, ok := IMChannelScopeFromContext(ctx)
	if !ok {
		return nil
	}
	for _, id := range ids {
		if err := scope.Check(ctx, scope.KnowledgeBases[id], GroupResourceTypeKnowledgeBase, id, ResourceActionRead); err != nil {
			return err
		}
	}
	return nil
}
