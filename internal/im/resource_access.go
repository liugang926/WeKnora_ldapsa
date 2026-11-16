package im

import (
	"context"
	"slices"

	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	secutils "github.com/Tencent/WeKnora/internal/utils"
)

// authorizeOutboundResource checks actual file ownership and live KB sharing.
// An answer can echo a user-supplied handle; appearing in that answer never
// grants access to another workspace's storage credentials.
func (s *Service) authorizeOutboundResource(
	ctx context.Context,
	tenant *types.Tenant,
	agent *types.CustomAgent,
	kbIDs []string,
	reference string,
) error {
	if tenant == nil || tenant.ID == 0 || s.resourceCatalog == nil {
		return access.ErrUnauthorized
	}
	caller := types.CallerFromContext(ctx)
	if caller.TenantID != tenant.ID {
		return access.ErrUnauthorized
	}
	physical, resource, err := s.resourceCatalog.ResolvePath(ctx, reference)
	if err != nil {
		return err
	}
	owner := secutils.ParseTenantIDFromStoragePath(physical)
	if resource != nil {
		owner = resource.TenantID
	}
	if owner == 0 {
		return access.ErrForbidden
	}
	if scope, ok := types.IMChannelScopeFromContext(ctx); ok {
		if err := s.checkChannelResource(ctx, scope, owner, agent, kbIDs, reference); err != nil {
			return err
		}
	}
	if owner == tenant.ID {
		return secutils.ValidateStoragePathTenant(physical, owner)
	}
	bindings, ok := s.resourceCatalog.(interface {
		interfaces.KBResourceLookup
		interfaces.MessageFileBindingLookup
	})
	if !ok || s.kbService == nil || s.kbShareService == nil {
		return access.ErrForbidden
	}
	origins, err := bindings.GetMessageFileBindings(ctx, owner, reference, "")
	if err != nil {
		return err
	}
	if origins == nil {
		return access.ErrForbidden
	}
	for _, kbID := range origins.KnowledgeBaseIDs {
		if !outboundKBInScope(agent, kbIDs, kbID) {
			continue
		}
		kb, err := s.kbService.GetKnowledgeBaseByIDOnly(ctx, kbID)
		if err != nil || kb == nil || kb.TenantID != owner {
			continue
		}
		grant, err := access.ResolveKB(ctx, access.KBRequest{Caller: caller}, kb,
			types.OrgRoleViewer, s.kbShareService, nil)
		if err != nil {
			continue
		}
		if _, err := access.ResolveKBFile(ctx, grant, kbID, reference, s.resourceCatalog, bindings); err == nil {
			return nil
		}
	}
	return access.ErrForbidden
}

func (s *Service) checkChannelResource(ctx context.Context, scope types.IMChannelScope, owner uint64,
	agent *types.CustomAgent, kbIDs []string, reference string,
) error {
	// Channel replies can echo any storage handle supplied by a model. Bind
	// each signed URL to a live channel KB or this reply's own artifact.
	if err := scope.Check(ctx, scope.TenantID, types.GroupResourceTypeAgent, scope.AgentID, types.ResourceActionUse); err != nil {
		return err
	}
	bindings, ok := s.resourceCatalog.(interfaces.MessageFileBindingLookup)
	if !ok {
		return access.ErrForbidden
	}
	messageID := ""
	if entry, _ := ctx.Value(inflightOwnerKey{}).(*inflightEntry); entry != nil {
		entry.mu.Lock()
		messageID = entry.assistantMessageID
		entry.mu.Unlock()
	}
	origins, err := bindings.GetMessageFileBindings(ctx, owner, reference, messageID)
	if err != nil {
		return err
	}
	if origins == nil {
		return access.ErrForbidden
	}
	if messageID != "" && owner == scope.TenantID && origins.MessageArtifact {
		return nil
	}
	for _, kbID := range origins.KnowledgeBaseIDs {
		if outboundKBInScope(agent, kbIDs, kbID) && scope.Check(ctx, owner, types.GroupResourceTypeKnowledgeBase, kbID, types.ResourceActionRead) == nil {
			return nil
		}
	}
	return access.ErrForbidden
}

func outboundKBInScope(agent *types.CustomAgent, kbIDs []string, kbID string) bool {
	if len(kbIDs) != 0 {
		return slices.Contains(kbIDs, kbID)
	}
	if agent == nil {
		return true
	}
	switch agent.Config.KBSelectionMode {
	case "all":
		return true
	case "none":
		return false
	default:
		return slices.Contains(agent.Config.KnowledgeBases, kbID)
	}
}
