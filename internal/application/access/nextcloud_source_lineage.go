package access

import (
	"context"
	"fmt"
	"strings"
	"time"

	apperrors "github.com/Tencent/WeKnora/internal/errors"

	"github.com/Tencent/WeKnora/internal/types"
)

// NextcloudLineageSnapshotLookup loads the durable source-lineage authorization snapshot.
type NextcloudLineageSnapshotLookup interface {
	ResolveNextcloudSourceLineage(
		context.Context,
		uint64,
		string,
		string,
	) (*types.Knowledge, types.SourceIdentity, error)
	RecheckNextcloudSourceLineage(context.Context, *types.SourceLineage) (bool, error)
}

// LineageKnowledgeBaseLookup resolves the authoritative knowledge-base scope.
type LineageKnowledgeBaseLookup interface {
	GetKnowledgeBaseByIDOnly(context.Context, string) (*types.KnowledgeBase, error)
}

// LineageGroupPermissionLookup checks group permission for the requested source scope.
type LineageGroupPermissionLookup interface {
	EffectivePermission(context.Context, uint64, types.ResourceType, string, types.ResourceAction, time.Time) (
		types.EffectiveResourcePermission,
		error,
	)
}

// LineageReadScope is supplied by a trusted entry point after owner/session
// resolution. Agent selectors are reauthorized, not accepted as grants.
type LineageReadScope struct {
	Targets       types.SearchTargets
	AgentID       string
	AgentTenantID uint64
}

// NextcloudSourceLineageGuard is deliberately unused by existing message and
// prompt paths. Wiring it before exhaustive producers would certify incomplete
// dependency sets. It is not a source lease or session-owner check.
type NextcloudSourceLineageGuard struct {
	snapshots   NextcloudLineageSnapshotLookup
	kbs         LineageKnowledgeBaseLookup
	shares      KBShareLookup
	agents      AgentShareLookup
	groups      LineageGroupPermissionLookup
	publication *NextcloudPublicationGuard
}

// NewNextcloudSourceLineageGuard returns a guard for the live source-lineage scope.
func NewNextcloudSourceLineageGuard(snapshots NextcloudLineageSnapshotLookup, kbs LineageKnowledgeBaseLookup,
	shares KBShareLookup,
	agents AgentShareLookup,
	groups LineageGroupPermissionLookup,
	publication *NextcloudPublicationGuard,
) *NextcloudSourceLineageGuard {
	return &NextcloudSourceLineageGuard{
		snapshots:   snapshots,
		kbs:         kbs,
		shares:      shares,
		agents:      agents,
		groups:      groups,
		publication: publication,
	}
}

// Check checks the live source-lineage and caller scope.
func (g *NextcloudSourceLineageGuard) Check(
	ctx context.Context,
	lineage *types.SourceLineage,
	scope LineageReadScope,
) error {
	if lineage.RequireComplete() != nil {
		return ErrNextcloudPublicationDenied
	}
	if ctx == nil || g == nil {
		return ErrNextcloudPublicationUnavailable
	}
	caller, captured := ctx.Value(types.CallerContextKey).(types.Caller)
	principal, principalCaptured := ctx.Value(types.PrincipalContextKey).(types.Principal)
	if !captured || caller.TenantID == 0 || !principalCaptured || !principal.Valid() {
		return ErrNextcloudPublicationDenied
	}
	if principal != principal.Normalize() {
		return ErrNextcloudPublicationDenied
	}
	switch principal.Type {
	case types.PrincipalWebUser, types.PrincipalAPITenant, types.PrincipalAPIPlatform, types.PrincipalAPIExternalUser,
		types.PrincipalIMUser,
		types.PrincipalEmbedChannel,
		types.PrincipalEmbedSession,
		types.PrincipalEmbedVisitor,
		types.PrincipalMCPEndpoint:
	default:
		return ErrNextcloudPublicationDenied
	}
	if principal.Type ==
		types.PrincipalWebUser &&
		(strings.TrimSpace(caller.UserID) ==
			"" ||
			principal.ID !=
				caller.UserID ||
			types.IsSyntheticUserID(caller.UserID)) {
		return ErrNextcloudPublicationDenied
	}
	if len(lineage.Sources) != 0 {
		if principal.Type !=
			types.PrincipalWebUser ||
			principal.ID !=
				caller.UserID ||
			strings.TrimSpace(caller.UserID) ==
				"" ||
			types.IsSyntheticUserID(caller.UserID) {
			return ErrNextcloudPublicationDenied
		}
		if _, machine := types.TenantAPIKeyScopeFromContext(ctx); machine {
			return ErrNextcloudPublicationDenied
		}
		if g.snapshots == nil || g.publication == nil || g.groups == nil {
			return ErrNextcloudPublicationUnavailable
		}
	}
	targets := append(types.SearchTargets{}, scope.Targets...)
	controlled := map[string]uint64{}
	for _, saved := range lineage.Sources {
		if owner, exists := controlled[saved.KnowledgeBaseID]; exists && owner != saved.TenantID {
			return ErrNextcloudPublicationDenied
		}
		controlled[saved.KnowledgeBaseID] = saved.TenantID
		targets = append(targets, &types.SearchTarget{
			Type:            types.SearchTargetTypeKnowledgeBase,
			TenantID:        saved.TenantID,
			KnowledgeBaseID: saved.KnowledgeBaseID,
		})
	}
	if err := g.checkCurrentKBReads(ctx, caller, targets, controlled, scope); err != nil {
		return err
	}
	var checkedDirectory *types.DirectoryIdentity
	if len(lineage.Sources) > 0 {
		var err error
		checkedDirectory, err = g.publication.currentDirectoryIdentity(ctx)
		if err != nil {
			return err
		}
	}
	for _, saved := range lineage.Sources {
		knowledge, current, err := g.snapshots.ResolveNextcloudSourceLineage(
			ctx,
			saved.TenantID,
			saved.KnowledgeBaseID,
			saved.KnowledgeID,
		)
		if err != nil {
			return apperrors.NewProtocolError(fmt.Errorf(
				"%w: trusted lineage lookup",
				ErrNextcloudPublicationUnavailable,
			), fmt.Sprintf("%s: trusted lineage lookup", apperrors.PublicMessage(
				ErrNextcloudPublicationUnavailable,
			)))
		}
		if current !=
			saved ||
			current.Validate() !=
				nil ||
			knowledge ==
				nil ||
			knowledge.ID !=
				saved.KnowledgeID ||
			knowledge.TenantID !=
				saved.TenantID ||
			knowledge.KnowledgeBaseID !=
				saved.KnowledgeBaseID ||
			knowledge.Channel !=
				types.ConnectorTypeNextcloud ||
			knowledge.DeletedAt.Valid ||
			knowledge.ParseStatus != types.ParseStatusCompleted || knowledge.EnableStatus != "enabled" {
			return ErrNextcloudPublicationDenied
		}
		if err := g.publication.CheckKnowledge(ctx, knowledge); err != nil {
			return err
		}
		// Catch a local version/fence change while directory/Nextcloud calls ran.
		_, after, err := g.snapshots.ResolveNextcloudSourceLineage(
			ctx,
			saved.TenantID,
			saved.KnowledgeBaseID,
			saved.KnowledgeID,
		)
		if err != nil {
			return apperrors.NewProtocolError(fmt.Errorf(
				"%w: trusted lineage recheck",
				ErrNextcloudPublicationUnavailable,
			), fmt.Sprintf("%s: trusted lineage recheck", apperrors.PublicMessage(
				ErrNextcloudPublicationUnavailable,
			)))
		}
		if after != saved {
			return ErrNextcloudPublicationDenied
		}
	}
	// Repeat grants after remote checks; no operation-wide/context grant cache
	// may extend a revoked org share, shared Agent or directory permission.
	if err := g.checkCurrentKBReads(ctx, caller, targets, controlled, scope); err != nil {
		return err
	}
	if len(lineage.Sources) == 0 {
		return nil
	}
	currentDirectory, err := g.publication.currentDirectoryIdentity(ctx)
	if err != nil {
		return err
	}
	if currentDirectory.ID != checkedDirectory.ID || currentDirectory.DirectoryID != checkedDirectory.DirectoryID ||
		currentDirectory.ObjectGUID !=
			checkedDirectory.ObjectGUID ||
		*currentDirectory.UserID !=
			*checkedDirectory.UserID {
		return ErrNextcloudPublicationDenied
	}
	matches, err := g.snapshots.RecheckNextcloudSourceLineage(ctx, lineage)
	if err != nil {
		return apperrors.NewProtocolError(fmt.Errorf(
			"%w: final coherent lineage snapshot",
			ErrNextcloudPublicationUnavailable,
		), fmt.Sprintf("%s: final coherent lineage snapshot", apperrors.PublicMessage(
			ErrNextcloudPublicationUnavailable,
		)))
	}
	if !matches {
		return ErrNextcloudPublicationDenied
	}
	return nil
}

// Resolve performs both trusted identity resolution and current authorization.
// Producers must still union every actual input, not just displayed citations.
func (
	g *NextcloudSourceLineageGuard,
) Resolve(ctx context.Context, tenantID uint64, kbID, knowledgeID string, scope LineageReadScope) (
	*types.SourceLineage,
	error,
) {
	if g == nil || g.snapshots == nil {
		return types.UnknownSourceLineage(), ErrNextcloudPublicationUnavailable
	}
	_, identity, err := g.snapshots.ResolveNextcloudSourceLineage(ctx, tenantID, kbID, knowledgeID)
	if err != nil {
		return types.UnknownSourceLineage(), apperrors.NewProtocolError(fmt.Errorf(
			"%w: trusted lineage lookup",
			ErrNextcloudPublicationUnavailable,
		), fmt.Sprintf("%s: trusted lineage lookup", apperrors.PublicMessage(ErrNextcloudPublicationUnavailable)))
	}
	if identity.TenantID != tenantID || identity.KnowledgeBaseID != kbID || identity.KnowledgeID != knowledgeID {
		return types.UnknownSourceLineage(), ErrNextcloudPublicationDenied
	}
	lineage, err := types.NewCompleteSourceLineage(identity)
	if err != nil {
		return types.UnknownSourceLineage(), ErrNextcloudPublicationDenied
	}
	if err := g.Check(ctx, lineage, scope); err != nil {
		return types.UnknownSourceLineage(), err
	}
	return lineage, nil
}

func (
	g *NextcloudSourceLineageGuard,
) checkCurrentKBReads(ctx context.Context, caller types.Caller, targets types.SearchTargets,
	controlled map[string]uint64, scope LineageReadScope,
) error {
	// A selected shared Agent supplies configuration/system input even when a
	// turn returns no KB target. Source-empty does not exempt its access check.
	if scope.AgentID != "" {
		if scope.AgentTenantID == 0 {
			return ErrNextcloudPublicationDenied
		}
		if scope.AgentTenantID != caller.TenantID {
			if g.agents == nil {
				return ErrNextcloudPublicationUnavailable
			}
			agent, err := g.agents.GetSharedAgentForTenant(
				ctx,
				caller.TenantID,
				caller.Role,
				scope.AgentID,
				scope.AgentTenantID,
			)
			if err != nil {
				return ErrNextcloudPublicationUnavailable
			}
			if agent == nil || agent.ID != scope.AgentID || agent.TenantID != scope.AgentTenantID {
				return ErrNextcloudPublicationDenied
			}
		}
		if g.groups == nil {
			return ErrNextcloudPublicationUnavailable
		}
		use, err := g.groups.EffectivePermission(
			ctx,
			scope.AgentTenantID,
			types.GroupResourceTypeAgent,
			scope.AgentID,
			types.ResourceActionUse,
			time.Now().UTC(),
		)
		if err != nil {
			return ErrNextcloudPublicationUnavailable
		}
		if !use.Allowed {
			return ErrNextcloudPublicationDenied
		}
	}
	if len(targets) == 0 {
		return nil
	}
	if g.kbs == nil {
		return ErrNextcloudPublicationUnavailable
	}
	seen := map[string]uint64{}
	for _, target := range targets {
		if target == nil || target.TenantID == 0 || strings.TrimSpace(target.KnowledgeBaseID) == "" {
			return ErrNextcloudPublicationDenied
		}
		if owner, exists := seen[target.KnowledgeBaseID]; exists {
			if owner != target.TenantID {
				return ErrNextcloudPublicationDenied
			}
			continue
		}
		seen[target.KnowledgeBaseID] = target.TenantID
		kb, err := g.kbs.GetKnowledgeBaseByIDOnly(ctx, target.KnowledgeBaseID)
		if err != nil || kb == nil {
			return ErrNextcloudPublicationUnavailable
		}
		if kb.ID != target.KnowledgeBaseID || kb.TenantID != target.TenantID || kb.DeletedAt.Valid {
			return ErrNextcloudPublicationDenied
		}
		if err := types.AuthorizeTenantAPIKeyKnowledgeBases(ctx, kb.ID); err != nil {
			return ErrNextcloudPublicationDenied
		}
		allowed := caller.TenantID == kb.TenantID
		// Current org permission is independent of an Agent's KB fallback.
		// Machine callers may hold tenant/role shares without a Web UserID.
		if !allowed && g.shares != nil {
			role, shared, err := g.shares.CheckTenantKBPermission(ctx, kb.ID, caller.TenantID, caller.Role)
			if err != nil {
				return ErrNextcloudPublicationUnavailable
			}
			allowed = shared && role.IsValid() && role.HasPermission(types.OrgRoleViewer)
		}
		if !allowed && scope.AgentID != "" {
			if g.agents == nil || scope.AgentTenantID == 0 {
				return ErrNextcloudPublicationUnavailable
			}
			agent, err := g.agents.GetSharedAgentForTenant(
				ctx,
				caller.TenantID,
				caller.Role,
				scope.AgentID,
				scope.AgentTenantID,
			)
			if err != nil {
				return ErrNextcloudPublicationUnavailable
			}
			allowed = agent !=
				nil &&
				agent.ID ==
					scope.AgentID &&
				agent.TenantID ==
					scope.AgentTenantID &&
				types.SharedAgentIncludesKB(agent, kb)
		} else if !allowed && g.agents != nil {
			var err error
			allowed, err = g.agents.TenantCanAccessKBViaSomeSharedAgent(ctx, caller.TenantID, caller.Role, kb)
			if err != nil {
				return ErrNextcloudPublicationUnavailable
			}
		}
		if !allowed {
			return ErrNextcloudPublicationDenied
		}
		_, sourceControlled := controlled[kb.ID]
		if (sourceControlled || kb.EverHadNextcloudSource) && g.groups == nil {
			return ErrNextcloudPublicationUnavailable
		}
		if g.groups != nil {
			grant, err := g.groups.EffectivePermission(
				ctx,
				kb.TenantID,
				types.GroupResourceTypeKnowledgeBase,
				kb.ID,
				types.ResourceActionRead,
				time.Now().UTC(),
			)
			if err != nil {
				return ErrNextcloudPublicationUnavailable
			}
			if !grant.Allowed {
				return ErrNextcloudPublicationDenied
			}
		}
	}
	return nil
}
