package access

import (
	"context"
	"fmt"

	"github.com/Tencent/WeKnora/internal/types"
)

// SourceLineageRead binds an original generated value to its trusted entry
// scope. A union alone cannot preserve distinct selected-Agent permissions.
type SourceLineageRead struct {
	Lineage *types.SourceLineage
	Scope   LineageReadScope
}
type SourceLineageAuthorizer interface {
	Check(context.Context, *types.SourceLineage, LineageReadScope) error
	CheckBatch(context.Context, []SourceLineageRead) error
}

// CheckBatch retains each original Agent/KB scope and then rechecks all
// source dependencies in one local snapshot after every remote and final scope
// lookup. As with Check, live remote ACLs are checked at the operation boundary;
// this is not a distributed lock on future permission changes.
func (g *NextcloudSourceLineageGuard) CheckBatch(ctx context.Context, reads []SourceLineageRead) error {
	if ctx == nil || g == nil {
		return ErrNextcloudPublicationUnavailable
	}
	var refreshErr error
	ctx, refreshErr = g.refreshCurrentReadActor(ctx)
	if refreshErr != nil {
		return refreshErr
	}
	union, _ := types.NewCompleteSourceLineage()
	frozen := make([]SourceLineageRead, 0, len(reads))
	for _, read := range reads {
		var err error
		union, err = types.UnionSourceLineage(union, read.Lineage)
		if err != nil || union.RequireComplete() != nil {
			return ErrNextcloudPublicationDenied
		}
		lineage, _ := types.UnionSourceLineage(read.Lineage)
		scope := read.Scope
		scope.Targets = make(types.SearchTargets, len(read.Scope.Targets))
		for i, target := range read.Scope.Targets {
			if target == nil {
				continue
			}
			copy := *target
			copy.KnowledgeIDs = append([]string(nil), target.KnowledgeIDs...)
			copy.TagIDs = append([]string(nil), target.TagIDs...)
			copy.ScopeTagIDs = append([]string(nil), target.ScopeTagIDs...)
			scope.Targets[i] = &copy
		}
		frozen = append(frozen, SourceLineageRead{Lineage: lineage, Scope: scope})
	}
	reads = frozen
	var initial *types.DirectoryIdentity
	if len(union.Sources) > 0 {
		if g.publication == nil || g.snapshots == nil {
			return ErrNextcloudPublicationUnavailable
		}
		var err error
		initial, err = g.publication.currentDirectoryIdentity(ctx)
		if err != nil {
			return err
		}
	}
	for _, read := range reads {
		if err := g.Check(ctx, read.Lineage, read.Scope); err != nil {
			return err
		}
	}
	caller := types.CallerFromContext(ctx)
	// No generic empty scope can replace the selected Agent and target grants.
	for _, read := range reads {
		targets := append(types.SearchTargets{}, read.Scope.Targets...)
		controlled := map[string]uint64{}
		for _, source := range read.Lineage.Sources {
			if owner, exists := controlled[source.KnowledgeBaseID]; exists && owner != source.TenantID {
				return ErrNextcloudPublicationDenied
			}
			controlled[source.KnowledgeBaseID] = source.TenantID
			targets = append(
				targets,
				&types.SearchTarget{
					Type:            types.SearchTargetTypeKnowledgeBase,
					TenantID:        source.TenantID,
					KnowledgeBaseID: source.KnowledgeBaseID,
				},
			)
		}
		if err := g.checkCurrentKBReads(ctx, caller, targets, controlled, read.Scope); err != nil {
			return err
		}
	}
	if len(union.Sources) == 0 {
		return g.recheckCurrentReadActor(ctx)
	}
	current, err := g.publication.currentDirectoryIdentity(ctx)
	if err != nil {
		return err
	}
	if current.ID != initial.ID || current.DirectoryID != initial.DirectoryID ||
		current.ObjectGUID != initial.ObjectGUID ||
		*current.UserID != *initial.UserID {
		return ErrNextcloudPublicationDenied
	}
	matches, err := g.snapshots.RecheckNextcloudSourceLineage(ctx, union)
	if err != nil {
		return fmt.Errorf("%w: final batch lineage snapshot", ErrNextcloudPublicationUnavailable)
	}
	if !matches {
		return ErrNextcloudPublicationDenied
	}
	return g.recheckCurrentReadActor(ctx)
}
