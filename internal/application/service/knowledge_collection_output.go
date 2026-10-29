package service

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
)

// KnowledgeCollectionOutputAuthorizer authorizes inputs used to select knowledge collections.
type KnowledgeCollectionOutputAuthorizer struct {
	reader  interfaces.KnowledgeCollectionInputReader
	checker knowledgeOutputLineageChecker
}

// NewKnowledgeCollectionOutputAuthorizer constructs the configured KnowledgeCollectionOutputAuthorizer.
func NewKnowledgeCollectionOutputAuthorizer(
	store *repository.KnowledgeInputProvenanceStore,
	guard *access.NextcloudSourceLineageGuard,
) *KnowledgeCollectionOutputAuthorizer {
	return &KnowledgeCollectionOutputAuthorizer{reader: store, checker: guard}
}

// KnowledgeCollectionOutputGuard holds and rechecks a knowledge collection selection.
type KnowledgeCollectionOutputGuard struct {
	authorizer *KnowledgeCollectionOutputAuthorizer
	snapshot   *types.KnowledgeCollectionInputSnapshot
	scope      access.LineageReadScope
	selection  *types.KnowledgeCollectionSelection
}

func cloneCollection[T any](value T) (T, error) {
	var copied T
	encoded, err := json.Marshal(value)
	if err != nil {
		return copied, err
	}
	err = json.Unmarshal(encoded, &copied)
	return copied, err
}

// Begin captures the original inputs and current controls for this operation.
func (a *KnowledgeCollectionOutputAuthorizer) Begin(
	ctx context.Context,
	request types.KnowledgeCollectionReadRequest,
	scope access.LineageReadScope,
) (*KnowledgeCollectionOutputGuard, error) {
	if a == nil || a.reader == nil || a.checker == nil {
		return nil, access.ErrNextcloudPublicationUnavailable
	}
	snapshot, err := a.reader.CaptureKnowledgeCollectionInputs(ctx, request)
	if err != nil {
		return nil, err
	}
	snapshot, err = cloneCollection(snapshot)
	if err != nil {
		return nil, err
	}
	scope.Targets = types.SearchTargets{}
	for _, checkpoint := range snapshot.Scopes {
		scope.Targets = append(
			scope.Targets,
			&types.SearchTarget{
				Type:            types.SearchTargetTypeKnowledgeBase,
				TenantID:        checkpoint.Scope.TenantID,
				KnowledgeBaseID: checkpoint.Scope.KBID,
			},
		)
	}
	scope, err = cloneCollection(scope)
	if err != nil {
		return nil, err
	}
	g := &KnowledgeCollectionOutputGuard{authorizer: a, snapshot: snapshot, scope: scope}
	if err := g.Check(ctx, nil); err != nil {
		return nil, err
	}
	return g, nil
}

func (g *KnowledgeCollectionOutputGuard) authorize(ctx context.Context) error {
	if g == nil || g.authorizer == nil || ctx == nil {
		return access.ErrNextcloudPublicationUnavailable
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if access.UseWeKnoraReadAuthority() {
		return g.authorizeWeKnoraScope(ctx)
	}
	// Each original producer stays bounded by SourceLineage's existing limit.
	// Empty independent origins share one current full-scope grant check; source
	// origins are checked individually, without a whole-KB lineage union.
	seen := map[string]bool{}
	check := func(original *types.SourceLineage) error {
		if original == nil || original.RequireComplete() != nil {
			return types.ErrSourceLineageUnknown
		}
		if len(original.Sources) == 0 {
			return nil
		}
		encoded, err := json.Marshal(original)
		if err != nil {
			return err
		}
		if seen[string(encoded)] {
			return nil
		}
		seen[string(encoded)] = true
		lineage, err := cloneCollection(original)
		if err != nil {
			return err
		}
		scope, err := cloneCollection(g.scope)
		if err != nil {
			return err
		}
		// Scope membership was captured separately. The final grant pass below
		// rechecks every selected KB once, after all per-source remote checks.
		scope.Targets = nil
		return g.authorizer.checker.Check(ctx, lineage, scope)
	}
	for _, checkpoint := range g.snapshot.Scopes {
		if err := check(checkpoint.ConfigurationLineage); err != nil {
			return err
		}
	}
	for _, candidate := range g.snapshot.Candidates {
		if candidate == nil || candidate.Proof == nil {
			return types.ErrSourceLineageUnknown
		}
		if err := check(candidate.Proof.SourceLineage); err != nil {
			return err
		}
	}
	if g.selection != nil && len(g.selection.FAQImportResults) > 0 {
		reader, ok := g.authorizer.reader.(interfaces.FAQImportOutputInputReader)
		if !ok {
			return types.ErrSourceLineageUnknown
		}
		for _, result := range g.selection.FAQImportResults {
			if err := checkFAQImportOutputReceipt(ctx, reader, g.authorizer.checker, result); err != nil {
				return err
			}
		}
	}
	if g.snapshot.Auxiliary != nil {
		for _, proof := range g.snapshot.Auxiliary.Proofs {
			if proof == nil {
				return types.ErrSourceLineageUnknown
			}
			if err := check(proof.SourceLineage); err != nil {
				return err
			}
		}
	}
	lineage, err := types.NewCompleteSourceLineage()
	if err != nil {
		return err
	}
	scope, err := cloneCollection(g.scope)
	if err != nil {
		return err
	}
	return g.authorizer.checker.Check(ctx, lineage, scope)
}

func (g *KnowledgeCollectionOutputGuard) authorizeWeKnoraScope(ctx context.Context) error {
	if g == nil || g.authorizer == nil || ctx == nil {
		return access.ErrNextcloudPublicationUnavailable
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	// The opt-in path is entered only after the HTTP scope resolver applied
	// WeKnora's KB/share/agent ACLs. Collection snapshots are still rechecked
	// against exact membership and stable source identity below.
	return nil
}

// Select selects a collection from the retained original inputs.
func (g *KnowledgeCollectionOutputGuard) Select(
	ctx context.Context,
	request types.KnowledgeCollectionSelectionRequest,
) (*types.KnowledgeCollectionSelection, error) {
	if g == nil || g.authorizer == nil || g.selection != nil {
		return nil, types.ErrSourceLineageUnknown
	}
	request, err := cloneCollection(request)
	if err != nil {
		return nil, err
	}
	snapshot, err := cloneCollection(g.snapshot)
	if err != nil {
		return nil, err
	}
	selection, err := g.authorizer.reader.SelectKnowledgeCollectionInputs(ctx, snapshot, request)
	if err != nil {
		return nil, err
	}
	g.selection, err = cloneCollection(selection)
	if err != nil {
		return nil, err
	}
	return selection, nil
}

// Check rechecks the retained inputs and their current read controls.
func (g *KnowledgeCollectionOutputGuard) Check(
	ctx context.Context,
	selection *types.KnowledgeCollectionSelection,
) error {
	if g == nil || g.authorizer == nil {
		return access.ErrNextcloudPublicationUnavailable
	}
	checkBody := func() error {
		if (selection == nil) != (g.selection == nil) {
			return repository.ErrKnowledgeInputChanged
		}
		actual, err := json.Marshal(selection)
		if err != nil {
			return err
		}
		expected, err := json.Marshal(g.selection)
		if err != nil {
			return err
		}
		if string(actual) != string(expected) {
			return repository.ErrKnowledgeInputChanged
		}
		return nil
	}
	if err := checkBody(); err != nil {
		return fmt.Errorf("knowledge collection body check: %w", err)
	}
	// Remote source/grant authorization precedes the final whole-set local
	// snapshot, including membership, control versions and non-returned rows.
	for round := 0; round < 2; round++ {
		if err := g.authorize(ctx); err != nil {
			return fmt.Errorf("knowledge collection authorization: %w", err)
		}
		snapshot, err := cloneCollection(g.snapshot)
		if err != nil {
			return err
		}
		original, err := cloneCollection(g.selection)
		if err != nil {
			return err
		}
		if err := g.authorizer.reader.RecheckKnowledgeCollectionInputs(ctx, snapshot, original); err != nil {
			return fmt.Errorf("knowledge collection input recheck: %w", err)
		}
	}
	if err := checkBody(); err != nil {
		return fmt.Errorf("knowledge collection final body check: %w", err)
	}
	return nil
}

// FAQResultReadPlan plans original material reads for FAQ result output.
func (a *KnowledgeCollectionOutputAuthorizer) FAQResultReadPlan(
	ctx context.Context,
	scopes []types.KnowledgeSearchScope,
) (*types.FAQImportOutputReadSetPlan, error) {
	if a == nil {
		return nil, access.ErrNextcloudPublicationUnavailable
	}
	store, ok := a.reader.(*repository.KnowledgeInputProvenanceStore)
	if !ok || store == nil {
		return nil, types.ErrSourceLineageUnknown
	}
	return store.PeekFAQImportOutputReadSetPlan(ctx, scopes, nil)
}
