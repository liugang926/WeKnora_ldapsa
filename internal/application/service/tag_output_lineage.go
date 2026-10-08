package service

import (
	"context"
	"encoding/json"
	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
)

type TagOutputAuthorizer struct {
	reader  interfaces.TagOutputInputReader
	checker knowledgeOutputLineageChecker
}

func NewTagOutputAuthorizer(store *repository.KnowledgeInputProvenanceStore, guard *access.NextcloudSourceLineageGuard) *TagOutputAuthorizer {
	return &TagOutputAuthorizer{reader: store, checker: guard}
}

type TagOutputGuard struct {
	authorizer *TagOutputAuthorizer
	snapshot   *types.TagOutputInputSnapshot
	selection  *types.TagOutputSelection
	scope      access.LineageReadScope
}

func (a *TagOutputAuthorizer) Begin(ctx context.Context, req types.TagOutputReadRequest) (*TagOutputGuard, error) {
	if a == nil || a.reader == nil || a.checker == nil {
		return nil, access.ErrNextcloudPublicationUnavailable
	}
	snapshot, err := a.reader.CaptureTagOutputInputs(ctx, req)
	if err != nil {
		return nil, err
	}
	return a.BeginCommitted(ctx, snapshot)
}
func (a *TagOutputAuthorizer) BeginCommitted(ctx context.Context, original *types.TagOutputInputSnapshot) (*TagOutputGuard, error) {
	if a == nil || a.reader == nil || a.checker == nil || original == nil {
		return nil, access.ErrNextcloudPublicationUnavailable
	}
	snapshot, err := cloneCollection(original)
	if err != nil {
		return nil, err
	}
	r := snapshot.Request
	if r.TenantID == 0 || r.KnowledgeBaseID == "" || (r.TagID != "" && (len(snapshot.Tags) != 1 || snapshot.Tags[0] == nil || snapshot.Tags[0].ID != r.TagID)) {
		return nil, types.ErrSourceLineageUnknown
	}
	g := &TagOutputGuard{authorizer: a, snapshot: snapshot, scope: access.LineageReadScope{Targets: types.SearchTargets{&types.SearchTarget{Type: types.SearchTargetTypeKnowledgeBase, TenantID: r.TenantID, KnowledgeBaseID: r.KnowledgeBaseID}}}}
	if err := g.Check(ctx, nil); err != nil {
		return nil, err
	}
	return g, nil
}
func (g *TagOutputGuard) Tag() (*types.KnowledgeTag, error) {
	if g == nil || g.snapshot == nil || g.snapshot.Request.TagID == "" || len(g.snapshot.Tags) != 1 {
		return nil, types.ErrSourceLineageUnknown
	}
	return cloneCollection(g.snapshot.Tags[0])
}
func (g *TagOutputGuard) Select(ctx context.Context, req types.TagOutputSelectionRequest) (*types.TagOutputSelection, error) {
	if g == nil || g.authorizer == nil || g.selection != nil {
		return nil, types.ErrSourceLineageUnknown
	}
	snapshot, err := cloneCollection(g.snapshot)
	if err != nil {
		return nil, err
	}
	selection, err := g.authorizer.reader.SelectTagOutputInputs(ctx, snapshot, req)
	if err != nil {
		return nil, err
	}
	g.selection, err = cloneCollection(selection)
	return selection, err
}
func (g *TagOutputGuard) authorize(ctx context.Context) error {
	seen := map[string]bool{}
	check := func(original *types.SourceLineage) error {
		if original.RequireComplete() != nil {
			return types.ErrSourceLineageUnknown
		}
		if len(original.Sources) == 0 {
			return nil
		}
		b, err := json.Marshal(original)
		if err != nil {
			return err
		}
		if seen[string(b)] {
			return nil
		}
		seen[string(b)] = true
		lineage, err := cloneCollection(original)
		if err != nil {
			return err
		}
		// Original source producers are checked individually. The captured KB
		// scope receives a final grant check; an entire KB never forms one union.
		return g.authorizer.checker.Check(ctx, lineage, access.LineageReadScope{})
	}
	for _, p := range g.snapshot.TagProofs {
		if p == nil {
			return types.ErrSourceLineageUnknown
		}
		if err := check(p.SourceLineage); err != nil {
			return err
		}
	}
	for _, k := range g.snapshot.Knowledge {
		if k == nil || k.Proof == nil {
			return types.ErrSourceLineageUnknown
		}
		if err := check(k.Proof.SourceLineage); err != nil {
			return err
		}
	}
	for _, c := range g.snapshot.Chunks {
		if c == nil {
			return types.ErrSourceLineageUnknown
		}
		if err := check(c.SourceLineage); err != nil {
			return err
		}
		if err := checkFAQOriginalKnowledgeScopes(ctx, g.authorizer.checker, c.OriginalKnowledgeInputs); err != nil {
			return err
		}
	}
	if g.snapshot.Auxiliary != nil {
		for _, p := range g.snapshot.Auxiliary.Proofs {
			if p == nil {
				return types.ErrSourceLineageUnknown
			}
			if err := check(p.SourceLineage); err != nil {
				return err
			}
		}
	}
	empty, _ := types.NewCompleteSourceLineage()
	scope, err := cloneCollection(g.scope)
	if err != nil {
		return err
	}
	return g.authorizer.checker.Check(ctx, empty, scope)
}
func (g *TagOutputGuard) Check(ctx context.Context, published any) error {
	if g == nil || g.authorizer == nil || ctx == nil {
		return access.ErrNextcloudPublicationUnavailable
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	body := func() error {
		if published == nil {
			if g.selection != nil {
				return repository.ErrKnowledgeInputChanged
			}
			return nil
		}
		var expected any = g.selection
		if g.snapshot.Request.TagID != "" {
			expected = g.snapshot.Tags[0]
		}
		actual, err := json.Marshal(published)
		if err != nil {
			return err
		}
		original, err := json.Marshal(expected)
		if err != nil {
			return err
		}
		if string(actual) != string(original) {
			return repository.ErrKnowledgeInputChanged
		}
		return nil
	}
	if err := body(); err != nil {
		return err
	}
	// Each remote authorization is followed by a coherent whole-set SQL pass.
	// Repeat after the pass to catch account/key or source changes during it.
	for round := 0; round < 2; round++ {
		if err := g.authorize(ctx); err != nil {
			return err
		}
		snapshot, err := cloneCollection(g.snapshot)
		if err != nil {
			return err
		}
		selection, err := cloneCollection(g.selection)
		if err != nil {
			return err
		}
		if err := g.authorizer.reader.RecheckTagOutputInputs(ctx, snapshot, selection); err != nil {
			return err
		}
	}
	return body()
}
