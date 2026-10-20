package service

import (
	"context"
	"encoding/json"

	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/types"
)

// FAQOutputAuthorizer authorizes original FAQ inputs and their current sources.
type FAQOutputAuthorizer struct {
	store   *repository.KnowledgeInputProvenanceStore
	checker knowledgeOutputLineageChecker
}

func faqOutputBodiesEqual(a, b any) bool {
	left, err := repository.KnowledgeInputDigest(a)
	if err != nil {
		return false
	}
	right, err := repository.KnowledgeInputDigest(b)
	return err == nil && left == right
}

// NewFAQOutputAuthorizer constructs the configured FAQOutputAuthorizer.
func NewFAQOutputAuthorizer(
	store *repository.KnowledgeInputProvenanceStore,
	guard *access.NextcloudSourceLineageGuard,
) *FAQOutputAuthorizer {
	return &FAQOutputAuthorizer{store, guard}
}

// FAQOutputGuard holds the original FAQ inputs selected for output.
type FAQOutputGuard struct {
	authorizer   *FAQOutputAuthorizer
	snapshot     *types.FAQInputSnapshot
	importOutput *types.FAQImportOutputSnapshot
	body         string
}
type (
	faqOutputBoundaryKey struct{}
	faqOutputBoundary    struct{ guard *FAQOutputGuard }
)

// WithFAQOutputBoundary attaches the retained FAQ output guard to a context.
func WithFAQOutputBoundary(ctx context.Context) context.Context {
	return context.WithValue(ctx, faqOutputBoundaryKey{}, &faqOutputBoundary{})
}

// FAQOutputGuardFromContext returns the FAQ guard attached to a context.
func FAQOutputGuardFromContext(ctx context.Context) *FAQOutputGuard {
	slot, _ := ctx.Value(faqOutputBoundaryKey{}).(*faqOutputBoundary)
	if slot == nil {
		return nil
	}
	return slot.guard
}

// Begin captures the original inputs and current controls for this operation.
func (a *FAQOutputAuthorizer) Begin(ctx context.Context, req types.FAQInputReadRequest) (*FAQOutputGuard, error) {
	if a == nil || a.store == nil || a.checker == nil {
		return nil, access.ErrNextcloudPublicationUnavailable
	}
	snapshot, err := a.store.CaptureFAQInputs(ctx, req)
	if err != nil {
		return nil, err
	}
	snapshot, err = cloneCollection(snapshot)
	if err != nil {
		return nil, err
	}
	g := &FAQOutputGuard{authorizer: a, snapshot: snapshot}
	return g, g.Recheck(ctx)
}

// Recheck rechecks the retained inputs and their current read controls.
func (g *FAQOutputGuard) Recheck(ctx context.Context) error {
	if g == nil || g.authorizer == nil || ctx == nil {
		return access.ErrNextcloudPublicationUnavailable
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if g.importOutput != nil {
		return g.recheckImportOutput(ctx)
	}
	lineages, err := repository.FAQInputLineages(g.snapshot)
	if err != nil {
		return err
	}
	for round := 0; round < 2; round++ {
		for _, proof := range g.snapshot.Proofs {
			if err := checkFAQOriginalKnowledgeScopes(
				ctx,
				g.authorizer.checker,
				proof.OriginalKnowledgeInputs,
			); err != nil {
				return err
			}
		}
		for _, lineage := range lineages {
			detached, err := types.UnionSourceLineage(lineage)
			if err != nil {
				return err
			}
			if err := g.authorizer.checker.Check(ctx, detached, access.LineageReadScope{}); err != nil {
				return err
			}
		}
		empty, _ := types.NewCompleteSourceLineage()
		if err := g.authorizer.checker.Check(
			ctx,
			empty,
			access.LineageReadScope{
				Targets: types.SearchTargets{
					&types.SearchTarget{
						Type:            types.SearchTargetTypeKnowledgeBase,
						TenantID:        g.snapshot.Request.TenantID,
						KnowledgeBaseID: g.snapshot.Request.KnowledgeBaseID,
					},
				},
			},
		); err != nil {
			return err
		}
		copied, err := cloneCollection(g.snapshot)
		if err != nil {
			return err
		}
		if err := g.authorizer.store.RecheckFAQInputs(ctx, copied); err != nil {
			return err
		}
	}
	return nil
}

// CheckChunks rechecks the selected chunk inputs before output.
func (g *FAQOutputGuard) CheckChunks(ctx context.Context, chunks []*types.Chunk) error {
	if g == nil {
		return access.ErrNextcloudPublicationUnavailable
	}
	byID := map[string]*types.Chunk{}
	for _, chunk := range g.snapshot.Chunks {
		byID[chunk.ID] = chunk
	}
	for _, chunk := range chunks {
		if chunk == nil || byID[chunk.ID] == nil || !faqOutputBodiesEqual(byID[chunk.ID], chunk) ||
			byID[chunk.ID].SourceContent != chunk.SourceContent ||
			byID[chunk.ID].ContextHeader != chunk.ContextHeader {
			return repository.ErrKnowledgeInputChanged
		}
	}
	return g.Recheck(ctx)
}

// KnowledgeBase returns the retained knowledge base projection.
func (g *FAQOutputGuard) KnowledgeBase() (*types.KnowledgeBase, error) {
	if g == nil || g.snapshot == nil {
		return nil, types.ErrSourceLineageUnknown
	}
	kb, err := cloneCollection(g.snapshot.KnowledgeBase)
	if err == nil {
		kb.EnsureDefaults()
	}
	return kb, err
}

// Entries returns the retained FAQ entry projection.
func (g *FAQOutputGuard) Entries(ctx context.Context, s *knowledgeService, entries []*types.FAQEntry) error {
	kb, err := g.KnowledgeBase()
	if err != nil {
		return err
	}
	chunks := map[string]*types.Chunk{}
	seqs := map[string]int64{}
	names := map[string]string{}
	for _, c := range g.snapshot.Chunks {
		chunks[c.ID] = c
	}
	for _, tag := range g.snapshot.Tags {
		seqs[tag.ID] = tag.SeqID
		names[tag.ID] = tag.Name
	}
	for _, entry := range entries {
		if entry == nil || chunks[entry.ChunkID] == nil {
			return repository.ErrKnowledgeInputChanged
		}
		chunk := chunks[entry.ChunkID]
		expected, err := s.chunkToFAQEntry(chunk, kb, seqs)
		if err != nil {
			return err
		}
		expected.TagName = names[chunk.TagID]
		// Runtime ranking values are authorized by the full original candidate
		// set. Matched text must be one of the actual admitted questions.
		if entry.MatchedQuestion != "" {
			found := entry.MatchedQuestion == expected.StandardQuestion
			for _, q := range expected.SimilarQuestions {
				if entry.MatchedQuestion == q {
					found = true
				}
			}
			if !found {
				return repository.ErrKnowledgeInputChanged
			}
		}
		expected.Score = entry.Score
		expected.MatchType = entry.MatchType
		expected.MatchedQuestion = entry.MatchedQuestion
		if !faqOutputBodiesEqual(expected, entry) {
			return repository.ErrKnowledgeInputChanged
		}
	}
	return g.Recheck(ctx)
}

func (s *knowledgeService) bindFAQEntryOutput(
	ctx context.Context,
	kb *types.KnowledgeBase,
	chunk *types.Chunk,
	entry *types.FAQEntry,
) error {
	guard, err := s.beginFAQOutput(ctx, kb, []string{chunk.ID}, false)
	if err != nil {
		return err
	}
	if guard != nil {
		if err := guard.Entries(ctx, s, []*types.FAQEntry{entry}); err != nil {
			return err
		}
	}
	return bindFAQOutput(ctx, guard, entry)
}

// Bind binds the exact response projection to the retained original inputs.
func (g *FAQOutputGuard) Bind(ctx context.Context, value any) error {
	if err := g.Recheck(ctx); err != nil {
		return err
	}
	body, err := json.Marshal(value)
	if err != nil {
		return err
	}
	g.body = string(body)
	if slot, _ := ctx.Value(faqOutputBoundaryKey{}).(*faqOutputBoundary); slot != nil {
		slot.guard = g
	}
	return nil
}

// Check rechecks the retained inputs and their current read controls.
func (g *FAQOutputGuard) Check(ctx context.Context, value any) error {
	if g == nil || g.body == "" {
		return access.ErrNextcloudPublicationUnavailable
	}
	body, err := json.Marshal(value)
	if err != nil {
		return err
	}
	if string(body) != g.body {
		return repository.ErrKnowledgeInputChanged
	}
	return g.Recheck(ctx)
}

func (s *knowledgeService) beginFAQOutput(
	ctx context.Context,
	kb *types.KnowledgeBase,
	ids []string,
	all bool,
) (*FAQOutputGuard, error) {
	if s.faqInputs == nil {
		if s.knowledgeInputs != nil {
			return nil, types.ErrSourceLineageUnknown
		}
		return nil, nil
	}
	return (&FAQOutputAuthorizer{s.faqInputs, s.faqLineageChecker}).Begin(
		ctx,
		types.FAQInputReadRequest{TenantID: kb.TenantID, KnowledgeBaseID: kb.ID, ChunkIDs: ids, AllEntries: all},
	)
}

func bindFAQOutput(ctx context.Context, guard *FAQOutputGuard, value any) error {
	if guard == nil {
		return nil
	}
	return guard.Bind(ctx, value)
}

// ReadPlan plans original material reads for the requested output.
func (a *FAQOutputAuthorizer) ReadPlan(
	ctx context.Context,
	request types.FAQInputReadRequest,
) (*types.FAQInputReadPlan, error) {
	if a == nil || a.store == nil {
		return nil, access.ErrNextcloudPublicationUnavailable
	}
	return a.store.PeekFAQInputReadPlan(ctx, request)
}
