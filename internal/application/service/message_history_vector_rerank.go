package service

import (
	"context"
	"math"
	"sort"

	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/logprivacy"
	"github.com/Tencent/WeKnora/internal/models/rerank"
	"github.com/Tencent/WeKnora/internal/types"
)

// searchHistoryVectorViaNativeBridge is the integration target for the
// existing vectorSearchViaKB method. No text-bearing legacy search is used.
func (s *messageService) searchHistoryVectorViaNativeBridge(
	ctx context.Context,
	params *types.MessageSearchParams,
	kbID string,
	rc *types.RetrievalConfig,
) ([]*types.MessageSearchResultItem, map[string][]*types.Message, error) {
	if ctx == nil || params == nil || s.materialStore == nil {
		return nil, nil, access.ErrNextcloudPublicationUnavailable
	}
	tenant, ok := types.TenantIDFromContext(ctx)
	if !ok || tenant == 0 || params.OwnerID == "" || params.OwnerID != types.SessionOwnerIDFromContext(ctx) {
		return nil, nil, access.ErrNextcloudPublicationDenied
	}
	kb, ok := s.kbService.(historyVectorKnowledgeService)
	if !ok {
		return nil, nil, types.ErrVectorCandidateRetrievalUnavailable
	}
	ownedIDs, err := s.materialStore.OwnedHistoryKnowledgeIDs(ctx, tenant, kbID, params.OwnerID, params.SessionIDs)
	if err != nil {
		return nil, nil, err
	}
	if len(ownedIDs) == 0 {
		return []*types.MessageSearchResultItem{}, map[string][]*types.Message{}, nil
	}
	search := types.SearchParams{
		QueryText:            params.Query,
		MatchCount:           min(rc.GetEffectiveEmbeddingTopK(), historySearchMaximumResults),
		VectorThreshold:      rc.GetEffectiveVectorThreshold(),
		DisableKeywordsMatch: true,
	}
	candidates, err := kb.SearchHistoryVectorCandidates(ctx, kbID, ownedIDs, search)
	if err != nil {
		return nil, nil, err
	}
	if len(candidates) == 0 {
		return []*types.MessageSearchResultItem{}, map[string][]*types.Message{}, nil
	}
	if len(candidates) > maxRetrievalPoolSize {
		return nil, nil, access.ErrNextcloudPublicationUnavailable
	}
	selected := map[string]bool{}
	for _, candidate := range candidates {
		if candidate == nil || candidate.KnowledgeID == "" || candidate.KnowledgeBaseID != kbID {
			return nil, nil, types.ErrVectorCandidateScope
		}
		selected[candidate.KnowledgeID] = true
	}
	ids := make([]string, 0, len(selected))
	for id := range selected {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	parents, err := s.materialStore.PeekOwnedHistoryKnowledgeParentPlans(
		ctx,
		tenant,
		kbID,
		params.OwnerID,
		ids,
		params.SessionIDs,
	)
	if err != nil {
		return nil, nil, err
	}
	// Missing/changed candidates make this attempt incomplete; they cannot
	// become an apparently complete empty result after truncating TopK.
	if len(parents) != len(ids) {
		return nil, nil, access.ErrNextcloudPublicationUnavailable
	}
	seen := map[string]bool{}
	for _, parent := range parents {
		if parent == nil || !selected[parent.KnowledgeID] || seen[parent.KnowledgeID] {
			return nil, nil, access.ErrNextcloudPublicationUnavailable
		}
		seen[parent.KnowledgeID] = true
	}
	bridge, err := s.beginHistoryVectorMaterialBridge(ctx, params, kbID, parents)
	if err != nil {
		return nil, nil, err
	}
	results, err := kb.HydrateHistoryVectorCandidates(bridge.Context(), kbID, candidates)
	if err != nil {
		return nil, nil, err
	}
	results, err = s.rerankHistoryVectorResults(bridge.Context(), bridge, rc, params.Query, results)
	if err != nil {
		return nil, nil, err
	}
	scores := map[string]float64{}
	for _, result := range results {
		if result == nil || !selected[result.KnowledgeID] || math.IsNaN(result.Score) || math.IsInf(result.Score, 0) {
			return nil, nil, access.ErrNextcloudPublicationDenied
		}
		if previous, exists := scores[result.KnowledgeID]; !exists || result.Score > previous {
			scores[result.KnowledgeID] = result.Score
		}
	}
	return bridge.OwnedCandidates(bridge.Context(), scores)
}

// The bridge authorizes every actual original and child snapshot. The common
// model boundary additionally rejects mutation of the already-hydrated input.
func (s *messageService) rerankHistoryVectorResults(
	ctx context.Context,
	bridge *historyVectorMaterialBridge,
	rc *types.RetrievalConfig,
	query string,
	results []*types.SearchResult,
) ([]*types.SearchResult, error) {
	if bridge == nil || bridge.service != s {
		return nil, access.ErrNextcloudPublicationUnavailable
	}
	documents, err := bridge.modelDocuments(results)
	if err != nil {
		return nil, err
	}
	check := func() error {
		if err := bridge.Checkpoint(ctx); err != nil {
			return err
		}
		_, err := bridge.modelDocuments(results)
		return err
	}
	load := func(ctx context.Context, id string) (rerank.Reranker, error) {
		if s.modelService == nil {
			return nil, access.ErrNextcloudPublicationUnavailable
		}
		return s.modelService.GetRerankModel(ctx, id)
	}
	return runHistoryVectorRerank(ctx, rc, query, results, documents, check, load)
}

// This is the production model boundary, separated for deterministic tests
// of provider stalls/revocation/lookup mutation without creating fake proof.
// A provider failure is explicit; hybrid callers must not claim a full empty
// vector search or silently downgrade to keyword success.
func runHistoryVectorRerank(
	ctx context.Context,
	rc *types.RetrievalConfig,
	query string,
	results []*types.SearchResult,
	documents []string,
	current func() error,
	load func(context.Context, string) (rerank.Reranker, error),
) ([]*types.SearchResult, error) {
	if current == nil || ctx == nil || len(results) != len(documents) || len(results) > maxRetrievalPoolSize {
		return nil, access.ErrNextcloudPublicationUnavailable
	}
	ctx = logprivacy.SuppressContent(ctx)
	inputDigests := make([]string, len(results))
	for i, result := range results {
		if result == nil || result.Content != documents[i] {
			return nil, repository.ErrKnowledgeInputChanged
		}
		digest, err := historyVectorResultDigest(result)
		if err != nil {
			return nil, err
		}
		inputDigests[i] = digest
	}
	checkpoint := func() error {
		if ctx.Err() != nil {
			return historyVectorCancellationError(ctx)
		}
		if err := current(); err != nil {
			return err
		}
		for i, result := range results {
			digest, err := historyVectorResultDigest(result)
			if err != nil {
				return err
			}
			if digest != inputDigests[i] || result.Content != documents[i] {
				return repository.ErrKnowledgeInputChanged
			}
		}
		return nil
	}
	copyResult := func(result *types.SearchResult) (*types.SearchResult, error) {
		copied, err := cloneCollection(result)
		if err != nil {
			return nil, err
		}
		// These fields are intentionally outside JSON. Keep the exact
		// original provenance/flags; never replace them with provider text.
		copied.SourceLineage = result.SourceLineage
		copied.ContentRevision = result.ContentRevision
		copied.ContentRewritten = result.ContentRewritten
		return copied, nil
	}
	if err := checkpoint(); err != nil {
		return nil, err
	}
	if len(results) == 0 || rc == nil || rc.RerankModelID == "" {
		out := make([]*types.SearchResult, 0, len(results))
		for _, result := range results {
			copied2, err := copyResult(result)
			if err != nil {
				return nil, err
			}
			out = append(out, copied2)
		}
		if err := checkpoint(); err != nil {
			return nil, err
		}
		return out, nil
	}
	if load == nil {
		return nil, access.ErrNextcloudPublicationUnavailable
	}
	// A model lookup may block or itself cross a remote boundary. Both
	// its success and error are followed by a current/original checkpoint.
	model, lookupErr := load(ctx, rc.RerankModelID)
	if err := checkpoint(); err != nil {
		return nil, err
	}
	if lookupErr != nil || model == nil {
		return nil, access.ErrNextcloudPublicationUnavailable
	}
	if err := checkpoint(); err != nil {
		return nil, err
	}
	ranks, rankErr := model.Rerank(ctx, query, append([]string{}, documents...))
	if err := checkpoint(); err != nil {
		return nil, err
	}
	if rankErr != nil {
		return nil, access.ErrNextcloudPublicationUnavailable
	}
	threshold := rc.GetEffectiveRerankThreshold()
	if math.IsNaN(threshold) || math.IsInf(threshold, 0) {
		return nil, access.ErrNextcloudPublicationUnavailable
	}
	// Index and score are the only accepted provider fields. Reject the
	// whole malformed response so invalid indexes cannot panic or hide hits.
	seen := map[int]bool{}
	for _, rank := range ranks {
		if rank.Index < 0 || rank.Index >= len(results) || seen[rank.Index] || math.IsNaN(rank.RelevanceScore) ||
			math.IsInf(rank.RelevanceScore, 0) {
			return nil, access.ErrNextcloudPublicationUnavailable
		}
		seen[rank.Index] = true
	}
	sort.SliceStable(ranks, func(i, j int) bool { return ranks[i].RelevanceScore > ranks[j].RelevanceScore })
	limit := min(rc.GetEffectiveRerankTopK(), historySearchMaximumResults)
	out := make([]*types.SearchResult, 0, min(limit, len(ranks)))
	for _, rank := range ranks {
		if rank.RelevanceScore < threshold {
			continue
		}
		copied3, err := copyResult(results[rank.Index])
		if err != nil {
			return nil, err
		}
		copied3.Score = rank.RelevanceScore
		out = append(out, copied3)
		if len(out) == limit {
			break
		}
	}
	if err := checkpoint(); err != nil {
		return nil, err
	}
	return out, nil
}
