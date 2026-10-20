package service

import (
	"context"
	"math"
	"sort"

	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/application/readlease"
	"github.com/Tencent/WeKnora/internal/logprivacy"
	"github.com/Tencent/WeKnora/internal/types"
)

// historyVectorKnowledgeService is deliberately optional. Missing native
// capability is an error, never permission to use content-bearing Retrieve.
type historyVectorKnowledgeService interface {
	SearchHistoryVectorCandidates(
		context.Context,
		string,
		[]string,
		types.SearchParams,
	) ([]*types.VectorIndexCandidate, error)
	HydrateHistoryVectorCandidates(
		context.Context,
		string,
		[]*types.VectorIndexCandidate,
	) ([]*types.SearchResult, error)
}

type historyNativeCompositeCandidates interface {
	RetrieveVectorCandidates(context.Context, []types.VectorCandidateParams) ([]*types.VectorCandidateResult, error)
}

// historyVectorReadContext always transfers a real broad KB fence to the
// output owner. It protects ordinary history children as well as source-backed
// children; acquiring a child snapshot itself does not acquire a body lease.
func (s *knowledgeBaseService) historyVectorReadContext(
	ctx context.Context,
	tenant uint64,
	kbID string,
) (context.Context, *readlease.NextcloudReadGuard, error) {
	if ctx == nil || ctx.Err() != nil || s.contentLeases == nil {
		return ctx, nil, access.ErrNextcloudPublicationUnavailable
	}
	slot, ok := ctx.Value(messageMaterialOutputKey{}).(*messageMaterialOutputSlot)
	if !ok || slot == nil {
		return ctx, nil, access.ErrNextcloudPublicationUnavailable
	}
	scopes := []readlease.NextcloudKBReadScope{{TenantID: tenant, KBID: kbID}}
	guard, _ := ragTurnPhysicalReadGuard(ctx, scopes)
	if guard == nil || !guard.Covers(scopes) {
		var err error
		guard, err = readlease.BeginNextcloudKBRead(ragPhysicalReadLifetimeContext(ctx), s.contentLeases, scopes)
		if err != nil {
			return ctx, nil, err
		}
		held, err := holdRAGPhysicalReadGuard(ctx, guard)
		if err != nil || !held {
			_ = guard.Close()
			if err == nil {
				err = access.ErrNextcloudPublicationUnavailable
			}
			return ctx, nil, err
		}
	}
	if err := guard.Verify(); err != nil {
		return ctx, nil, err
	}
	return guard.ContextFor(logprivacy.SuppressContent(ctx)), guard, nil
}

// SearchHistoryVectorCandidates reads native control IDs only. The nonempty
// whitelist comes from immutable history admissions plus currently owned
// sessions. It is a selector, not original/source authorization.
func (s *knowledgeBaseService) SearchHistoryVectorCandidates(
	ctx context.Context,
	kbID string,
	ownedKnowledgeIDs []string,
	params types.SearchParams,
) ([]*types.VectorIndexCandidate, error) {
	if len(ownedKnowledgeIDs) == 0 {
		return []*types.VectorIndexCandidate{}, nil
	}
	tenant, ok := types.TenantIDFromContext(ctx)
	if !ok || tenant == 0 || kbID == "" || len(ownedKnowledgeIDs) > 16384 || params.DisableVectorMatch ||
		s.repo == nil {
		return nil, types.ErrVectorCandidateScope
	}
	for _, id := range params.KnowledgeBaseIDs {
		if id != kbID {
			return nil, types.ErrVectorCandidateScope
		}
	}
	if len(params.TagIDs) > 0 || len(params.ScopeTagIDs) > 0 {
		return nil, types.ErrVectorCandidateScope
	}
	owned := make(map[string]bool, len(ownedKnowledgeIDs))
	ids := make([]string, 0, len(ownedKnowledgeIDs))
	for _, id := range ownedKnowledgeIDs {
		if id == "" || len(id) > 128 {
			return nil, types.ErrVectorCandidateScope
		}
		if !owned[id] {
			owned[id] = true
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	ctx = logprivacy.SuppressContent(ctx)
	kbs, err := s.repo.GetKnowledgeBaseByIDs(ctx, []string{kbID})
	if err != nil {
		return nil, err
	}
	kb := pickPrimary(kbs, kbID)
	if kb == nil || kb.TenantID != tenant {
		return nil, access.ErrNextcloudPublicationDenied
	}
	if err := s.authorizeKBAccess(ctx, kbs); err != nil {
		return nil, err
	}
	if err := s.checkSearchGroupAccess(ctx, kbs); err != nil {
		return nil, err
	}
	if err := s.validateSameEmbeddingModel(ctx, kbs); err != nil {
		return nil, err
	}
	if kb.Type != types.KnowledgeBaseTypeDocument || !kb.IsVectorEnabled() || kb.EmbeddingModelID == "" ||
		s.retrieveEngine == nil {
		return nil, types.ErrVectorCandidateRetrievalUnavailable
	}
	ctx, guard, err := s.historyVectorReadContext(ctx, tenant, kbID)
	if err != nil {
		return nil, err
	}
	checkpoint := func() error {
		if err := s.authorizeKBAccess(ctx, kbs); err != nil {
			return err
		}
		if err := s.checkSearchGroupAccess(ctx, kbs); err != nil {
			return err
		}
		return guard.Verify()
	}
	params.MatchCount = min(normalizedMatchCount(params.MatchCount), maxRetrievalPoolSize)
	params.KnowledgeIDs = ids
	params.KnowledgeBaseIDs = []string{kbID}
	params.DisableKeywordsMatch = true
	if err := checkpoint(); err != nil {
		return nil, err
	}
	if len(params.QueryEmbedding) == 0 {
		if s.modelService == nil {
			return nil, types.ErrVectorCandidateRetrievalUnavailable
		}
		params.QueryEmbedding, err = s.GetQueryEmbedding(ctx, kbID, params.QueryText)
		if checkErr := checkpoint(); checkErr != nil {
			return nil, checkErr
		}
		if err != nil {
			return nil, types.ErrVectorCandidateRetrievalUnavailable
		}
	}
	probe := types.VectorCandidateParams{
		TenantID: tenant, KnowledgeBaseIDs: []string{kbID}, KnowledgeIDs: ids,
		Embedding: params.QueryEmbedding, TopK: params.MatchCount, Threshold: params.VectorThreshold,
	}
	if err := types.ValidateVectorCandidateParams(ctx, probe); err != nil {
		return nil, err
	}
	groups, err := s.resolveStoreGroups(ctx, kb, kbs, params, params.MatchCount)
	if checkErr := checkpoint(); checkErr != nil {
		return nil, checkErr
	}
	if err != nil {
		return nil, err
	}
	if len(groups) == 0 {
		return nil, types.ErrVectorCandidateRetrievalUnavailable
	}
	byChunk := map[string]*types.VectorIndexCandidate{}
	for _, group := range groups {
		if group == nil || group.OwnerTenantID != tenant || group.Engine == nil {
			return nil, types.ErrVectorCandidateScope
		}
		vectorConfigured := false
		for _, param := range group.BaseParams {
			vectorConfigured = vectorConfigured || param.RetrieverType == types.VectorRetrieverType
		}
		native, ok := any(group.Engine).(historyNativeCompositeCandidates)
		if !ok || !vectorConfigured {
			return nil, types.ErrVectorCandidateRetrievalUnavailable
		}
		request := probe
		request.KnowledgeBaseIDs = append([]string{}, group.KBIDs...)
		if err := types.ValidateVectorCandidateParams(ctx, request); err != nil {
			return nil, err
		}
		if err := checkpoint(); err != nil {
			return nil, err
		}
		pages, retrieveErr := native.RetrieveVectorCandidates(ctx, []types.VectorCandidateParams{request})
		if err := checkpoint(); err != nil {
			return nil, err
		}
		if retrieveErr != nil {
			return nil, retrieveErr
		}
		for _, page := range pages {
			if page == nil || len(page.Candidates) > request.TopK {
				return nil, types.ErrVectorCandidateRetrievalUnavailable
			}
			for _, row := range page.Candidates {
				if row == nil || row.IndexID == "" || row.ChunkID == "" || !owned[row.KnowledgeID] ||
					row.KnowledgeBaseID != kbID ||
					math.IsNaN(row.Score) ||
					math.IsInf(row.Score, 0) {
					return nil, types.ErrVectorCandidateScope
				}
				key := row.KnowledgeID + "\x00" + row.ChunkID
				if prior := byChunk[key]; prior == nil || row.Score > prior.Score {
					copied := *row
					byChunk[key] = &copied
				}
			}
		}
	}
	rows := make([]*types.VectorIndexCandidate, 0, len(byChunk))
	for _, row := range byChunk {
		rows = append(rows, row)
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].Score != rows[j].Score {
			return rows[i].Score > rows[j].Score
		}
		if rows[i].KnowledgeID != rows[j].KnowledgeID {
			return rows[i].KnowledgeID < rows[j].KnowledgeID
		}
		return rows[i].ChunkID < rows[j].ChunkID
	})
	if len(rows) > params.MatchCount {
		rows = rows[:params.MatchCount]
	}
	if err := checkpoint(); err != nil {
		return nil, err
	}
	return rows, nil
}

// HydrateHistoryVectorCandidates is reachable only after actual parent
// message guards have been obtained and all their plans/lease coverage merged.
func (s *knowledgeBaseService) HydrateHistoryVectorCandidates(
	ctx context.Context,
	kbID string,
	candidates []*types.VectorIndexCandidate,
) ([]*types.SearchResult, error) {
	if ctx == nil {
		return nil, access.ErrNextcloudPublicationUnavailable
	}
	bridge, ok := ctx.Value(historyVectorMaterialBridgeKey{}).(*historyVectorMaterialBridge)
	if !ok || bridge == nil || bridge.kbID != kbID || s.repo == nil || s.ragChunkSnapshots == nil ||
		s.ragChunkLineageGuard == nil {
		return nil, access.ErrNextcloudPublicationUnavailable
	}
	ctx, guard, err := s.historyVectorReadContext(ctx, bridge.tenant, kbID)
	if err != nil {
		return nil, err
	}
	// Child proofs establish original material, not the caller's current
	// KB/group permission. Reauthorize that separately around every model
	// boundary, including ordinary raw-history parents with no file sources.
	if bridge.cancel == nil {
		return nil, access.ErrNextcloudPublicationUnavailable
	}
	stop := context.AfterFunc(guard.Context(), bridge.cancel)
	context.AfterFunc(bridge.Context(), func() { stop() })
	bridge.kbCurrent = func(current context.Context) error {
		kbs, err := s.repo.GetKnowledgeBaseByIDs(current, []string{kbID})
		if err != nil {
			return err
		}
		kb := pickPrimary(kbs, kbID)
		if kb == nil || kb.TenantID != bridge.tenant || kb.Type != types.KnowledgeBaseTypeDocument ||
			!kb.IsVectorEnabled() ||
			kb.EmbeddingModelID == "" {
			return access.ErrNextcloudPublicationDenied
		}
		if err := s.authorizeKBAccess(current, kbs); err != nil {
			return err
		}
		if err := s.checkSearchGroupAccess(current, kbs); err != nil {
			return err
		}
		return guard.Verify()
	}
	if err := bridge.captureChildren(ctx, candidates, guard); err != nil {
		return nil, err
	}
	indices := make([]*types.IndexWithScore, 0, len(candidates))
	for _, candidate := range candidates {
		indices = append(indices, &types.IndexWithScore{
			ID: candidate.IndexID, ChunkID: candidate.ChunkID,
			KnowledgeID: candidate.KnowledgeID, KnowledgeBaseID: candidate.KnowledgeBaseID,
			Score: candidate.Score, MatchType: types.MatchTypeEmbedding, IsEnabled: true,
		})
	}
	if err := bridge.Checkpoint(ctx); err != nil {
		return nil, err
	}
	results, err := s.processSearchResults(ctx, indices, true)
	if checkErr := bridge.Checkpoint(ctx); checkErr != nil {
		return nil, checkErr
	}
	if err != nil {
		return nil, err
	}
	results, err = s.filterSearchResultsAtPublicationBoundary(ctx, results)
	if err != nil {
		return nil, err
	}
	if err := bridge.bindHydratedResults(results); err != nil {
		return nil, err
	}
	if err := holdHistoryVectorMaterialOutput(ctx, bridge); err != nil {
		return nil, err
	}
	if err := bridge.Checkpoint(ctx); err != nil {
		return nil, err
	}
	return results, nil
}
