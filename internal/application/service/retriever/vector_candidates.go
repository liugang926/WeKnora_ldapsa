package retriever

import (
	"context"
	"fmt"
	"slices"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
)

// RetrieveVectorCandidates retrieves candidate identities before original material hydration.
func (v *KeywordsVectorHybridRetrieveEngineService) RetrieveVectorCandidates(
	ctx context.Context,
	params types.VectorCandidateParams,
) ([]*types.VectorIndexCandidate, error) {
	if err := types.ValidateVectorCandidateParams(ctx, params); err != nil {
		return nil, err
	}
	native, ok := v.indexRepository.(interfaces.NativeVectorCandidateRetriever)
	if !ok {
		return nil, types.ErrVectorCandidateRetrievalUnavailable
	}
	return native.RetrieveVectorCandidates(ctx, params)
}

// RetrieveVectorCandidates follows the same selected vector backend as
// Retrieve. Missing positive native capability is terminal: it never calls
// the old content-bearing method, including after a native query error.
func (c *CompositeRetrieveEngine) RetrieveVectorCandidates(
	ctx context.Context,
	params []types.VectorCandidateParams,
) ([]*types.VectorCandidateResult, error) {
	var results []*types.VectorCandidateResult
	for _, param := range params {
		if err := types.ValidateVectorCandidateParams(ctx, param); err != nil {
			return nil, err
		}
		found := false
		for _, info := range c.engineInfos {
			if info == nil || !slices.Contains(info.retrieverType, types.VectorRetrieverType) {
				continue
			}
			native, ok := info.retrieveEngine.(interfaces.NativeVectorCandidateRetriever)
			if !ok {
				return nil, types.ErrVectorCandidateRetrievalUnavailable
			}
			rows, err := native.RetrieveVectorCandidates(ctx, param)
			if err != nil {
				return nil, err
			}
			results = append(
				results,
				&types.VectorCandidateResult{RetrieverEngineType: info.retrieveEngine.EngineType(), Candidates: rows},
			)
			found = true
			break
		}
		if !found {
			return nil, fmt.Errorf("%w: vector backend not selected", types.ErrVectorCandidateRetrievalUnavailable)
		}
	}
	return results, nil
}

var _ interfaces.NativeVectorCandidateRetriever = (*KeywordsVectorHybridRetrieveEngineService)(nil)
