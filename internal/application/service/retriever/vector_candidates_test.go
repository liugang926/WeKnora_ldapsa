package retriever

import (
	"context"
	"testing"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/stretchr/testify/require"
)

type oldBodyVectorRepository struct {
	interfaces.RetrieveEngineRepository
	BodyReads int
}

func (r *oldBodyVectorRepository) Retrieve(context.Context, types.RetrieveParams) ([]*types.RetrieveResult, error) {
	r.BodyReads++
	return nil, nil
}

func TestCompositeVectorCandidatesUnsupportedNeverCallsBodyRetriever(t *testing.T) {
	repo := &oldBodyVectorRepository{}
	engine := &KeywordsVectorHybridRetrieveEngineService{indexRepository: repo, engineType: types.MilvusRetrieverEngineType}
	composite := &CompositeRetrieveEngine{engineInfos: []*engineInfo{{retrieveEngine: engine, retrieverType: []types.RetrieverType{types.VectorRetrieverType}}}}
	ctx := types.WithExecutionTenant(context.Background(), 7)
	p := types.VectorCandidateParams{TenantID: 7, KnowledgeBaseIDs: []string{"kb"}, KnowledgeIDs: []string{"owned"}, Embedding: []float32{1, 0}, TopK: 1}
	rows, err := composite.RetrieveVectorCandidates(ctx, []types.VectorCandidateParams{p})
	require.ErrorIs(t, err, types.ErrVectorCandidateRetrievalUnavailable)
	require.Nil(t, rows)
	require.Zero(t, repo.BodyReads)
}

type candidateVectorRepository struct {
	oldBodyVectorRepository
	CandidateReads int
	Fail           bool
}

func (r *candidateVectorRepository) RetrieveVectorCandidates(context.Context, types.VectorCandidateParams) ([]*types.VectorIndexCandidate, error) {
	r.CandidateReads++
	if r.Fail {
		return nil, types.ErrVectorCandidateRetrievalUnavailable
	}
	return []*types.VectorIndexCandidate{{IndexID: "1", ChunkID: "chunk", KnowledgeID: "owned", KnowledgeBaseID: "kb", Score: 0.9}}, nil
}

func TestCompositeVectorCandidatesNativeSuccessAndErrorNeverFallbackBody(t *testing.T) {
	for _, fail := range []bool{false, true} {
		repo := &candidateVectorRepository{Fail: fail}
		engine := &KeywordsVectorHybridRetrieveEngineService{indexRepository: repo, engineType: types.SQLiteRetrieverEngineType}
		composite := &CompositeRetrieveEngine{engineInfos: []*engineInfo{{retrieveEngine: engine, retrieverType: []types.RetrieverType{types.VectorRetrieverType}}}}
		ctx := types.WithExecutionTenant(context.Background(), 7)
		p := types.VectorCandidateParams{TenantID: 7, KnowledgeBaseIDs: []string{"kb"}, KnowledgeIDs: []string{"owned"}, Embedding: []float32{1, 0}, TopK: 1}
		rows, err := composite.RetrieveVectorCandidates(ctx, []types.VectorCandidateParams{p})
		if fail {
			require.ErrorIs(t, err, types.ErrVectorCandidateRetrievalUnavailable)
			require.Nil(t, rows)
		} else {
			require.NoError(t, err)
			require.Len(t, rows, 1)
			require.Len(t, rows[0].Candidates, 1)
			require.Equal(t, "owned", rows[0].Candidates[0].KnowledgeID)
		}
		require.Equal(t, 1, repo.CandidateReads)
		require.Zero(t, repo.BodyReads)
	}
}
