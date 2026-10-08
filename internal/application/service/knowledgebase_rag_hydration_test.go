package service

import (
	"context"
	"errors"
	"testing"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/stretchr/testify/require"
)

type ragHydrationKnowledgeRepo struct {
	interfaces.KnowledgeRepository
	knowledge *types.Knowledge
}

func (r *ragHydrationKnowledgeRepo) GetKnowledgeBatch(context.Context, uint64, []string) ([]*types.Knowledge, error) {
	return []*types.Knowledge{r.knowledge}, nil
}

type ragHydrationActualReader struct {
	request []types.RAGChunkReadRequest
	lineage *types.SourceLineage
}

func (r *ragHydrationActualReader) HydrateRAGChunkSnapshots(_ context.Context, requests []types.RAGChunkReadRequest) ([]types.RAGChunkInputSnapshot, error) {
	r.request = requests
	return []types.RAGChunkInputSnapshot{{Knowledge: &types.Knowledge{ID: "doc", TenantID: 7, KnowledgeBaseID: "kb", Title: "actual snapshot header", Channel: "web", EnableStatus: "enabled", ParseStatus: types.ParseStatusCompleted},
		Chunk: &types.Chunk{ID: "chunk", TenantID: 7, KnowledgeBaseID: "kb", KnowledgeID: "doc", Content: "actual snapshot body", ChunkType: types.ChunkTypeText, ImageInfo: "[]", IsEnabled: true, IndexStatus: "ready"}, SourceLineage: r.lineage}}, nil
}

func ragHydrationServiceFixture(t *testing.T, lineage *types.SourceLineage, authorize func(context.Context, *types.SourceLineage) error) (*knowledgeBaseService, context.Context, *ragHydrationActualReader) {
	t.Helper()
	ctx := types.WithCaller(context.Background(), types.Caller{TenantID: 7, UserID: "person"})
	ctx = types.WithExecutionTenant(ctx, 7)
	ctx = types.WithPrincipal(ctx, types.Principal{Type: types.PrincipalWebUser, ID: "person"})
	ctx = types.WithRAGSourceLineage(ctx, types.NewRAGSourceLineage(authorize))
	reader := &ragHydrationActualReader{lineage: lineage}
	s := &knowledgeBaseService{kgRepo: &ragHydrationKnowledgeRepo{knowledge: &types.Knowledge{ID: "doc", TenantID: 7, KnowledgeBaseID: "kb", Title: "old cached metadata", Channel: "web"}}, ragChunkSnapshots: reader,
		searchPublicationCheck: func(context.Context, *types.Knowledge) error { return nil }}
	return s, ctx, reader
}

func TestRAGChunkHydrationIgnoresCachedIndexTextAndAccountsBeforeResult(t *testing.T) {
	lineage, err := types.NewCompleteSourceLineage()
	require.NoError(t, err)
	checks := 0
	s, ctx, reader := ragHydrationServiceFixture(t, lineage, func(_ context.Context, actual *types.SourceLineage) error {
		checks++
		require.NoError(t, actual.RequireComplete())
		return nil
	})
	rows, err := s.processSearchResults(ctx, []*types.IndexWithScore{{KnowledgeID: "doc", ChunkID: "chunk", Content: "old index text must not be restamped", Score: 1}}, true)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, "actual snapshot body", rows[0].Content)
	require.Equal(t, "actual snapshot header", rows[0].KnowledgeTitle)
	require.Empty(t, rows[0].MatchedContent)
	require.Equal(t, lineage, rows[0].SourceLineage)
	require.Equal(t, []string{"chunk"}, reader.request[0].ChunkIDs)
	require.Positive(t, checks, "permission check must precede building metadata/progress results")
}

func TestRAGChunkHydrationUnknownAndLateRevocationReturnNoProgressMaterial(t *testing.T) {
	denied := errors.New("permission revoked after capture")
	for _, mode := range []string{"legacy unknown", "late revocation"} {
		t.Run(mode, func(t *testing.T) {
			lineage := types.UnknownSourceLineage()
			if mode == "late revocation" {
				var err error
				lineage, err = types.NewCompleteSourceLineage()
				require.NoError(t, err)
			}
			s, ctx, _ := ragHydrationServiceFixture(t, lineage, func(context.Context, *types.SourceLineage) error { return denied })
			rows, err := s.processSearchResults(ctx, []*types.IndexWithScore{{KnowledgeID: "doc", ChunkID: "chunk", Content: "cached text", Score: 1}}, true)
			require.Error(t, err)
			require.Nil(t, rows)
			if mode == "late revocation" {
				require.ErrorIs(t, err, denied)
			} else {
				require.ErrorIs(t, err, types.ErrSourceLineageUnknown)
			}
		})
	}
}
