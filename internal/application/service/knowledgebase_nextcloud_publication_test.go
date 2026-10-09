package service

import (
	"testing"

	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
)

// A stale vector hit must never hydrate a withdrawn Nextcloud document into a
// RAG result, even while its knowledge and chunk rows are awaiting cleanup.
func TestSearchResultAssemblyOmitsUnverifiableNextcloudPublication(t *testing.T) {
	for _, tc := range []struct {
		name  string
		guard *access.NextcloudPublicationGuard
	}{
		{name: "source authorization unavailable", guard: access.NewNextcloudPublicationGuard(nil, nil)},
		{name: "guard provider missing"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			knowledgeService, db := newKnowledgeSharedAccessService(t, nil)
			require.NoError(t, db.AutoMigrate(&types.Chunk{}))
			seedKnowledge(t, db, &types.Knowledge{
				ID: "withdrawn", TenantID: 1, KnowledgeBaseID: "kb-1", Type: "file",
				Channel: types.ConnectorTypeNextcloud, Title: "withdrawn title",
				Metadata: types.JSON(`{"datasource_id":"source-1","nextcloud_file_id":"42"}`),
			})
			seedKnowledge(t, db, &types.Knowledge{
				ID: "ordinary", TenantID: 1, KnowledgeBaseID: "kb-1", Type: "file",
				Channel: "web", Title: "ordinary title",
			})
			for _, id := range []string{"withdrawn", "ordinary"} {
				require.NoError(t, db.Create(&types.Chunk{
					ID: id + "-chunk", TenantID: 1, KnowledgeBaseID: "kb-1", KnowledgeID: id,
					Content: id + " secret", ChunkType: types.ChunkTypeText, IsEnabled: true,
					IndexStatus: "ready", ImageInfo: "[]",
				}).Error)
			}
			svc := &knowledgeBaseService{
				kgRepo: knowledgeService.repo, chunkRepo: repository.NewChunkRepository(db),
				publicationGuard: tc.guard,
			}
			results, err := svc.processSearchResults(newSharedAccessContext(), []*types.IndexWithScore{
				{ChunkID: "withdrawn-chunk", KnowledgeID: "withdrawn", Score: 0.99},
				{ChunkID: "ordinary-chunk", KnowledgeID: "ordinary", Score: 0.98},
			}, true)
			require.NoError(t, err)
			require.Len(t, results, 1)
			require.Equal(t, "ordinary", results[0].KnowledgeID)
			require.Equal(t, "ordinary secret", results[0].Content)
		})
	}
}
