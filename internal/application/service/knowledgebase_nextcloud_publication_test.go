package service

import (
	"testing"

	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
)

// Strict reads omit an index hit with an incomplete source identity. Workspace
// reads must still check the KB grant for each hit.
func TestSearchResultAssemblyOmitsUnverifiableNextcloudPublication(t *testing.T) {
	for _, tc := range []struct {
		name  string
		guard *access.NextcloudPublicationGuard
	}{
		{name: "source authorization unavailable", guard: access.NewNextcloudPublicationGuard(nil, nil)},
		{name: "guard provider missing"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("WEKNORA_NEXTCLOUD_READ_AUTHORITY", "")
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
			hits := []*types.IndexWithScore{
				{ChunkID: "withdrawn-chunk", KnowledgeID: "withdrawn", Score: 0.99},
				{ChunkID: "ordinary-chunk", KnowledgeID: "ordinary", Score: 0.98},
			}
			results, err := svc.processSearchResults(newSharedAccessContext(), hits, true)
			require.NoError(t, err)
			require.Len(t, results, 1)
			require.Equal(t, "ordinary", results[0].KnowledgeID)
			require.Equal(t, "ordinary secret", results[0].Content)

			t.Setenv("WEKNORA_NEXTCLOUD_READ_AUTHORITY", "weknora")
			results, err = svc.processSearchResults(newSharedAccessContext(), hits, true)
			require.NoError(t, err)
			require.Len(t, results, 2)
			require.Equal(t, "withdrawn secret", results[0].Content)
			otherTenant := types.WithCaller(newSharedAccessContext(), types.Caller{TenantID: 2, UserID: "other"})
			results, err = svc.processSearchResults(otherTenant, hits, true)
			require.NoError(t, err)
			require.Empty(t, results)
		})
	}
}
