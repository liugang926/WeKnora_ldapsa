package repository

import (
	"context"
	"testing"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

func chunkOutputInputCases(t *testing.T, fixture func(*testing.T) (*KnowledgeInputProvenanceStore, *gorm.DB)) {
	for _, change := range []string{"positive", "legacy nonmatching", "candidate addition", "candidate deletion", "content ABA", "wire field tamper", "graph projection"} {
		t.Run(change, func(t *testing.T) {
			store, db := fixture(t)
			ctx, k, c, _ := createAdmittedManual(t, store, db)
			request := types.ChunkOutputReadRequest{TenantID: k.TenantID, KnowledgeBaseID: k.KnowledgeBaseID, KnowledgeID: k.ID, AllChunks: true}
			snapshot, err := store.CaptureChunkOutputInputs(context.Background(), request)
			require.NoError(t, err)
			require.Len(t, snapshot.Chunks, 1)
			require.Equal(t, c.Content, snapshot.Chunks[0].Content)
			require.NoError(t, store.RecheckChunkOutputInputs(context.Background(), snapshot))
			switch change {
			case "positive":
				return
			case "legacy nonmatching":
				require.NoError(t, NewChunkRepository(db).CreateChunks(context.Background(), []*types.Chunk{{ID: "unproved-image", TenantID: k.TenantID, KnowledgeBaseID: k.KnowledgeBaseID, KnowledgeID: k.ID, ChunkType: types.ChunkTypeImageCaption, Content: "orphan controlled body", IsEnabled: true}}))
				_, err = store.CaptureChunkOutputInputs(context.Background(), request)
				require.Error(t, err)
				return
			case "candidate addition":
				require.NoError(t, NewChunkRepository(db).CreateChunks(ctx, []*types.Chunk{{ID: "new-original-row", TenantID: k.TenantID, KnowledgeBaseID: k.KnowledgeBaseID, KnowledgeID: k.ID, ChunkType: types.ChunkTypeText, Content: "new admitted body", IsEnabled: true}}))
			case "candidate deletion":
				require.NoError(t, db.Unscoped().Where("id = ?", c.ID).Delete(&types.Chunk{}).Error)
			case "content ABA":
				require.NoError(t, db.Model(c).Update("content", "changed").Error)
				require.NoError(t, db.Model(c).Update("content", c.Content).Error)
			case "wire field tamper":
				snapshot.Chunks[0].ImageInfo = "unaccounted raw image output"
			case "graph projection":
				snapshot.Proof.Projection = types.KnowledgeBuildProjectionGraph
			}
			require.Error(t, store.RecheckChunkOutputInputs(context.Background(), snapshot))
		})
	}
}
func TestChunkOutputInputSQLiteOriginalBodyAndCompleteCandidateMembership(t *testing.T) {
	chunkOutputInputCases(t, knowledgeInputFixture)
}
func TestChunkOutputInputPostgresOriginalBodyAndCompleteCandidateMembership(t *testing.T) {
	chunkOutputInputCases(t, knowledgeInputPostgresFixture)
}
