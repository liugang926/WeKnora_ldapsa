//go:build sqlite_fts5

package service

import (
	"testing"

	"github.com/Tencent/WeKnora/internal/application/readlease"
	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
)

func TestProcessedSummarySnapshotRequiresPersistedProducerReceipt(t *testing.T) {
	runDocumentGraphPipeline(t, "", func(m *machinePipelineFixture, knowledge *types.Knowledge) {
		var summary types.Chunk
		require.NoError(
			t,
			m.f.db.Where("knowledge_id=? AND chunk_type=?", knowledge.ID, types.ChunkTypeSummary).Take(&summary).Error,
		)
		guard, err := readlease.BeginNextcloudKBRead(
			m.f.ctx,
			m.f.store.ContentLeaseStore(),
			[]readlease.NextcloudKBReadScope{{TenantID: 7, KBID: knowledge.KnowledgeBaseID}},
		)
		require.NoError(t, err)
		defer func() { require.NoError(t, guard.Close()) }()
		require.NoError(t, guard.PinKnowledge(knowledge))
		lease, err := guard.ExactLease(knowledge)
		require.NoError(t, err)
		requests := []types.RAGChunkReadRequest{
			{
				TenantID:        7,
				KnowledgeBaseID: knowledge.KnowledgeBaseID,
				KnowledgeID:     knowledge.ID,
				ChunkIDs:        []string{summary.ID},
				Lease:           lease,
			},
		}
		reader := repository.NewRAGChunkSnapshotReader(m.f.db, m.f.store)
		rows, err := reader.HydrateRAGChunkSnapshots(guard.Context(), requests)
		require.NoError(t, err)
		require.Len(t, rows, 1)
		require.True(t, rows[0].ProducerMaterialVerified)
		require.NoError(t, rows[0].SourceLineage.RequireComplete())
		require.Equal(t, summary.Content, rows[0].Chunk.Content)
		// A current publication pin is not the summary's original producer proof.
		legacy, err := repository.NewRAGChunkSnapshotReader(m.f.db, nil).
			HydrateRAGChunkSnapshots(guard.Context(), requests)
		require.NoError(t, err)
		require.Len(t, legacy, 1)
		require.NoError(t, legacy[0].SourceLineage.RequireComplete())
		require.False(t, legacy[0].ProducerMaterialVerified)
		// Losing the actual receipt cannot be repaired by current source identity.
		deleted := m.f.db.Exec(
			"DELETE FROM knowledge_input_material_receipts WHERE row_kind='chunk' AND row_id=?",
			summary.ID,
		)
		require.NoError(t, deleted.Error)
		require.EqualValues(t, 1, deleted.RowsAffected)
		missing, err := reader.HydrateRAGChunkSnapshots(guard.Context(), requests)
		if err != nil {
			require.Nil(t, missing)
		} else {
			require.Len(t, missing, 1)
			require.False(t, missing[0].ProducerMaterialVerified)
			require.Error(t, missing[0].SourceLineage.RequireComplete())
		}
	})
}
