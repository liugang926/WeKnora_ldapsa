package postgres

import (
	"context"
	"fmt"
	"math"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/pgvector/pgvector-go"
)

// RetrieveVectorCandidates deliberately selects no embedding content. Both
// sides of the nearest-neighbor query project only native control IDs/score.
// Exact tenant/KB/owned knowledge filters apply before LIMIT. Missing control
// tables (e.g. a separate remote vector database) refuse this optional port.
func (g *pgRepository) RetrieveVectorCandidates(ctx context.Context, p types.VectorCandidateParams) ([]*types.VectorIndexCandidate, error) {
	if err := types.ValidateVectorCandidateParams(ctx, p); err != nil {
		return nil, err
	}
	if !g.db.Migrator().HasTable("knowledges") || !g.db.Migrator().HasTable("knowledge_bases") {
		return nil, types.ErrVectorCandidateRetrievalUnavailable
	}
	dim := len(p.Embedding)
	query := fmt.Sprintf(`SELECT candidates.id, candidates.chunk_id, candidates.knowledge_id, candidates.knowledge_base_id, (1-candidates.distance) AS score
		FROM (
			SELECT e.id,e.chunk_id,e.knowledge_id,e.knowledge_base_id,
				e.embedding::halfvec(%[1]d) <=> ?::halfvec(%[1]d) AS distance
			FROM embeddings e
			JOIN knowledges kg ON kg.id=e.knowledge_id AND kg.knowledge_base_id=e.knowledge_base_id
			JOIN knowledge_bases kb ON kb.id=kg.knowledge_base_id AND kb.tenant_id=kg.tenant_id
			WHERE e.dimension=? AND kg.tenant_id=? AND kb.tenant_id=?
			AND kg.deleted_at IS NULL AND kb.deleted_at IS NULL
			AND kb.id IN ? AND kg.id IN ? AND kg.parse_status=? AND kg.enable_status=?
			AND (e.is_enabled IS NULL OR e.is_enabled=?)
			ORDER BY e.embedding::halfvec(%[1]d) <=> ?::halfvec(%[1]d), e.id ASC LIMIT ?
		) candidates ORDER BY candidates.distance ASC,candidates.id ASC`, dim)
	vector := pgvector.NewHalfVector(p.Embedding)
	var rows []struct {
		ID                                    uint
		ChunkID, KnowledgeID, KnowledgeBaseID string
		Score                                 float64
	}
	if err := g.db.WithContext(ctx).Raw(query, vector, dim, p.TenantID, p.TenantID, p.KnowledgeBaseIDs, p.KnowledgeIDs, types.ParseStatusCompleted, "enabled", true, vector, p.TopK).Scan(&rows).Error; err != nil {
		return nil, types.ErrVectorCandidateRetrievalUnavailable
	}
	out := make([]*types.VectorIndexCandidate, 0, len(rows))
	for _, row := range rows {
		if math.IsNaN(row.Score) || math.IsInf(row.Score, 0) {
			return nil, types.ErrVectorCandidateRetrievalUnavailable
		}
		if p.Threshold > 0 && row.Score < p.Threshold {
			continue
		}
		out = append(out, &types.VectorIndexCandidate{IndexID: fmt.Sprintf("%d", row.ID), ChunkID: row.ChunkID, KnowledgeID: row.KnowledgeID, KnowledgeBaseID: row.KnowledgeBaseID, Score: row.Score})
	}
	return out, nil
}

var _ interfaces.NativeVectorCandidateRetriever = (*pgRepository)(nil)
