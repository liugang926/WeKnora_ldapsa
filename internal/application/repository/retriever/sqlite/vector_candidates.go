package sqlite

import (
	"context"
	"fmt"
	"math"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	sqlite_vec "github.com/asg017/sqlite-vec-go-bindings/cgo"
)

// RetrieveVectorCandidates supports only a native index sharing the control
// database. A remote/native store without current KB/knowledge control tables
// is unavailable, never a reason to read its content-bearing Retrieve path.
func (r *sqliteRepository) RetrieveVectorCandidates(
	ctx context.Context,
	p types.VectorCandidateParams,
) ([]*types.VectorIndexCandidate, error) {
	if err := types.ValidateVectorCandidateParams(ctx, p); err != nil {
		return nil, err
	}
	if r.initErr != nil {
		return nil, types.ErrVectorCandidateRetrievalUnavailable
	}
	queryBlob, err := sqlite_vec.SerializeFloat32(p.Embedding)
	if err != nil {
		return nil, types.ErrVectorCandidateRetrievalUnavailable
	}
	// This read port does not provision a new dimension table or index.
	table := vecTableName(len(p.Embedding))
	if !r.db.Migrator().HasTable(table) || !r.db.Migrator().HasTable("knowledges") ||
		!r.db.Migrator().HasTable("knowledge_bases") {
		return nil, types.ErrVectorCandidateRetrievalUnavailable
	}
	query := fmt.Sprintf(`SELECT v.rowid, v.distance, e.chunk_id, e.knowledge_id, e.knowledge_base_id
		FROM %s v JOIN lite_embeddings e ON e.id=v.rowid
		WHERE v.embedding MATCH ? AND k=? AND v.rowid IN (
			SELECT filtered.id FROM lite_embeddings filtered
			JOIN knowledges kg ON kg.id=filtered.knowledge_id AND kg.knowledge_base_id=filtered.knowledge_base_id
			JOIN knowledge_bases kb ON kb.id=kg.knowledge_base_id AND kb.tenant_id=kg.tenant_id
			WHERE kg.tenant_id=? AND kb.tenant_id=? AND kg.deleted_at IS NULL AND kb.deleted_at IS NULL
			AND kb.id IN ? AND kg.id IN ? AND kg.parse_status=? AND kg.enable_status=?
			AND (filtered.is_enabled IS NULL OR filtered.is_enabled=1)
		) ORDER BY v.distance ASC`, table)
	var rows []struct {
		Rowid                                 uint
		Distance                              float64
		ChunkID, KnowledgeID, KnowledgeBaseID string
	}
	if err := r.db.WithContext(ctx).
		Raw(query, queryBlob, p.TopK, p.TenantID, p.TenantID, p.KnowledgeBaseIDs, p.KnowledgeIDs, types.ParseStatusCompleted, "enabled").
		Scan(&rows).
		Error; err != nil {
		return nil, types.ErrVectorCandidateRetrievalUnavailable
	}
	out := make([]*types.VectorIndexCandidate, 0, len(rows))
	for _, row := range rows {
		score := 1 - row.Distance
		if math.IsNaN(score) || math.IsInf(score, 0) {
			return nil, types.ErrVectorCandidateRetrievalUnavailable
		}
		if p.Threshold > 0 && score < p.Threshold {
			continue
		}
		out = append(
			out,
			&types.VectorIndexCandidate{
				IndexID:         fmt.Sprintf("%d", row.Rowid),
				ChunkID:         row.ChunkID,
				KnowledgeID:     row.KnowledgeID,
				KnowledgeBaseID: row.KnowledgeBaseID,
				Score:           score,
			},
		)
	}
	return out, nil
}

var _ interfaces.NativeVectorCandidateRetriever = (*sqliteRepository)(nil)
