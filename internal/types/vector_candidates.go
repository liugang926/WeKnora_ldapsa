package types

import (
	"context"
	"errors"
	"math"
)

var ErrVectorCandidateRetrievalUnavailable = errors.New("native vector candidate retrieval unavailable")
var ErrVectorCandidateScope = errors.New("invalid native vector candidate scope")

// VectorCandidateParams is private server state. TenantID and both nonempty
// selector sets must come from current authorized KB rows and owned-message
// control metadata. UUID knowledge selectors are not authorization receipts.
// The native same-database adapters recheck tenant/KB/knowledge control joins;
// original message/source authorization still precedes every content read.
type VectorCandidateParams struct {
	TenantID         uint64
	KnowledgeBaseIDs []string
	KnowledgeIDs     []string
	Embedding        []float32
	TopK             int
	Threshold        float64
}

// VectorIndexCandidate has no content, title, filename, metadata JSON or other
// body field. Implementations must project this shape in the native query,
// rather than calling ordinary Retrieve and erasing already-consumed text.
type VectorIndexCandidate struct {
	IndexID         string
	ChunkID         string
	KnowledgeID     string
	KnowledgeBaseID string
	Score           float64
}

type VectorCandidateResult struct {
	RetrieverEngineType RetrieverEngineType
	Candidates          []*VectorIndexCandidate
}

func ValidateVectorCandidateParams(ctx context.Context, p VectorCandidateParams) error {
	tenant, ok := TenantIDFromContext(ctx)
	if !ok || tenant != p.TenantID || p.TenantID == 0 || p.TenantID > math.MaxInt64 ||
		len(p.KnowledgeBaseIDs) == 0 || len(p.KnowledgeBaseIDs) > 100 ||
		len(p.KnowledgeIDs) == 0 || len(p.KnowledgeIDs) > 16384 ||
		len(p.Embedding) == 0 || len(p.Embedding) > 4096 || p.TopK <= 0 || p.TopK > 500 ||
		math.IsNaN(p.Threshold) || math.IsInf(p.Threshold, 0) || p.Threshold < -1 || p.Threshold > 1 {
		return ErrVectorCandidateScope
	}
	for _, ids := range [][]string{p.KnowledgeBaseIDs, p.KnowledgeIDs} {
		seen := make(map[string]bool, len(ids))
		for _, id := range ids {
			if id == "" || len(id) > 128 || seen[id] {
				return ErrVectorCandidateScope
			}
			seen[id] = true
		}
	}
	for _, value := range p.Embedding {
		if math.IsNaN(float64(value)) || math.IsInf(float64(value), 0) {
			return ErrVectorCandidateScope
		}
	}
	return nil
}
