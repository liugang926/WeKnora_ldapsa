package repository

import (
	"context"
	"encoding/json"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"gorm.io/gorm"
)

type tagCommittedOutputKey struct{}
type tagCommittedOutput struct {
	snapshot  *types.TagOutputInputSnapshot
	err       error
	committed bool
}

// A request-local private slot is populated only from the actual writer SQL
// transaction and published only after commit. Partial edits cannot certify
// retained unknown fields. The slot and proofs never appear in response JSON.
func WithTagCommittedOutput(ctx context.Context) context.Context {
	return context.WithValue(ctx, tagCommittedOutputKey{}, &tagCommittedOutput{})
}
func TagCommittedOutput(ctx context.Context) (*types.TagOutputInputSnapshot, error) {
	slot, ok := ctx.Value(tagCommittedOutputKey{}).(*tagCommittedOutput)
	if !ok || !slot.committed {
		return nil, types.ErrSourceLineageUnknown
	}
	if slot.err != nil {
		return nil, slot.err
	}
	return cloneTagOutput(slot.snapshot)
}
func cloneTagOutput[T any](in T) (T, error) {
	var out T
	b, e := json.Marshal(in)
	if e != nil {
		return out, e
	}
	e = json.Unmarshal(b, &out)
	return out, e
}
func tagOutputEqual(a, b any) bool {
	x, e := json.Marshal(a)
	if e != nil {
		return false
	}
	y, e := json.Marshal(b)
	return e == nil && string(x) == string(y)
}

func captureTagFields(tx *gorm.DB, tags []*types.KnowledgeTag) ([]*types.KnowledgeCollectionAuxiliaryProof, error) {
	ids := make([]string, 0, len(tags))
	for _, tag := range tags {
		ids = append(ids, tag.ID)
	}
	cached, err := cacheKnowledgeDisplayInputs(tx, ids)
	if err != nil {
		return nil, err
	}
	tx = cached
	proofs := []*types.KnowledgeCollectionAuxiliaryProof{}
	for _, tag := range tags {
		for _, field := range []string{"name", "color", "sort_order"} {
			proof, err := readKnowledgeDisplayOrigin(tx, "tag-"+field, tag.ID, tag.TenantID, tag.KnowledgeBaseID, tagDisplayFieldValue(tag, field))
			if err != nil {
				return nil, err
			}
			proofs = append(proofs, proof)
		}
		revision, err := knowledgeDisplayRevision(tx, "tag-envelope", tag.ID)
		if err != nil {
			return nil, err
		}
		digest, err := KnowledgeInputDigest(tag)
		if err != nil {
			return nil, err
		}
		lineage, _ := types.NewCompleteSourceLineage()
		proofs = append(proofs, &types.KnowledgeCollectionAuxiliaryProof{Kind: "tag-envelope", ID: tag.ID, TenantID: tag.TenantID, KnowledgeBaseID: tag.KnowledgeBaseID, Revision: revision, Digest: digest, SourceLineage: lineage})
	}
	return proofs, nil
}
func (s *KnowledgeInputProvenanceStore) captureTagOutputInTx(tx *gorm.DB, req types.TagOutputReadRequest) (*types.TagOutputInputSnapshot, error) {
	if err := s.checkFAQInputReadPlanInTx(tx, types.FAQInputReadRequest{TenantID: req.TenantID, KnowledgeBaseID: req.KnowledgeBaseID, AllEntries: true}); err != nil {
		return nil, err
	}
	if req.TenantID == 0 || req.KnowledgeBaseID == "" {
		return nil, types.ErrSourceLineageUnknown
	}
	var kb types.KnowledgeBase
	if err := tx.Where("tenant_id=? AND id=?", req.TenantID, req.KnowledgeBaseID).Take(&kb).Error; err != nil {
		return nil, err
	}
	result := &types.TagOutputInputSnapshot{Request: req, Tags: []*types.KnowledgeTag{}, TagProofs: []*types.KnowledgeCollectionAuxiliaryProof{}, Knowledge: []*types.KnowledgeCollectionCandidate{}, Chunks: []*types.TagOutputChunkCandidate{}}
	if req.TagID != "" {
		var tag types.KnowledgeTag
		if err := tx.Where("tenant_id=? AND knowledge_base_id=? AND id=?", req.TenantID, req.KnowledgeBaseID, req.TagID).Take(&tag).Error; err != nil {
			return nil, err
		}
		result.Tags = append(result.Tags, &tag)
	} else {
		var version struct{ Revision uint64 }
		if err := tx.Table("knowledge_tag_statistics_scope_versions").Where("tenant_id=? AND knowledge_base_id=?", req.TenantID, req.KnowledgeBaseID).Take(&version).Error; err != nil {
			return nil, err
		}
		if version.Revision == 0 {
			return nil, ErrKnowledgeInputChanged
		}
		result.ScopeRevision = version.Revision
		last := ""
		for {
			var rows []*types.KnowledgeTag
			q := tx.Where("tenant_id=? AND knowledge_base_id=?", req.TenantID, req.KnowledgeBaseID)
			if last != "" {
				q = q.Where("id > ?", last)
			}
			if err := q.Order("id ASC").Limit(256).Find(&rows).Error; err != nil {
				return nil, err
			}
			result.Tags = append(result.Tags, rows...)
			if len(rows) < 256 {
				break
			}
			last = rows[len(rows)-1].ID
		}
	}
	proofs, err := captureTagFields(tx, result.Tags)
	if err != nil {
		return nil, err
	}
	result.TagProofs = proofs
	if req.TagID != "" {
		return result, nil
	}
	// Actual count SQL joins living knowledge membership and all living tagged
	// chunks (including FAQ). Unrelated untagged rows are never authenticated as
	// count producers. Keyset batches impose no whole-KB or chunk-count ceiling.
	tagsQuery := tx.Model(&types.KnowledgeTag{}).Select("id").Where("tenant_id=? AND knowledge_base_id=?", req.TenantID, req.KnowledgeBaseID)
	byKnowledge := map[string]*types.KnowledgeCollectionCandidate{}
	last := ""
	for {
		var rows []*types.Knowledge
		q := tx.Where("tenant_id=? AND knowledge_base_id=?", req.TenantID, req.KnowledgeBaseID).
			Where("EXISTS (SELECT 1 FROM knowledge_tag_relations r WHERE r.knowledge_id=knowledges.id AND r.tag_id IN (?)) OR EXISTS (SELECT 1 FROM chunks c WHERE c.knowledge_id=knowledges.id AND c.tenant_id=? AND c.knowledge_base_id=? AND c.deleted_at IS NULL AND c.tag_id IN (?))", tagsQuery, req.TenantID, req.KnowledgeBaseID, tagsQuery)
		if last != "" {
			q = q.Where("id > ?", last)
		}
		if err := q.Order("id ASC").Limit(256).Find(&rows).Error; err != nil {
			return nil, err
		}
		for _, k := range rows {
			a, _, lineage, err := s.admissionInTx(tx, k, nil)
			if err != nil {
				return nil, err
			}
			proof, err := collectionProof(tx, k, a, lineage)
			if err != nil {
				return nil, err
			}
			candidate := &types.KnowledgeCollectionCandidate{Knowledge: k, Proof: proof}
			result.Knowledge = append(result.Knowledge, candidate)
			byKnowledge[k.ID] = candidate
		}
		if len(rows) < 256 {
			break
		}
		last = rows[len(rows)-1].ID
	}
	last = ""
	for {
		var rows []*types.Chunk
		q := tx.Where("tenant_id=? AND knowledge_base_id=? AND tag_id IN (?)", req.TenantID, req.KnowledgeBaseID, tagsQuery)
		if last != "" {
			q = q.Where("id > ?", last)
		}
		if err := q.Order("id ASC").Limit(256).Find(&rows).Error; err != nil {
			return nil, err
		}
		for _, chunk := range rows {
			candidate := byKnowledge[chunk.KnowledgeID]
			if candidate == nil {
				return nil, ErrKnowledgeInputChanged
			}
			admission, _, _, err := s.admissionInTx(tx, candidate.Knowledge, candidate.Proof)
			if err != nil {
				return nil, err
			}
			lineage := candidate.Proof.SourceLineage
			var parents []*types.KnowledgeBuildInputProof
			if candidate.Knowledge.Type == types.KnowledgeTypeFAQ && chunk.ChunkType == types.ChunkTypeFAQ {
				proof, readErr := s.readFAQEntryInputInTx(tx, chunk)
				err = readErr
				if err != nil {
					return nil, err
				}
				lineage = proof.SourceLineage
				parents = proof.OriginalKnowledgeInputs
			} else if err := verifyKnowledgeInputMaterial(tx, admission, candidate.Knowledge, chunk); err != nil {
				return nil, err
			}
			revision, err := inputRowRevision(tx, "chunk", chunk.ID)
			if err != nil {
				return nil, err
			}
			digest, err := knowledgeChunkDigest(chunk)
			if err != nil {
				return nil, err
			}
			result.Chunks = append(result.Chunks, &types.TagOutputChunkCandidate{Chunk: chunk, Revision: revision, Digest: digest, SourceLineage: lineage, OriginalKnowledgeInputs: parents})
		}
		if len(rows) < 256 {
			break
		}
		last = rows[len(rows)-1].ID
	}
	knowledge := make([]*types.Knowledge, 0, len(result.Knowledge))
	for _, candidate := range result.Knowledge {
		knowledge = append(knowledge, candidate.Knowledge)
	}
	result.Auxiliary, err = captureKnowledgeCollectionAuxiliaryWithNames(tx, []types.KnowledgeSearchScope{{TenantID: req.TenantID, KBID: req.KnowledgeBaseID}}, knowledge, map[string]bool{})
	return result, err
}
func (s *KnowledgeInputProvenanceStore) CaptureTagOutputInputs(ctx context.Context, req types.TagOutputReadRequest) (*types.TagOutputInputSnapshot, error) {
	var result *types.TagOutputInputSnapshot
	err := s.collectionTransaction(ctx, func(tx *gorm.DB) error { var err error; result, err = s.captureTagOutputInTx(tx, req); return err })
	return result, err
}
func (s *KnowledgeInputProvenanceStore) recheckTagOutputInTx(tx *gorm.DB, snapshot *types.TagOutputInputSnapshot) error {
	if snapshot == nil {
		return types.ErrSourceLineageUnknown
	}
	actual, err := s.captureTagOutputInTx(tx, snapshot.Request)
	if err != nil {
		return err
	}
	if !tagOutputEqual(actual, snapshot) {
		return ErrKnowledgeInputChanged
	}
	return nil
}
func selectTagOutputInTx(ctx context.Context, tx *gorm.DB, snapshot *types.TagOutputInputSnapshot, req types.TagOutputSelectionRequest) (*types.TagOutputSelection, error) {
	if snapshot.Request.TagID != "" {
		return nil, types.ErrSourceLineageUnknown
	}
	repo := &knowledgeTagRepository{db: tx}
	r := snapshot.Request
	tags, total, err := repo.ListByKB(ctx, r.TenantID, r.KnowledgeBaseID, &req.Page, req.Keyword)
	if err != nil {
		return nil, err
	}
	ids := make([]string, 0, len(tags))
	for _, tag := range tags {
		ids = append(ids, tag.ID)
	}
	counts, err := repo.BatchCountReferences(ctx, r.TenantID, r.KnowledgeBaseID, ids)
	if err != nil {
		return nil, err
	}
	result := &types.TagOutputSelection{Request: req, Tags: []*types.KnowledgeTagWithStats{}, Total: total}
	for _, tag := range tags {
		c := counts[tag.ID]
		result.Tags = append(result.Tags, &types.KnowledgeTagWithStats{KnowledgeTag: *tag, KnowledgeCount: c.KnowledgeCount, ChunkCount: c.ChunkCount})
	}
	return result, nil
}
func (s *KnowledgeInputProvenanceStore) SelectTagOutputInputs(ctx context.Context, snapshot *types.TagOutputInputSnapshot, req types.TagOutputSelectionRequest) (*types.TagOutputSelection, error) {
	var result *types.TagOutputSelection
	err := s.collectionTransaction(ctx, func(tx *gorm.DB) error {
		if err := s.recheckTagOutputInTx(tx, snapshot); err != nil {
			return err
		}
		var err error
		result, err = selectTagOutputInTx(ctx, tx, snapshot, req)
		return err
	})
	return result, err
}
func (s *KnowledgeInputProvenanceStore) RecheckTagOutputInputs(ctx context.Context, snapshot *types.TagOutputInputSnapshot, selection *types.TagOutputSelection) error {
	return s.collectionTransaction(ctx, func(tx *gorm.DB) error {
		if err := s.recheckTagOutputInTx(tx, snapshot); err != nil {
			return err
		}
		if selection != nil {
			actual, err := selectTagOutputInTx(ctx, tx, snapshot, selection.Request)
			if err != nil {
				return err
			}
			if !tagOutputEqual(actual, selection) {
				return ErrKnowledgeInputChanged
			}
		}
		return nil
	})
}

var _ interfaces.TagOutputInputReader = (*KnowledgeInputProvenanceStore)(nil)
