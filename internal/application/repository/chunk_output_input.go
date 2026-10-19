package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"gorm.io/gorm"
)

func (s *KnowledgeInputProvenanceStore) CaptureChunkOutputInputs(
	ctx context.Context,
	request types.ChunkOutputReadRequest,
) (*types.ChunkOutputInputSnapshot, error) {
	if ctx == nil || s == nil || s.db == nil || request.TenantID == 0 || request.KnowledgeBaseID == "" ||
		request.KnowledgeID == "" ||
		len(request.ChunkIDs) > 4096 {
		return nil, types.ErrSourceLineageUnknown
	}
	ids := append([]string{}, request.ChunkIDs...)
	if request.AllChunks {
		if len(ids) != 0 {
			return nil, types.ErrSourceLineageUnknown
		}
		if err := s.db.WithContext(ctx).
			Model(&types.Chunk{}).
			Where("tenant_id = ? AND knowledge_base_id = ? AND knowledge_id = ?", request.TenantID, request.KnowledgeBaseID, request.KnowledgeID).
			Order("id ASC").
			Limit(4097).
			Pluck("id", &ids).
			Error; err != nil {
			return nil, err
		}
		if len(ids) > 4096 {
			return nil, types.ErrSourceLineageLimit
		}
	}
	if !request.AllChunks && len(ids) == 0 {
		return nil, types.ErrSourceLineageUnknown
	}
	actual, err := s.CaptureKnowledgeOutputInputs(
		ctx,
		types.KnowledgeBuildInputCaptureRequest{
			TenantID:        request.TenantID,
			KnowledgeBaseID: request.KnowledgeBaseID,
			KnowledgeID:     request.KnowledgeID,
			ChunkIDs:        ids,
		},
	)
	if err != nil {
		return nil, err
	}
	if actual == nil || actual.Knowledge == nil || actual.Proof == nil || actual.Proof.Projection != "" ||
		actual.Proof.SourceLineage.RequireComplete() != nil ||
		len(actual.Chunks) != len(ids) {
		return nil, types.ErrSourceLineageUnknown
	}
	snapshot := &types.ChunkOutputInputSnapshot{
		Knowledge: actual.Knowledge,
		Chunks:    actual.Chunks,
		Proof:     actual.Proof,
		AllChunks: request.AllChunks,
	}
	if err := s.RecheckChunkOutputInputs(ctx, snapshot); err != nil {
		return nil, err
	}
	return snapshot, nil
}

// Recheck the exact body and complete candidate membership with the original
// metadata/chunk material receipts in one SQLite/RR snapshot. A current ETag or
// a freshly obtained document origin cannot authenticate these old values.
func (s *KnowledgeInputProvenanceStore) RecheckChunkOutputInputs(
	ctx context.Context,
	snapshot *types.ChunkOutputInputSnapshot,
) error {
	if ctx == nil || s == nil || s.db == nil || snapshot == nil || snapshot.Knowledge == nil || snapshot.Proof == nil ||
		snapshot.Proof.Projection != "" ||
		snapshot.Proof.BodyDigest == "" ||
		snapshot.Proof.SourceLineage.RequireComplete() != nil ||
		len(snapshot.Chunks) > 4096 {
		return types.ErrSourceLineageUnknown
	}
	proof := snapshot.Proof
	k := snapshot.Knowledge
	if k.ID != proof.KnowledgeID || k.TenantID != proof.TenantID || k.KnowledgeBaseID != proof.KnowledgeBaseID {
		return ErrKnowledgeInputChanged
	}
	options := &sql.TxOptions{ReadOnly: true}
	switch s.db.Name() {
	case "postgres":
		options.Isolation = sql.LevelRepeatableRead
	case "sqlite":
	default:
		return ErrKnowledgeInputChanged
	}
	if _, nested := s.db.Statement.ConnPool.(gorm.TxCommitter); nested {
		return ErrKnowledgeInputChanged
	}
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var current types.Knowledge
		if err := tx.Where("id = ? AND tenant_id = ? AND knowledge_base_id = ?", k.ID, k.TenantID, k.KnowledgeBaseID).
			Take(&current).
			Error; err != nil {
			return err
		}
		suppliedBody, err := json.Marshal(k)
		if err != nil {
			return err
		}
		actualBody, err := json.Marshal(&current)
		if err != nil {
			return err
		}
		if string(suppliedBody) != string(actualBody) {
			return ErrKnowledgeInputChanged
		}
		admission, _, _, err := s.admissionInTx(tx, &current, proof)
		if err != nil {
			return err
		}
		material := []string{admission.AdmissionID, admission.RootDigest, admission.ConfigDigest}
		kgDigest, err := knowledgeMaterialDigest(&current)
		if err != nil {
			return err
		}
		material = append(material, kgDigest)
		supplied := map[string]*types.Chunk{}
		ids := append([]string{}, proof.ChunkIDs...)
		sort.Strings(ids)
		for _, chunk := range snapshot.Chunks {
			if chunk == nil || chunk.ID == "" || supplied[chunk.ID] != nil || chunk.KnowledgeID != k.ID ||
				chunk.TenantID != k.TenantID ||
				chunk.KnowledgeBaseID != k.KnowledgeBaseID {
				return ErrKnowledgeInputChanged
			}
			supplied[chunk.ID] = chunk
		}
		if len(ids) != len(supplied) {
			return ErrKnowledgeInputChanged
		}
		if snapshot.AllChunks {
			var allIDs []string
			if err := tx.Model(&types.Chunk{}).
				Where("tenant_id = ? AND knowledge_base_id = ? AND knowledge_id = ?", k.TenantID, k.KnowledgeBaseID, k.ID).
				Order("id ASC").
				Limit(4097).
				Pluck("id", &allIDs).
				Error; err != nil {
				return err
			}
			if len(allIDs) > 4096 {
				return types.ErrSourceLineageLimit
			}
			if !reflect.DeepEqual(ids, allIDs) && (len(ids) != 0 || len(allIDs) != 0) {
				return ErrKnowledgeInputChanged
			}
		}
		for i, id := range ids {
			if id == "" || i > 0 && ids[i-1] == id || supplied[id] == nil {
				return ErrKnowledgeInputChanged
			}
			var chunk types.Chunk
			if err := tx.Where("id = ? AND tenant_id = ? AND knowledge_base_id = ? AND knowledge_id = ?", id, k.TenantID, k.KnowledgeBaseID, k.ID).
				Take(&chunk).
				Error; err != nil {
				return err
			}
			if err := verifyKnowledgeInputMaterial(tx, admission, &current, &chunk); err != nil {
				return err
			}
			body, err := json.Marshal(supplied[id])
			if err != nil {
				return err
			}
			actual, err := json.Marshal(&chunk)
			if err != nil {
				return err
			}
			if string(body) != string(actual) {
				return ErrKnowledgeInputChanged
			}
			digest, err := knowledgeChunkDigest(&chunk)
			if err != nil {
				return err
			}
			revision, err := inputRowRevision(tx, "chunk", id)
			if err != nil {
				return err
			}
			material = append(material, fmt.Sprintf("%s:%d:%s", id, revision, digest))
		}
		revision, err := inputRowRevision(tx, "knowledge", k.ID)
		if err != nil {
			return err
		}
		material = append(material, fmt.Sprint(revision))
		digest, err := KnowledgeInputDigest(material)
		if err != nil {
			return err
		}
		if digest != proof.BodyDigest {
			return ErrKnowledgeInputChanged
		}
		return nil
	}, options)
}

var _ interfaces.ChunkOutputInputReader = (*KnowledgeInputProvenanceStore)(nil)
