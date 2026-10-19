package repository

import (
	"context"
	"encoding/json"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

type autoTagActualTaskInput struct {
	Kind    string
	Payload types.KnowledgeAutoTagPayload
}

// AdmitKnowledgeAutoTagTaskInput admits knowledge auto tag task input.
func (s *KnowledgeInputProvenanceStore) AdmitKnowledgeAutoTagTaskInput(
	ctx context.Context,
	p types.KnowledgeAutoTagPayload,
) (*types.KnowledgeAutoTagTaskInputProof, error) {
	if p.TaskInput != nil || p.OriginalInput == nil || p.OriginalInput.BodyDigest == "" ||
		p.TenantID != p.OriginalInput.TenantID ||
		p.KnowledgeBaseID != p.OriginalInput.KnowledgeBaseID ||
		p.KnowledgeID != p.OriginalInput.KnowledgeID {
		return nil, types.ErrSourceLineageUnknown
	}
	if err := s.RecheckKnowledgeBuildInputs(ctx, p.OriginalInput); err != nil {
		return nil, err
	}
	envelope := autoTagActualTaskInput{Kind: "actual_queue_input", Payload: p}
	digest, err := KnowledgeInputDigest(envelope)
	if err != nil {
		return nil, err
	}
	data, err := json.Marshal(envelope)
	if err != nil {
		return nil, err
	}
	lineage, err := json.Marshal(p.OriginalInput.SourceLineage)
	if err != nil {
		return nil, err
	}
	id := uuid.NewString()
	err = s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		actual, err := s.captureKnowledgeBuildSnapshotInTx(
			tx,
			types.KnowledgeBuildInputCaptureRequest{
				TenantID:        p.TenantID,
				KnowledgeBaseID: p.KnowledgeBaseID,
				KnowledgeID:     p.KnowledgeID,
				ChunkIDs:        p.OriginalInput.ChunkIDs,
				Expected:        p.OriginalInput,
			},
			true,
		)
		if err != nil {
			return err
		}
		if s.displayAuthorityFactory == nil {
			return types.ErrSourceLineageUnknown
		}
		authority := s.displayAuthorityFactory(tx)
		if authority == nil || authority.db != tx {
			return types.ErrSourceLineageUnknown
		}
		if _, err := authority.authorizeOriginalKnowledgeProducerWrite(
			ctx,
			actual.KnowledgeBase,
			p.OriginalInput,
		); err != nil {
			return err
		}
		return tx.Create(
			&knowledgeAutoTagManifest{
				ManifestID:      id,
				TenantID:        p.TenantID,
				KnowledgeBaseID: p.KnowledgeBaseID,
				KnowledgeID:     p.KnowledgeID,
				AdmissionID:     p.OriginalInput.AdmissionID,
				InputDigest:     digest,
				ActualInputs:    string(data),
				SourceLineage:   string(lineage),
			},
		).Error
	}, knowledgeDisplayWriteOptions(s.db))
	if err != nil {
		return nil, err
	}
	return &types.KnowledgeAutoTagTaskInputProof{ID: id, Digest: digest}, nil
}

// VerifyKnowledgeAutoTagTaskInput verifies knowledge auto tag task input.
func (s *KnowledgeInputProvenanceStore) VerifyKnowledgeAutoTagTaskInput(
	ctx context.Context,
	p types.KnowledgeAutoTagPayload,
) error {
	if p.TaskInput == nil || p.OriginalInput == nil {
		return types.ErrSourceLineageUnknown
	}
	var row knowledgeAutoTagManifest
	if err := s.db.WithContext(ctx).
		Where(("manifest_id = ? AND tenant_id = ? AND knowledge_base_id = ? AND knowledge_" +
			"id = ? AND admission_id = ?"), p.TaskInput.ID, p.TenantID, p.KnowledgeBaseID, p.KnowledgeID, p.OriginalInput.AdmissionID).
		Take(&row).
		Error; err != nil {
		return err
	}
	if p.TaskInput.Digest != row.InputDigest {
		return ErrKnowledgeInputChanged
	}
	p.TaskInput = nil
	got := autoTagActualTaskInput{Kind: "actual_queue_input", Payload: p}
	digest, err := KnowledgeInputDigest(got)
	if err != nil || digest != row.InputDigest {
		return ErrKnowledgeInputChanged
	}
	var saved autoTagActualTaskInput
	if json.Unmarshal([]byte(row.ActualInputs), &saved) != nil || saved.Kind != "actual_queue_input" {
		return ErrKnowledgeInputChanged
	}
	stored, err := KnowledgeInputDigest(saved)
	if err != nil || stored != digest {
		return ErrKnowledgeInputChanged
	}
	return nil
}
