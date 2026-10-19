package repository

import (
	"context"
	"encoding/json"

	"github.com/google/uuid"

	"github.com/Tencent/WeKnora/internal/types"
	"gorm.io/gorm"
)

type knowledgeAutoPublicationTransition struct {
	TransitionID                              string `gorm:"primaryKey"`
	SnapshotID                                string
	TenantID                                  uint64
	KnowledgeBaseID, KnowledgeID, AdmissionID string
	BeforeRevision, AfterRevision             uint64
	BeforeDigest, AfterDigest                 string
}

func (knowledgeAutoPublicationTransition) TableName() string {
	return "knowledge_auto_publication_transitions"
}

func knowledgeAutoPublicationRevision(tx *gorm.DB, id string) (uint64, error) {
	var row struct{ Revision uint64 }
	if err := tx.Table("knowledge_auto_publication_versions").
		Where("knowledge_id = ?", id).
		Take(&row).
		Error; err != nil {
		return 0, err
	}
	if row.Revision == 0 {
		return 0, ErrKnowledgeInputChanged
	}
	return row.Revision, nil
}

// Only the real, source-checked publication transaction can write this exact
// before/after transition. Neither startup nor Auto can fill a missing receipt.
func recordKnowledgeAutoPublicationTransitionInTx(
	ctx context.Context,
	tx *gorm.DB,
	before, after *types.Knowledge,
	beforeRevision uint64,
) error {
	if before == nil || after == nil || before.ID != after.ID {
		return ErrKnowledgeInputChanged
	}
	var pending []*types.TaskPendingOp
	if err := tx.Where("tenant_id=? AND task_type=? AND scope=? AND scope_id=? AND op IN ?", before.TenantID, types.TypeKnowledgeAutoTag, types.TaskScopeKnowledge, before.ID, []string{autoTagCompletionOp, autoTagCompletionRunning}).
		Find(&pending).
		Error; err != nil {
		return err
	}
	if len(pending) == 0 {
		return nil
	}
	afterRevision, err := knowledgeAutoPublicationRevision(tx, after.ID)
	if err != nil {
		return err
	}
	beforeDigest, err := KnowledgeInputDigest(before)
	if err != nil {
		return err
	}
	afterDigest, err := KnowledgeInputDigest(after)
	if err != nil {
		return err
	}
	for _, op := range pending {
		var p types.KnowledgeAutoTagPayload
		if json.Unmarshal(op.Payload, &p) != nil || p.CompletionInput == "" || p.TaskInput == nil {
			return ErrKnowledgeInputChanged
		}
		var manifest knowledgeAutoTagManifest
		if err := tx.Where("manifest_id=? AND tenant_id=? AND knowledge_base_id=? AND knowledge_id=?", p.CompletionInput, before.TenantID, before.KnowledgeBaseID, before.ID).
			Take(&manifest).
			Error; err != nil {
			return err
		}
		var snapshot types.KnowledgeAutoTagInputSnapshot
		if json.Unmarshal([]byte(manifest.ActualInputs), &snapshot) != nil || snapshot.Original == nil ||
			snapshot.Original.Proof == nil ||
			snapshot.Original.Knowledge == nil {
			return ErrKnowledgeInputChanged
		}
		digest, err := autoTagSnapshotDigest(&snapshot)
		if err != nil || digest != manifest.InputDigest || snapshot.InputDigest != digest ||
			snapshot.ManifestID != p.CompletionInput {
			return ErrKnowledgeInputChanged
		}
		originalRevision, originalDigest, err := knowledgeAutoPublicationTail(tx, &snapshot)
		if err != nil || originalDigest != beforeDigest || originalRevision != beforeRevision ||
			snapshot.Original.Proof.AdmissionID != manifest.AdmissionID ||
			p.OriginalInput == nil ||
			p.OriginalInput.AdmissionID != manifest.AdmissionID {
			return ErrKnowledgeInputChanged
		}
		if err := ValidateNextcloudBuildWrite(ctx, tx, before.TenantID, before.KnowledgeBaseID, before.ID); err != nil {
			return err
		}
		// All influencing bodies and original source controls are still checked
		// using their immutable admission/material receipts in this same tx.
		input, ok := ctx.Value(knowledgeBuildInputKey{}).(knowledgeBuildInputContext)
		store, okStore := input.reader.(*KnowledgeInputProvenanceStore)
		if !ok || !okStore {
			return types.ErrSourceLineageUnknown
		}
		if _, err := store.captureKnowledgeBuildSnapshotInTx(
			tx,
			types.KnowledgeBuildInputCaptureRequest{
				TenantID:        p.TenantID,
				KnowledgeBaseID: p.KnowledgeBaseID,
				KnowledgeID:     p.KnowledgeID,
				ChunkIDs:        p.OriginalInput.ChunkIDs,
				Expected:        p.OriginalInput,
			},
			true,
		); err != nil {
			return err
		}
		receipt := knowledgeAutoPublicationTransition{
			TransitionID:    uuid.NewString(),
			SnapshotID:      snapshot.ManifestID,
			TenantID:        before.TenantID,
			KnowledgeBaseID: before.KnowledgeBaseID,
			KnowledgeID:     before.ID,
			AdmissionID:     manifest.AdmissionID,
			BeforeRevision:  beforeRevision,
			AfterRevision:   afterRevision,
			BeforeDigest:    beforeDigest,
			AfterDigest:     afterDigest,
		}
		if err := tx.Create(&receipt).Error; err != nil {
			return err
		}
	}
	return nil
}

func acceptKnowledgeAutoPublicationTransition(tx *gorm.DB, saved, actual *types.KnowledgeAutoTagInputSnapshot) error {
	if saved.Original == nil || actual.Original == nil || saved.Original.Knowledge == nil ||
		actual.Original.Knowledge == nil {
		return ErrKnowledgeInputChanged
	}
	if saved.Original.Knowledge.Channel != types.ConnectorTypeNextcloud {
		return nil
	}
	if saved.PublicationRevision == actual.PublicationRevision {
		return nil
	}
	revision, digest, err := knowledgeAutoPublicationTail(tx, saved)
	if err != nil {
		return err
	}
	after, err := KnowledgeInputDigest(actual.Original.Knowledge)
	if err != nil || revision != actual.PublicationRevision || digest != after {
		return ErrKnowledgeInputChanged
	}
	// Normalize only the exact row committed by this admission's publication
	// receipt. An external timestamp/ETag/metadata ABA increments the dedicated
	// counter and cannot reuse this transition even if it restores the bytes.
	actual.Original.Knowledge = saved.Original.Knowledge
	actual.PublicationRevision = saved.PublicationRevision
	return nil
}

// Immutable, contiguous transitions cover a legitimate publish/unpublish and
// later retry. Any external update introduces a revision gap, including ABA.
func knowledgeAutoPublicationTail(tx *gorm.DB, saved *types.KnowledgeAutoTagInputSnapshot) (uint64, string, error) {
	proof := saved.Original.Proof
	revision := saved.PublicationRevision
	digest, err := KnowledgeInputDigest(saved.Original.Knowledge)
	if err != nil {
		return 0, "", err
	}
	var receipts []knowledgeAutoPublicationTransition
	if err := tx.Where("snapshot_id=? AND tenant_id=? AND knowledge_base_id=? AND knowledge_id=? AND admission_id=?", saved.ManifestID, proof.TenantID, proof.KnowledgeBaseID, proof.KnowledgeID, proof.AdmissionID).
		Order("before_revision ASC").
		Find(&receipts).
		Error; err != nil {
		return 0, "", err
	}
	for _, receipt := range receipts {
		if receipt.BeforeRevision != revision || receipt.BeforeDigest != digest || receipt.AfterRevision <= revision {
			return 0, "", ErrKnowledgeInputChanged
		}
		revision, digest = receipt.AfterRevision, receipt.AfterDigest
	}
	return revision, digest, nil
}
