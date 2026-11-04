package repository

import (
	"context"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// FinalizeDingTalkIndexedKnowledge uses the existing material writer checks and
// StorageSize as the accounting baseline. Replaying a failed downstream handoff
// replaces that knowledge's indexed footprint, rather than adding it twice.
// No run claim is needed: this is the independently authorized queue worker,
// fenced by its original proof and the currently published knowledge identity.
func (s *KnowledgeInputProvenanceStore) FinalizeDingTalkIndexedKnowledge(ctx context.Context, k *types.Knowledge) error {
	return s.persistDingTalkProcessingState(ctx, k, "")
}

// SetDingTalkProcessingFailure retains the current accounted size and cannot
// overwrite a cancellation, deletion, replacement or completed newer result.
func (s *KnowledgeInputProvenanceStore) SetDingTalkProcessingFailure(ctx context.Context, k *types.Knowledge, code string) error {
	if code == "" {
		return ErrKnowledgeInputChanged
	}
	return s.persistDingTalkProcessingState(ctx, k, code)
}

func (s *KnowledgeInputProvenanceStore) persistDingTalkProcessingState(ctx context.Context, k *types.Knowledge, failureCode string) error {
	if k == nil || k.Channel != types.ConnectorTypeDingTalk || k.StorageSize < 0 {
		return ErrKnowledgeInputChanged
	}
	if err := PrecheckKnowledgeBuildWrite(ctx); err != nil {
		return err
	}
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		proof := KnowledgeBuildInputProofFromContext(ctx)
		if proof == nil {
			return ErrKnowledgeInputChanged
		}
		var candidate knowledgeInputAdmission
		if err := tx.Where("admission_id=? AND tenant_id=? AND knowledge_base_id=? AND knowledge_id=?", proof.AdmissionID, k.TenantID, k.KnowledgeBaseID, k.ID).Take(&candidate).Error; err != nil {
			return err
		}
		local, err := isDingTalkLocalReparseAdmission(tx, &candidate)
		if err != nil {
			return err
		}
		if !local {
			var ds types.DataSource
			if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("id = ? AND tenant_id = ? AND knowledge_base_id = ? AND type = ?", k.GetMetadata()["datasource_id"], k.TenantID, k.KnowledgeBaseID, types.ConnectorTypeDingTalk).Take(&ds).Error; err != nil {
				return err
			}
			var publication dingTalkPublication
			if err := tx.Where("tenant_id = ? AND knowledge_base_id = ? AND data_source_id = ? AND external_id = ? AND knowledge_id = ? AND tombstone = ?", k.TenantID, k.KnowledgeBaseID, ds.ID, k.GetMetadata()["external_id"], k.ID, false).Take(&publication).Error; err != nil {
				return err
			}
			if publication.Generation == 0 {
				return ErrDingTalkPublicationConflict
			}
		}
		var old types.Knowledge
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("id = ? AND tenant_id = ? AND knowledge_base_id = ?", k.ID, k.TenantID, k.KnowledgeBaseID).Take(&old).Error; err != nil {
			return err
		}
		if old.StorageSize < 0 {
			return ErrKnowledgeInputChanged
		}
		if old.ParseStatus == types.ParseStatusCancelled || old.ParseStatus == types.ParseStatusDeleting ||
			(old.ParseStatus == types.ParseStatusCompleted && (k.ParseStatus != types.ParseStatusCompleted || failureCode != "")) {
			return ErrDingTalkPublicationConflict
		}
		admission, err := validateKnowledgeInputWrite(ctx, tx, k.TenantID, k.KnowledgeBaseID, k.ID)
		if err != nil {
			return err
		}
		if admission == nil || (admission.OriginKind != dingTalkOriginalKind && !local) {
			return ErrKnowledgeInputChanged
		}
		digest, err := knowledgeRootDigest(k)
		if err != nil {
			return err
		}
		if digest != admission.RootDigest {
			return ErrKnowledgeInputChanged
		}
		if failureCode != "" {
			*k = old
			k.ParseStatus, k.ErrorMessage, k.UpdatedAt = types.ParseStatusFailed, failureCode, time.Now().UTC()
		}
		var tenant types.Tenant
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("id = ?", k.TenantID).Take(&tenant).Error; err != nil {
			return err
		}
		remaining := tenant.StorageUsed + k.StorageSize - old.StorageSize
		if remaining < 0 {
			remaining = 0
		}
		omit := append([]string(nil), omitFieldsOnUpdate...)
		if k.CustomMetadata == nil {
			omit = append(omit, "custom_metadata")
		}
		if err := dingTalkCAS(tx.Model(&types.Knowledge{}).Where("id = ? AND tenant_id = ? AND knowledge_base_id = ? AND storage_size = ? AND parse_status = ?", k.ID, k.TenantID, k.KnowledgeBaseID, old.StorageSize, old.ParseStatus).Select("*").Omit(omit...).Updates(k)); err != nil {
			return err
		}
		var actual types.Knowledge
		if err := tx.Where("id = ? AND tenant_id = ? AND knowledge_base_id = ?", k.ID, k.TenantID, k.KnowledgeBaseID).Take(&actual).Error; err != nil {
			return err
		}
		if err := sealKnowledgeInputMaterial(tx, admission.AdmissionID, &actual, nil); err != nil {
			return err
		}
		return dingTalkCAS(tx.Model(&types.Tenant{}).Where("id = ? AND storage_used = ?", k.TenantID, tenant.StorageUsed).Update("storage_used", remaining))
	})
}
