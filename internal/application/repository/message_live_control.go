package repository

import (
	"context"

	"github.com/Tencent/WeKnora/internal/types"
	"gorm.io/gorm"
)

// MessageLiveControl checks current receiver identity on the same transaction
// and reads only immutable control metadata; it never hydrates saved text.
func (s *KnowledgeInputProvenanceStore) MessageLiveControl(
	ctx context.Context,
	sessionID, messageID string,
) (*types.Message, string, error) {
	var result *types.Message
	var generation string
	err := s.collectionTransaction(ctx, func(tx *gorm.DB) error {
		var err error
		result, generation, err = s.messageLiveControlInTx(tx, sessionID, messageID)
		return err
	})
	return result, generation, err
}

func (s *KnowledgeInputProvenanceStore) messageLiveControlInTx(
	tx *gorm.DB,
	sessionID, messageID string,
) (*types.Message, string, error) {
	_, session, ownerRevision, err := s.currentMessageOwnerInTx(tx, sessionID)
	if err != nil {
		return nil, "", err
	}
	var origin messageGenerationOrigin
	if err := tx.Select(messageGenerationMetadataFields).
		Where("message_id=? AND session_id=? AND tenant_id=?", messageID, sessionID, session.TenantID).
		Take(&origin).
		Error; err != nil {
		return nil, "", err
	}
	if origin.OwnerID != session.UserID || origin.OwnerRevision != ownerRevision {
		return nil, "", ErrKnowledgeInputChanged
	}
	var control types.Message
	if err := tx.Select("id,session_id,request_id,role,is_completed,agent_id,agent_tenant_id,model_id,execution_context,created_at").
		Where("id=? AND session_id=? AND deleted_at IS NULL", messageID, sessionID).
		Take(&control).
		Error; err != nil {
		return nil, "", err
	}
	if origin.RequestID != control.RequestID || origin.Role != control.Role {
		return nil, "", ErrKnowledgeInputChanged
	}
	var admission messageMaterialAdmission
	if err := tx.Table("message_material_admissions a").
		Select("a.id,a.generation_id,a.revision").
		Joins("JOIN message_material_receipts r ON r.admission_id=a.id AND r.message_id=a.message_id").
		Where("a.message_id=? AND a.session_id=?", messageID, sessionID).
		Take(&admission).
		Error; err != nil {
		return nil, "", err
	}
	revision, err := messageVersion(tx, "message_material_versions", "message_id", messageID)
	if err != nil {
		return nil, "", err
	}
	if admission.GenerationID != origin.ID || admission.Revision != revision {
		return nil, "", ErrKnowledgeInputChanged
	}
	return &control, origin.ID, nil
}

func (s *KnowledgeInputProvenanceStore) MessageOwnedControlMetadata(
	ctx context.Context,
	sessionID, messageID string,
) (*types.Message, error) {
	var result types.Message
	err := s.collectionTransaction(ctx, func(tx *gorm.DB) error {
		if _, _, _, err := s.currentMessageOwnerInTx(tx, sessionID); err != nil {
			return err
		}
		return tx.Select("id,session_id,request_id,role,is_completed,agent_id,agent_tenant_id").
			Where("id=? AND session_id=? AND deleted_at IS NULL", messageID, sessionID).
			Take(&result).
			Error
	})
	return &result, err
}
