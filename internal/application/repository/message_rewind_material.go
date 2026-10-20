package repository

import (
	"context"

	"github.com/Tencent/WeKnora/internal/types"
	"gorm.io/gorm"
)

type (
	messageRewindStoreKey struct{}
	messageRewindHolder   struct {
		Store    *KnowledgeInputProvenanceStore
		Prewrite func() error
		Output   *MessageRewindOutputReceipt
	}
)

// MessageRewindOutputReceipt represents message rewind output receipt repository data.
type MessageRewindOutputReceipt struct {
	Parent          *types.MessageMaterialSnapshot
	OriginalPlan    *types.MessageMaterialReadPlan
	PostPlan        *types.MessageMaterialReadPlan
	Deleted         []*types.MessageMaterialProof
	DeletedVersions map[string]uint64
	FieldVersions   map[string]uint64
}

// WithMessageRewindMaterialStore binds message rewind material store.
func WithMessageRewindMaterialStore(
	ctx context.Context,
	store *KnowledgeInputProvenanceStore,
	prewrite func() error,
) context.Context {
	return context.WithValue(
		ctx,
		messageRewindStoreKey{},
		messageRewindHolder{Store: store, Prewrite: prewrite, Output: &MessageRewindOutputReceipt{}},
	)
}

// MessageRewindMaterialRequest returns the message rewind material request result.
func (s *KnowledgeInputProvenanceStore) MessageRewindMaterialRequest(
	ctx context.Context,
	tenant uint64,
	session, pointID string,
) (*types.MessageMaterialCandidateControl, types.MessageMaterialReadRequest, error) {
	req := types.MessageMaterialReadRequest{
		TenantID:                  tenant,
		SessionID:                 session,
		AllSession:                true,
		IncludeOriginalUserFields: true,
	}
	var point types.MessageMaterialCandidateControl
	err := s.collectionTransaction(ctx, func(tx *gorm.DB) error {
		if _, owner, _, err := s.currentMessageOwnerInTx(tx, session); err != nil {
			return err
		} else if owner.TenantID != tenant {
			return ErrKnowledgeInputChanged
		}
		return tx.Table("messages").
			Select("id,request_id,role,is_completed AS complete,created_at").
			Where("id=? AND session_id=? AND deleted_at IS NULL", pointID, session).
			Take(&point).
			Error
	})
	return &point, req, err
}

func (r *messageRepository) DeleteMessagesFromMaterial(
	ctx context.Context,
	session, pointID string,
	expected *types.MessageMaterialSnapshot,
) ([]*types.Message, error) {
	holder, ok := ctx.Value(messageRewindStoreKey{}).(messageRewindHolder)
	if !ok || holder.Store == nil || holder.Store != r.materialStore || holder.Prewrite == nil || expected == nil ||
		expected.Request.SessionID != session ||
		!expected.Request.AllSession {
		return nil, types.ErrSourceLineageUnknown
	}
	if err := holder.Prewrite(); err != nil {
		return nil, err
	}
	deleted := []*types.Message{}
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		actual, err := holder.Store.captureMessageMaterialInTx(tx, expected.Request)
		if err != nil {
			return err
		}
		a, b := *actual, *expected
		a.Messages = nil
		b.Messages = nil
		if !faqInputEqual(a, b) {
			return ErrKnowledgeInputChanged
		}
		var point *types.Message
		for _, message := range actual.Messages {
			if message.ID == pointID {
				point = message
			}
		}
		if point == nil || point.Role != "user" && point.Role != "assistant" {
			return ErrKnowledgeInputChanged
		}
		ids := []string{}
		for _, message := range actual.Messages {
			cut := message.CreatedAt.After(point.CreatedAt) ||
				message.CreatedAt.Equal(point.CreatedAt) &&
					(message.ID > point.ID || message.ID == point.ID && point.Role == "user")
			if !cut {
				continue
			}
			ids = append(ids, message.ID)
			cleanupKnowledge := ""
			if message.KnowledgeID != "" {
				var links []struct{ KnowledgeID string }
				if err := tx.Table("knowledge_input_history_parents p").
					Select("a.knowledge_id").
					Joins(("JOIN knowledge_input_admissions a ON a.admission_id=p.admission_id AND a.t" +
						"enant_id=p.tenant_id")).
					Where(("p.assistant_message_id=? AND p.session_id=? AND p.tenant_id=? AND a.origin" +
						"_kind='history'"), message.ID, session, expected.Request.TenantID).
					Scan(&links).
					Error; err != nil {
					return err
				}
				for _, link := range links {
					if link.KnowledgeID == message.KnowledgeID {
						cleanupKnowledge = link.KnowledgeID
					}
				}
				if cleanupKnowledge == "" {
					return ErrKnowledgeInputChanged
				}
			}
			deleted = append(
				deleted,
				&types.Message{
					ID:          message.ID,
					SessionID:   message.SessionID,
					KnowledgeID: cleanupKnowledge,
					RequestID:   message.RequestID,
					Role:        message.Role,
					CreatedAt:   message.CreatedAt,
				},
			)
		}
		if len(ids) > 0 {
			if err := tx.Where("session_id=? AND id IN ?", session, ids).Delete(&types.Message{}).Error; err != nil {
				return err
			}
		}
		parent := *expected
		parent.Messages = nil
		post, err := holder.Store.peekMessageMaterialReadPlanInTx(tx, expected.Request)
		if err != nil {
			return err
		}
		removed := []*types.MessageMaterialProof{}
		for _, proof := range expected.Proofs {
			for _, id := range ids {
				if proof.MessageID == id {
					removed = append(removed, proof)
				}
			}
		}
		versions := map[string]uint64{}
		for _, proof := range removed {
			version, err := messageVersion(tx, "message_material_versions", "message_id", proof.MessageID)
			if err != nil {
				return err
			}
			versions[proof.MessageID] = version
		}
		var fieldRows []struct {
			MessageID, Field string
			Revision         uint64
		}
		if err := tx.Table("message_generated_field_versions").
			Select("message_id,field,revision").
			Where("message_id IN ?", messageIDsFromProofs(expected.Proofs)).
			Scan(&fieldRows).
			Error; err != nil {
			return err
		}
		fieldVersions := map[string]uint64{}
		for _, row := range fieldRows {
			fieldVersions[row.MessageID+":"+row.Field] = row.Revision
		}
		*holder.Output = MessageRewindOutputReceipt{
			Parent:          &parent,
			OriginalPlan:    findMessageMaterialReadPlan(tx.Statement.Context, expected.Request),
			PostPlan:        post,
			Deleted:         removed,
			DeletedVersions: versions,
			FieldVersions:   fieldVersions,
		}
		return nil
	})
	return deleted, err
}

// MessageRewindOutputReceiptFromContext returns the message rewind output receipt from context result.
func MessageRewindOutputReceiptFromContext(ctx context.Context) *MessageRewindOutputReceipt {
	holder, _ := ctx.Value(messageRewindStoreKey{}).(messageRewindHolder)
	return holder.Output
}

// CheckMessageRewindOutput checks message rewind output.
func (s *KnowledgeInputProvenanceStore) CheckMessageRewindOutput(
	ctx context.Context,
	receipt *MessageRewindOutputReceipt,
) error {
	if receipt == nil || receipt.Parent == nil || receipt.PostPlan == nil {
		return types.ErrSourceLineageUnknown
	}
	return s.collectionTransaction(ctx, func(tx *gorm.DB) error {
		if _, _, _, err := s.currentMessageOwnerInTx(tx, receipt.Parent.Request.SessionID); err != nil {
			return err
		}
		current, err := s.peekMessageMaterialReadPlanInTx(tx, receipt.PostPlan.Request)
		if err != nil {
			return err
		}
		if !faqInputEqual(current, receipt.PostPlan) {
			return ErrKnowledgeInputChanged
		}
		isolated := context.WithValue(
			tx.Statement.Context,
			messageMaterialPlanKey{},
			[]*types.MessageMaterialReadPlan{},
		)
		marked, err := WithMessageMaterialReadPlan(isolated, receipt.PostPlan)
		if err != nil {
			return err
		}
		remaining, err := s.captureMessageMaterialInTx(tx.WithContext(marked), receipt.PostPlan.Request)
		if err != nil {
			return err
		}
		expectedRemaining := []*types.MessageMaterialProof{}
		removed := map[string]bool{}
		for _, proof := range receipt.Deleted {
			removed[proof.MessageID] = true
		}
		for _, proof := range receipt.Parent.Proofs {
			if !removed[proof.MessageID] {
				expectedRemaining = append(expectedRemaining, proof)
			}
		}
		if !faqInputEqual(remaining.Proofs, expectedRemaining) {
			return ErrKnowledgeInputChanged
		}
		for _, proof := range receipt.Deleted {
			if err := s.checkMessageOriginalBodyAvailability(tx, proof.GenerationID); err != nil {
				return err
			}
			var identity struct{ DeletedAt gorm.DeletedAt }
			if err := tx.Unscoped().
				Table("messages").
				Select("deleted_at").
				Where("id=? AND session_id=?", proof.MessageID, proof.SessionID).
				Take(&identity).
				Error; err != nil {
				return err
			}
			if !identity.DeletedAt.Valid {
				return ErrKnowledgeInputChanged
			}
			version, err := messageVersion(tx, "message_material_versions", "message_id", proof.MessageID)
			if err != nil {
				return err
			}
			if version != receipt.DeletedVersions[proof.MessageID] {
				return ErrKnowledgeInputChanged
			}
			var fact messageMaterialAdmission
			if err := tx.Table("message_material_admissions a").
				Select("a.*").
				Joins("JOIN message_material_receipts r ON r.admission_id=a.id AND r.message_id=a.message_id").
				Where("a.message_id=?", proof.MessageID).
				Take(&fact).
				Error; err != nil {
				return err
			}
			if fact.ID != proof.AdmissionID || fact.BodyDigest != proof.BodyDigest || fact.Revision != proof.Revision {
				return ErrKnowledgeInputChanged
			}
			fields, err := messageGeneratedFieldProofs(tx, proof.MessageID)
			if err != nil {
				return err
			}
			if !faqInputEqual(fields, proof.GeneratedFields) {
				return ErrKnowledgeInputChanged
			}
		}
		var fieldRows []struct {
			MessageID, Field string
			Revision         uint64
		}
		if err := tx.Table("message_generated_field_versions").
			Select("message_id,field,revision").
			Where("message_id IN ?", messageIDsFromProofs(receipt.Parent.Proofs)).
			Scan(&fieldRows).
			Error; err != nil {
			return err
		}
		fieldVersions := map[string]uint64{}
		for _, row := range fieldRows {
			fieldVersions[row.MessageID+":"+row.Field] = row.Revision
		}
		if !faqInputEqual(fieldVersions, receipt.FieldVersions) {
			return ErrKnowledgeInputChanged
		}
		if receipt.OriginalPlan == nil {
			return types.ErrSourceLineageUnknown
		}
		if len(receipt.OriginalPlan.Scopes) > 0 {
			return validateFAQReadLeaseCoverageInTx(tx, receipt.OriginalPlan.Scopes, receipt.OriginalPlan.SourceLineage)
		}
		return nil
	})
}

func messageIDsFromProofs(proofs []*types.MessageMaterialProof) []string {
	ids := []string{}
	for _, proof := range proofs {
		ids = append(ids, proof.MessageID)
	}
	return ids
}
