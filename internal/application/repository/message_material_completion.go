package repository

import (
	"context"
	"github.com/Tencent/WeKnora/internal/types"
	"gorm.io/gorm"
)

// This transfer requires an exact actual producer marker and immutable edge.
// It updates one excluded control; no selected parent's body is re-certified.
func (s *KnowledgeInputProvenanceStore) AdvanceMessageMaterialAfterOwnCompletion(ctx context.Context, expected *types.MessageMaterialSnapshot) (*types.MessageMaterialSnapshot, error) {
	marker, ok := ctx.Value(messageProducerAdvanceKey{}).(messageProducerAdvanceBoundary)
	if !ok || marker.Store != s || expected == nil || expected.Request.ExcludeRequestID == "" {
		return nil, types.ErrSourceLineageUnknown
	}
	var result *types.MessageMaterialSnapshot
	err := s.collectionTransaction(ctx, func(tx *gorm.DB) error {
		old := findMessageMaterialReadPlan(tx.Statement.Context, expected.Request)
		if old == nil {
			return ErrKnowledgeInputChanged
		}
		actual, err := s.peekMessageMaterialReadPlanInTx(tx, expected.Request)
		if err != nil {
			return err
		}
		if len(old.Controls) != len(actual.Controls) || !faqInputEqual(old.Scopes, actual.Scopes) || !faqInputEqual(old.SourceLineage, actual.SourceLineage) || !faqInputEqual(old.ParentPlans, actual.ParentPlans) {
			return ErrKnowledgeInputChanged
		}
		var edge messageProducerAdvance
		if err := tx.Table("message_producer_advances p").Select("p.*").Joins("JOIN message_material_receipts r ON r.admission_id=p.admission_id AND r.message_id=p.message_id").Where("p.message_id=? AND p.session_id=?", marker.MessageID, marker.SessionID).Take(&edge).Error; err != nil {
			return err
		}
		if edge.PreviousMembership != expected.MembershipRevision {
			return ErrKnowledgeInputChanged
		}
		found := false
		for i, before := range old.Controls {
			after := actual.Controls[i]
			if before.ID != marker.MessageID {
				if !faqInputEqual(before, after) {
					return ErrKnowledgeInputChanged
				}
				continue
			}
			found = true
			if before.RequestID != expected.Request.ExcludeRequestID || before.Role != "assistant" || before.Complete || !after.Complete || before.AdmissionID != edge.PreviousAdmissionID || before.Revision != edge.PreviousRevision || after.AdmissionID != edge.AdmissionID || after.Revision != edge.ResultRevision || before.GenerationID != edge.GenerationID || after.GenerationID != edge.GenerationID {
				return ErrKnowledgeInputChanged
			}
			a, b := *before, *after
			a.AdmissionID = ""
			b.AdmissionID = ""
			a.Revision = 0
			b.Revision = 0
			a.CurrentRevision = 0
			b.CurrentRevision = 0
			a.Complete = false
			b.Complete = false
			if !faqInputEqual(a, b) {
				return ErrKnowledgeInputChanged
			}
		}
		if !found {
			return ErrKnowledgeInputChanged
		}
		var admission messageMaterialAdmission
		if err := tx.Where("id=? AND message_id=?", edge.AdmissionID, marker.MessageID).Take(&admission).Error; err != nil {
			return err
		}
		if admission.BodyDigest != marker.Digest {
			return ErrKnowledgeInputChanged
		}
		if len(actual.Scopes) > 0 {
			if err := validateFAQReadLeaseCoverageInTx(tx, actual.Scopes, actual.SourceLineage); err != nil {
				return err
			}
		}
		isolated := context.WithValue(tx.Statement.Context, messageMaterialPlanKey{}, []*types.MessageMaterialReadPlan{})
		marked, err := WithMessageMaterialReadPlan(isolated, actual)
		if err != nil {
			return err
		}
		result, err = s.captureMessageMaterialInTx(tx.WithContext(marked), expected.Request)
		if err != nil {
			return err
		}
		a, b := *expected, *result
		a.MetadataDigest = ""
		b.MetadataDigest = ""
		a.Messages = nil
		b.Messages = nil
		if !faqInputEqual(a, b) {
			return ErrKnowledgeInputChanged
		}
		return nil
	})
	return result, err
}
