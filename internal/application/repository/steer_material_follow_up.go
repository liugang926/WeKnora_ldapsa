package repository

import (
	"context"

	"github.com/Tencent/WeKnora/internal/types"
	"gorm.io/gorm"
)

// AdvanceMessageMaterialAfterOwnFollowUpRow admits exactly one already sealed
// owned insertion. Every prior candidate/control and original body proof stays
// equal; the new request is excluded from the previous model's material.
func (s *KnowledgeInputProvenanceStore) AdvanceMessageMaterialAfterOwnFollowUpRow(
	ctx context.Context,
	expected *types.MessageMaterialSnapshot,
	id, generation, admission, request, role string,
) (*types.MessageMaterialSnapshot, error) {
	if expected == nil || id == "" || generation == "" || request == "" ||
		request == expected.Request.ExcludeRequestID ||
		role != "assistant" && role != "user" {
		return nil, types.ErrSourceLineageUnknown
	}
	var result *types.MessageMaterialSnapshot
	err := s.collectionTransaction(ctx, func(tx *gorm.DB) error {
		old := findMessageMaterialReadPlan(tx.Statement.Context, expected.Request)
		if old == nil {
			return ErrKnowledgeInputChanged
		}
		var inserted types.MessageMaterialCandidateControl
		if err := tx.Table("messages m").
			Select("m.id,m.request_id,m.role,m.is_completed AS complete,m.created_at,a.id AS admission_id,a.generation_id,a.revision,v.revision AS current_revision").
			Joins("JOIN message_material_receipts r ON r.message_id=m.id").
			Joins("JOIN message_material_admissions a ON a.id=r.admission_id AND a.message_id=m.id").
			Joins("JOIN message_material_versions v ON v.message_id=m.id").
			Where("m.id=? AND m.session_id=? AND m.deleted_at IS NULL", id, expected.Request.SessionID).
			Take(&inserted).
			Error; err != nil {
			return err
		}
		if inserted.GenerationID != generation || inserted.RequestID != request || inserted.Role != role ||
			inserted.Revision != 1 ||
			inserted.CurrentRevision != 1 ||
			inserted.Complete != (role == "user") ||
			admission != "" && inserted.AdmissionID != admission {
			return ErrKnowledgeInputChanged
		}
		req := expected.Request
		req.ExcludeRequestIDs = append([]string{}, req.ExcludeRequestIDs...)
		found := false
		for _, existing := range req.ExcludeRequestIDs {
			found = found || existing == request
		}
		if !found {
			req.ExcludeRequestIDs = append(req.ExcludeRequestIDs, request)
		}
		current, err := s.peekMessageMaterialReadPlanInTx(tx, req)
		if err != nil {
			return err
		}
		if !faqInputEqual(old.Scopes, current.Scopes) || !faqInputEqual(old.SourceLineage, current.SourceLineage) ||
			!faqInputEqual(old.ParentPlans, current.ParentPlans) {
			return ErrKnowledgeInputChanged
		}
		controls := map[string]*types.MessageMaterialCandidateControl{}
		for _, control := range current.Controls {
			controls[control.ID] = control
		}
		for _, control := range old.Controls {
			if !faqInputEqual(control, controls[control.ID]) {
				return ErrKnowledgeInputChanged
			}
			delete(controls, control.ID)
		}
		if old.Request.AllSession {
			if len(controls) != 1 || !faqInputEqual(&inserted, controls[id]) {
				return ErrKnowledgeInputChanged
			}
		} else if len(controls) != 0 {
			return ErrKnowledgeInputChanged
		}
		isolated := context.WithValue(
			tx.Statement.Context,
			messageMaterialPlanKey{},
			[]*types.MessageMaterialReadPlan{},
		)
		marked, err := WithMessageMaterialReadPlan(isolated, current)
		if err != nil {
			return err
		}
		result, err = s.captureMessageMaterialInTx(tx.WithContext(marked), req)
		if err != nil {
			return err
		}
		if result.MembershipRevision != expected.MembershipRevision+1 {
			return ErrKnowledgeInputChanged
		}
		a, b := *expected, *result
		a.Request, b.Request = types.MessageMaterialReadRequest{}, types.MessageMaterialReadRequest{}
		a.MetadataDigest, b.MetadataDigest = "", ""
		a.MembershipRevision, b.MembershipRevision = 0, 0
		a.Messages, b.Messages = nil, nil
		if !faqInputEqual(a, b) {
			return ErrKnowledgeInputChanged
		}
		return nil
	})
	return result, err
}
