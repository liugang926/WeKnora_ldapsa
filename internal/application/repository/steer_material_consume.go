package repository

import (
	"context"
	"encoding/json"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

type steerConsumptionOrigin struct {
	ID, OriginID, SelectionManifest, PreviousHeadID, ResultHeadID, UserMessageID, UserGenerationID, UserAdmissionID, UserBodyDigest, SessionID, RunMessageID, RunGenerationID, RunRequestID string
	PreviousMembershipRevision, ResultMembershipRevision                                                                                                                                    uint64
}

func (steerConsumptionOrigin) TableName() string { return "steer_consumption_origins" }

type (
	steerConsumptionKey      struct{}
	steerConsumptionBoundary struct {
		Store       *KnowledgeInputProvenanceStore
		OriginalID  string
		InputDigest string
	}
)

// ConsumeSteerIntoMessage returns the consume steer into message result.
func (s *KnowledgeInputProvenanceStore) ConsumeSteerIntoMessage(
	ctx context.Context,
	input *types.SteerMaterialInput,
	runID, generation, request string,
) (*types.Message, *types.SteerConsumptionProof, error) {
	return s.consumeSteerIntoMessage(ctx, input, runID, generation, request, false)
}

// ConsumeSteerIntoFollowUp returns the consume steer into follow up result.
func (s *KnowledgeInputProvenanceStore) ConsumeSteerIntoFollowUp(
	ctx context.Context,
	input *types.SteerMaterialInput,
	runID, generation, request string,
) (*types.Message, *types.SteerConsumptionProof, error) {
	return s.consumeSteerIntoMessage(ctx, input, runID, generation, request, true)
}

func (s *KnowledgeInputProvenanceStore) consumeSteerIntoMessage(
	ctx context.Context,
	input *types.SteerMaterialInput,
	runID, generation, request string,
	followUp bool,
) (*types.Message, *types.SteerConsumptionProof, error) {
	if input == nil || input.Proof == nil || input.Control == nil {
		return nil, nil, types.ErrSourceLineageUnknown
	}
	var result *types.Message
	var transition *types.SteerConsumptionProof
	err := s.steerWriteTransaction(ctx, func(tx *gorm.DB) error {
		boundary, ok := tx.Statement.Context.Value(steerReadBoundaryKey{}).(steerReadBoundary)
		if !ok {
			return ErrOriginalBodyUnavailable
		}
		snapshot, err := s.captureSteerMaterialInTx(tx, boundary.Plan)
		if err != nil {
			return err
		}
		var original *types.SteerMaterialInput
		for _, candidate := range snapshot.Inputs {
			if candidate.Proof.OriginID == input.Proof.OriginID {
				original = candidate
			}
		}
		if original == nil || !steerInputEqual(original, input) {
			return ErrKnowledgeInputChanged
		}
		run, currentGeneration, err := s.messageLiveControlInTx(tx, input.Proof.SessionID, runID)
		if err != nil {
			return err
		}
		if run.IsCompleted || currentGeneration != generation || run.RequestID != request {
			return ErrKnowledgeInputChanged
		}
		if original.Control.Consumed || original.Control.Discarded || original.Control.Deleted {
			return ErrKnowledgeInputChanged
		}
		if followUp {
			if boundary.Plan.Request.Use != types.SteerMaterialFollowUp ||
				original.Control.RunMessageID != boundary.Plan.Request.RunMessageID ||
				original.Control.RunGenerationID != boundary.Plan.Request.RunGenerationID ||
				original.Control.RunMessageID == runID ||
				original.Control.RunRequestID == request {
				return ErrKnowledgeInputChanged
			}
		} else if original.Control.RunMessageID != runID || original.Control.RunGenerationID != generation || original.Control.RunRequestID != request || original.Control.Delivery != types.SteerDeliveryInject {
			return ErrKnowledgeInputChanged
		}
		before, err := messageVersion(tx, "message_session_membership_versions", "session_id", input.Proof.SessionID)
		if err != nil {
			return err
		}
		message := &types.Message{
			ID:             uuid.NewString(),
			SessionID:      input.Proof.SessionID,
			RequestID:      request,
			Role:           "user",
			Content:        original.Query,
			MentionedItems: original.MentionedItems,
			Channel:        original.Channel,
			IsCompleted:    true,
			SourceLineage:  original.Proof.SourceLineage,
		}
		marked, err := s.WithMessageProducerInput(tx.Statement.Context, message, true)
		if err != nil {
			return err
		}
		marked = context.WithValue(
			marked,
			steerConsumptionKey{},
			steerConsumptionBoundary{s, original.Proof.OriginID, original.Proof.BodyDigest},
		)
		origin, err := s.beginMessageProducerInTx(tx.WithContext(marked), message, true)
		if err != nil {
			return err
		}
		if err := tx.WithContext(marked).Create(message).Error; err != nil {
			return err
		}
		var actual types.Message
		if err := tx.WithContext(marked).Where("id=?", message.ID).Take(&actual).Error; err != nil {
			return err
		}
		if err := s.sealMessageMaterialInTx(tx.WithContext(marked), origin, &actual); err != nil {
			return err
		}
		if err := s.sealInitialUserFieldsInTx(tx.WithContext(marked), origin, &actual); err != nil {
			return err
		}
		after, err := messageVersion(tx, "message_session_membership_versions", "session_id", message.SessionID)
		if err != nil {
			return err
		}
		if after != before+1 {
			return ErrKnowledgeInputChanged
		}
		var admission messageMaterialAdmission
		if err := tx.Table("message_material_admissions a").
			Select("a.*").
			Joins("JOIN message_material_receipts r ON r.admission_id=a.id AND r.message_id=a.message_id").
			Where("a.message_id=?", message.ID).
			Take(&admission).
			Error; err != nil {
			return err
		}
		control, err := s.steerCurrentControlInTx(tx, original.Proof.OriginID)
		if err != nil {
			return err
		}
		next := *control
		next.ID = uuid.NewString()
		next.PreviousHeadID = control.ID
		next.Revision++
		next.Operation = string(types.SteerControlConsume)
		next.Consumed = true
		next.UserMessageID = message.ID
		if followUp {
			next.RunMessageID, next.RunGenerationID, next.RunRequestID = runID, generation, request
		}
		next.CreatedAtMS, err = nextcloudLeaseNowMS(tx)
		if err != nil {
			return err
		}
		if err := tx.Create(&next).Error; err != nil {
			return err
		}
		changed := tx.Model(&steerControlHead{}).
			Where("origin_id=? AND head_id=? AND revision=?", next.OriginID, control.ID, control.Revision).
			Updates(map[string]any{"head_id": next.ID, "revision": next.Revision})
		if changed.Error != nil {
			return changed.Error
		}
		if changed.RowsAffected != 1 {
			return ErrKnowledgeInputChanged
		}
		selection, err := json.Marshal(struct {
			Plan   *types.SteerMaterialPlan
			Source *types.SourceLineage
		}{snapshot.Plan, snapshot.Plan.SourceLineage})
		if err != nil {
			return err
		}
		edge := steerConsumptionOrigin{
			ID:                         uuid.NewString(),
			OriginID:                   original.Proof.OriginID,
			SelectionManifest:          string(selection),
			PreviousHeadID:             control.ID,
			ResultHeadID:               next.ID,
			UserMessageID:              message.ID,
			UserGenerationID:           origin.ID,
			UserAdmissionID:            admission.ID,
			UserBodyDigest:             admission.BodyDigest,
			SessionID:                  message.SessionID,
			RunMessageID:               runID,
			RunGenerationID:            generation,
			RunRequestID:               request,
			PreviousMembershipRevision: before,
			ResultMembershipRevision:   after,
		}
		if err := tx.Create(&edge).Error; err != nil {
			return err
		}
		result = &actual
		transition = steerConsumptionProof(&edge, control.Revision, next.Revision)
		return nil
	})
	return result, transition, err
}

func steerInputEqual(a, b *types.SteerMaterialInput) bool {
	type material struct {
		Input       *types.SteerMaterialInput
		Source      *types.SourceLineage
		Execution   types.MessageExecutionContext
		AgentTenant uint64
	}
	x, _ := json.Marshal(material{a, a.Proof.SourceLineage, a.Proof.ExecutionContext, a.Proof.AgentTenantID})
	y, _ := json.Marshal(material{b, b.Proof.SourceLineage, b.Proof.ExecutionContext, b.Proof.AgentTenantID})
	return string(x) == string(y)
}

func steerConsumptionProof(edge *steerConsumptionOrigin, previous, result uint64) *types.SteerConsumptionProof {
	return &types.SteerConsumptionProof{
		OriginalAdmissionID:        edge.OriginID,
		PreviousHeadID:             edge.PreviousHeadID,
		ResultHeadID:               edge.ResultHeadID,
		PreviousRevision:           previous,
		ResultRevision:             result,
		UserMessageID:              edge.UserMessageID,
		UserGenerationID:           edge.UserGenerationID,
		UserAdmissionID:            edge.UserAdmissionID,
		UserBodyDigest:             edge.UserBodyDigest,
		SessionID:                  edge.SessionID,
		RunMessageID:               edge.RunMessageID,
		RunGenerationID:            edge.RunGenerationID,
		RunRequestID:               edge.RunRequestID,
		PreviousMembershipRevision: edge.PreviousMembershipRevision,
		ResultMembershipRevision:   edge.ResultMembershipRevision,
	}
}

// AdvanceMessageMaterialAfterOwnSteerInsertion advances message material after own steer insertion.
func (s *KnowledgeInputProvenanceStore) AdvanceMessageMaterialAfterOwnSteerInsertion(
	ctx context.Context,
	expected *types.MessageMaterialSnapshot,
	proof *types.SteerConsumptionProof,
) (*types.MessageMaterialSnapshot, error) {
	if expected == nil || proof == nil || expected.Request.SessionID != proof.SessionID ||
		expected.Request.ExcludeRequestID != "" && expected.Request.ExcludeRequestID != proof.RunRequestID {
		return nil, types.ErrSourceLineageUnknown
	}
	var result *types.MessageMaterialSnapshot
	err := s.collectionTransaction(ctx, func(tx *gorm.DB) error {
		var edge steerConsumptionOrigin
		if err := tx.Where("origin_id=? AND user_message_id=? AND result_head_id=?", proof.OriginalAdmissionID, proof.UserMessageID, proof.ResultHeadID).
			Take(&edge).
			Error; err != nil {
			return err
		}
		if !faqInputEqual(steerConsumptionProof(&edge, proof.PreviousRevision, proof.ResultRevision), proof) ||
			edge.PreviousMembershipRevision != expected.MembershipRevision ||
			edge.ResultMembershipRevision != expected.MembershipRevision+1 {
			return ErrKnowledgeInputChanged
		}
		old := findMessageMaterialReadPlan(tx.Statement.Context, expected.Request)
		if old == nil {
			return ErrKnowledgeInputChanged
		}
		current, err := s.peekMessageMaterialReadPlanInTx(tx, expected.Request)
		if err != nil {
			return err
		}
		if !faqInputEqual(old.Scopes, current.Scopes) || !faqInputEqual(old.SourceLineage, current.SourceLineage) ||
			!faqInputEqual(old.ParentPlans, current.ParentPlans) {
			return ErrKnowledgeInputChanged
		}
		if old.Request.AllSession && len(current.Controls) != len(old.Controls)+1 ||
			!old.Request.AllSession && len(current.Controls) != len(old.Controls) {
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
		var inserted *types.MessageMaterialCandidateControl
		if old.Request.AllSession {
			inserted = controls[proof.UserMessageID]
			if len(controls) != 1 {
				return ErrKnowledgeInputChanged
			}
		} else {
			if len(controls) != 0 {
				return ErrKnowledgeInputChanged
			}
			var actual types.MessageMaterialCandidateControl
			if err := tx.Table("messages m").
				Select(("m.id,m.request_id,m.role,m.is_completed AS complete,a.id AS admission_id,a" +
					".generation_id,a.revision,v.revision AS current_revision")).
				Joins("JOIN message_material_receipts r ON r.message_id=m.id").
				Joins("JOIN message_material_admissions a ON a.id=r.admission_id").
				Joins("JOIN message_material_versions v ON v.message_id=m.id").
				Where("m.id=? AND m.session_id=?", proof.UserMessageID, proof.SessionID).
				Take(&actual).
				Error; err != nil {
				return err
			}
			inserted = &actual
		}
		if inserted == nil || inserted.AdmissionID != proof.UserAdmissionID ||
			inserted.GenerationID != proof.UserGenerationID ||
			inserted.RequestID != proof.RunRequestID ||
			inserted.Role != "user" ||
			!inserted.Complete {
			return ErrKnowledgeInputChanged
		}
		if len(current.Scopes) > 0 {
			if err := validateFAQReadLeaseCoverageInTx(tx, current.Scopes, current.SourceLineage); err != nil {
				return err
			}
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
		result, err = s.captureMessageMaterialInTx(tx.WithContext(marked), expected.Request)
		if err != nil {
			return err
		}
		a, b := *expected, *result
		a.MetadataDigest = ""
		b.MetadataDigest = ""
		a.MembershipRevision = 0
		b.MembershipRevision = 0
		a.Messages = nil
		b.Messages = nil
		if !faqInputEqual(a, b) {
			return ErrKnowledgeInputChanged
		}
		return nil
	})
	return result, err
}

func (s *KnowledgeInputProvenanceStore) verifySteerConsumedMessageInTx(tx *gorm.DB, message *types.Message) error {
	var edge steerConsumptionOrigin
	err := tx.Where("user_message_id=?", message.ID).Take(&edge).Error
	if err == gorm.ErrRecordNotFound {
		return nil
	}
	if err != nil {
		return err
	}
	if err := verifySteerConsumptionSelectionInTx(tx, &edge); err != nil {
		return err
	}
	var before, consumed steerControlAdmission
	if err := tx.Where("id=? AND origin_id=?", edge.PreviousHeadID, edge.OriginID).Take(&before).Error; err != nil {
		return err
	}
	if err := tx.Where("id=? AND origin_id=?", edge.ResultHeadID, edge.OriginID).Take(&consumed).Error; err != nil {
		return err
	}
	if err := s.checkSteerConsumptionInTx(
		tx,
		steerConsumptionProof(&edge, before.Revision, consumed.Revision),
	); err != nil {
		return err
	}
	if message.Role != "user" || message.SessionID != edge.SessionID || message.RequestID != edge.RunRequestID {
		return ErrKnowledgeInputChanged
	}
	var original steerAuthoredOrigin
	if err := tx.Select(steerOriginMetadataFields).Where("id=?", edge.OriginID).Take(&original).Error; err != nil {
		return err
	}
	if err := checkSteerOriginalWindow(tx, &original); err != nil {
		return err
	}
	if err := CheckOriginalBodyAvailableInTx(
		tx.Statement.Context,
		tx,
		"steer_authored_origins",
		edge.OriginID,
	); err != nil {
		return err
	}
	var fact messageMaterialAdmission
	if err := tx.Table("message_material_admissions a").
		Select("a.*").
		Joins("JOIN message_material_receipts r ON r.admission_id=a.id AND r.message_id=a.message_id").
		Where("a.message_id=?", message.ID).
		Take(&fact).
		Error; err != nil {
		return err
	}
	if fact.GenerationID != edge.UserGenerationID || fact.ID != edge.UserAdmissionID ||
		fact.BodyDigest != edge.UserBodyDigest {
		return ErrKnowledgeInputChanged
	}
	return nil
}

// CheckSteerConsumption checks steer consumption.
func (s *KnowledgeInputProvenanceStore) CheckSteerConsumption(
	ctx context.Context,
	proof *types.SteerConsumptionProof,
) error {
	if proof == nil {
		return types.ErrSourceLineageUnknown
	}
	return s.collectionTransaction(ctx, func(tx *gorm.DB) error { return s.checkSteerConsumptionInTx(tx, proof) })
}

func (s *KnowledgeInputProvenanceStore) checkSteerConsumptionInTx(
	tx *gorm.DB,
	proof *types.SteerConsumptionProof,
) error {
	var edge steerConsumptionOrigin
	if err := tx.Where("origin_id=? AND user_message_id=? AND result_head_id=?", proof.OriginalAdmissionID, proof.UserMessageID, proof.ResultHeadID).
		Take(&edge).
		Error; err != nil {
		return err
	}
	if !faqInputEqual(steerConsumptionProof(&edge, proof.PreviousRevision, proof.ResultRevision), proof) {
		return ErrKnowledgeInputChanged
	}
	if err := verifySteerConsumptionSelectionInTx(tx, &edge); err != nil {
		return err
	}
	if _, _, _, err := s.currentMessageOwnerInTx(tx, proof.SessionID); err != nil {
		return err
	}
	current, err := s.steerCurrentControlInTx(tx, edge.OriginID)
	if err != nil {
		return err
	}
	if current.ID != proof.ResultHeadID || current.Revision != proof.ResultRevision || !current.Consumed ||
		current.Discarded ||
		current.Deleted ||
		current.UserMessageID != proof.UserMessageID ||
		current.RunMessageID != proof.RunMessageID ||
		current.RunGenerationID != proof.RunGenerationID ||
		current.RunRequestID != proof.RunRequestID {
		return ErrKnowledgeInputChanged
	}
	return CheckOriginalBodyAvailableInTx(tx.Statement.Context, tx, "steer_authored_origins", edge.OriginID)
}

// ExistingSteerConsumption returns the existing steer consumption result.
func (s *KnowledgeInputProvenanceStore) ExistingSteerConsumption(
	ctx context.Context,
	input *types.SteerMaterialInput,
	run, generation, request string,
) (*types.SteerConsumptionProof, error) {
	if input == nil || input.Proof == nil || input.Control == nil {
		return nil, types.ErrSourceLineageUnknown
	}
	var result *types.SteerConsumptionProof
	err := s.collectionTransaction(ctx, func(tx *gorm.DB) error {
		if _, _, _, err := s.currentMessageOwnerInTx(tx, input.Proof.SessionID); err != nil {
			return err
		}
		var edge steerConsumptionOrigin
		err := tx.Where("origin_id=?", input.Proof.OriginID).Take(&edge).Error
		if err == gorm.ErrRecordNotFound {
			return nil
		}
		if err != nil {
			return err
		}
		if edge.RunMessageID != run || edge.RunGenerationID != generation || edge.RunRequestID != request ||
			edge.PreviousHeadID != input.Control.HeadID {
			return ErrKnowledgeInputChanged
		}
		var origin steerAuthoredOrigin
		if err := tx.Select(steerOriginMetadataFields).Where("id=?", edge.OriginID).Take(&origin).Error; err != nil {
			return err
		}
		var first steerControlAdmission
		if err := tx.Where("origin_id=? AND revision=1", edge.OriginID).Take(&first).Error; err != nil {
			return err
		}
		body := steerAuthoredBody{
			Query:          input.Query,
			MentionedItems: input.MentionedItems,
			Channel:        input.Channel,
			Delivery:       types.SteerDelivery(first.Delivery),
		}
		digest, err := steerCanonicalDigest(&body)
		if err != nil {
			return err
		}
		if digest != origin.CanonicalDigest {
			return ErrKnowledgeInputChanged
		}
		var before, after steerControlAdmission
		if err := tx.Where("id=?", edge.PreviousHeadID).Take(&before).Error; err != nil {
			return err
		}
		if err := tx.Where("id=?", edge.ResultHeadID).Take(&after).Error; err != nil {
			return err
		}
		result = steerConsumptionProof(&edge, before.Revision, after.Revision)
		return CheckOriginalBodyAvailableInTx(tx.Statement.Context, tx, "steer_authored_origins", edge.OriginID)
	})
	return result, err
}

func verifySteerConsumptionSelectionInTx(tx *gorm.DB, edge *steerConsumptionOrigin) error {
	var manifest struct {
		Plan   *types.SteerMaterialPlan
		Source *types.SourceLineage
	}
	if err := json.Unmarshal([]byte(edge.SelectionManifest), &manifest); err != nil {
		return err
	}
	if manifest.Plan == nil || manifest.Plan.MetadataDigest == "" || manifest.Source.RequireComplete() != nil ||
		manifest.Plan.Request.SessionID != edge.SessionID {
		return types.ErrSourceLineageUnknown
	}
	found := false
	for _, selected := range manifest.Plan.SelectedSteerIDs {
		for _, candidate := range manifest.Plan.CandidateControls {
			if candidate.SteerID == selected && candidate.OriginalAdmissionID == edge.OriginID {
				found = true
			}
		}
	}
	if !found {
		return ErrKnowledgeInputChanged
	}
	for _, candidate := range manifest.Plan.CandidateControls {
		var origin steerAuthoredOrigin
		if err := tx.Select(steerOriginMetadataFields).
			Where("id=? AND session_id=?", candidate.OriginalAdmissionID, edge.SessionID).
			Take(&origin).
			Error; err != nil {
			return err
		}
		var control steerControlAdmission
		if err := tx.Where("id=? AND origin_id=?", candidate.HeadID, origin.ID).Take(&control).Error; err != nil {
			return err
		}
		if !faqInputEqual(steerCandidate(&control, &origin), candidate) {
			return ErrKnowledgeInputChanged
		}
	}
	return nil
}

// SteerConsumptionSelectionDigest returns the steer consumption selection digest result.
func (s *KnowledgeInputProvenanceStore) SteerConsumptionSelectionDigest(
	ctx context.Context,
	proof *types.SteerConsumptionProof,
) (string, error) {
	var edge steerConsumptionOrigin
	if err := s.db.WithContext(ctx).
		Select("selection_manifest").
		Where("origin_id=? AND user_message_id=? AND result_head_id=?", proof.OriginalAdmissionID, proof.UserMessageID, proof.ResultHeadID).
		Take(&edge).
		Error; err != nil {
		return "", err
	}
	return KnowledgeInputDigest(json.RawMessage(edge.SelectionManifest))
}
