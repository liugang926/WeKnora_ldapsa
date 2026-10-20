package repository

import (
	"context"
	"encoding/json"
	"sort"
	"strings"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"gorm.io/gorm"
)

type (
	steerReadBoundaryKey struct{}
	steerReadBoundary    struct {
		Plan       *types.SteerMaterialPlan
		BodyLeases []string
	}
)

// WithSteerMaterialReadPlan binds steer material read plan.
func WithSteerMaterialReadPlan(
	ctx context.Context,
	plan *types.SteerMaterialPlan,
	bodyLeases []string,
) (context.Context, error) {
	if plan == nil || plan.MetadataDigest == "" || plan.SourceLineage.RequireComplete() != nil {
		return ctx, types.ErrSourceLineageUnknown
	}
	raw, err := json.Marshal(plan)
	if err != nil {
		return ctx, err
	}
	var copied types.SteerMaterialPlan
	if err := json.Unmarshal(raw, &copied); err != nil {
		return ctx, err
	}
	copied.SourceLineage, err = types.UnionSourceLineage(plan.SourceLineage)
	if err != nil {
		return ctx, err
	}
	for i, proof := range plan.OriginalProofs {
		copied.OriginalProofs[i].SourceLineage, err = types.UnionSourceLineage(proof.SourceLineage)
		if err != nil {
			return ctx, err
		}
		encoded, _ := json.Marshal(proof.ExecutionContext)
		if err := json.Unmarshal(encoded, &copied.OriginalProofs[i].ExecutionContext); err != nil {
			return ctx, err
		}
		copied.OriginalProofs[i].AgentTenantID = proof.AgentTenantID
	}
	return context.WithValue(
		ctx,
		steerReadBoundaryKey{},
		steerReadBoundary{&copied, append([]string{}, bodyLeases...)},
	), nil
}

// PeekSteerMaterialPlan reads steer material plan.
func (s *KnowledgeInputProvenanceStore) PeekSteerMaterialPlan(
	ctx context.Context,
	req *types.SteerMaterialRequest,
) (*types.SteerMaterialPlan, error) {
	var result *types.SteerMaterialPlan
	err := s.collectionTransaction(
		ctx,
		func(tx *gorm.DB) error { var err error; result, err = s.peekSteerMaterialPlanInTx(tx, req); return err },
	)
	return result, err
}

func (s *KnowledgeInputProvenanceStore) peekSteerMaterialPlanInTx(
	tx *gorm.DB,
	req *types.SteerMaterialRequest,
) (*types.SteerMaterialPlan, error) {
	if req == nil || req.TenantID == 0 || req.SessionID == "" || req.RunMessageID == "" || req.RunGenerationID == "" {
		return nil, types.ErrSourceLineageUnknown
	}
	_, session, ownerRev, err := s.currentMessageOwnerInTx(tx, req.SessionID)
	if err != nil {
		return nil, err
	}
	if session.TenantID != req.TenantID {
		return nil, ErrKnowledgeInputChanged
	}
	run, generation, err := s.messageLiveControlInTx(tx, req.SessionID, req.RunMessageID)
	if err != nil {
		return nil, err
	}
	if generation != req.RunGenerationID {
		return nil, ErrKnowledgeInputChanged
	}
	if req.Use != types.SteerMaterialFollowUp && run.IsCompleted {
		return nil, ErrKnowledgeInputChanged
	}
	var origins []steerAuthoredOrigin
	if err := tx.Table("steer_authored_origins o").
		Select("o."+strings.ReplaceAll(steerOriginMetadataFields, ",", ",o.")).
		Joins("JOIN steer_control_heads h ON h.origin_id=o.id").
		Joins("JOIN steer_control_admissions c ON c.id=h.head_id AND c.origin_id=o.id").
		Where(("o.tenant_id=? AND o.session_id=? AND c.run_message_id=? AND c.run_generati" +
			"on_id=?"), req.TenantID, req.SessionID, req.RunMessageID, req.RunGenerationID).
		Order("o.created_at_ms,o.id").
		Find(&origins).
		Error; err != nil {
		return nil, err
	}
	result := &types.SteerMaterialPlan{
		Request:           *req,
		OwnerID:           session.UserID,
		OwnerRevision:     ownerRev,
		CandidateControls: []*types.SteerCandidateControl{},
		SelectedSteerIDs:  []string{},
		OriginalProofs:    []*types.SteerOriginalProof{},
	}
	sources := []*types.SourceLineage{}
	metadata := []any{}
	wanted := map[string]bool{}
	for _, id := range req.SteerIDs {
		wanted[id] = true
	}
	for _, origin := range origins {
		if origin.OwnerID != session.UserID || origin.OwnerRevision != ownerRev {
			return nil, ErrKnowledgeInputChanged
		}
		control, err := s.steerCurrentControlInTx(tx, origin.ID)
		if err != nil {
			return nil, err
		}
		input, err := steerInputFromOriginal(&origin, control, &steerAuthoredBody{})
		if err != nil {
			return nil, err
		}
		result.CandidateControls = append(result.CandidateControls, input.Control)
		result.OriginalProofs = append(result.OriginalProofs, input.Proof)
		sources = append(sources, input.Proof.SourceLineage)
		metadata = append(metadata, struct {
			Origin  steerAuthoredOrigin
			Control *steerControlAdmission
		}{origin, control})
		selected := !control.Consumed && !control.Discarded && !control.Deleted
		if req.Use == types.SteerMaterialInject {
			selected = selected && types.SteerDelivery(control.Delivery) == types.SteerDeliveryInject
		}
		if len(wanted) > 0 {
			selected = selected && wanted[origin.SteerID]
		}
		if selected {
			if err := checkSteerOriginalWindow(tx, &origin); err != nil {
				return nil, err
			}
			result.SelectedSteerIDs = append(result.SelectedSteerIDs, origin.SteerID)
		}
	}
	if len(sources) == 0 {
		result.SourceLineage, err = types.NewCompleteSourceLineage()
	} else {
		result.SourceLineage, err = types.UnionSourceLineage(sources...)
	}
	if err != nil {
		return nil, err
	}
	result.MetadataDigest, err = KnowledgeInputDigest(struct {
		Request  *types.SteerMaterialRequest
		Owner    string
		OwnerRev uint64
		Facts    []any
	}{req, session.UserID, ownerRev, metadata})
	if err != nil {
		return nil, err
	}
	scopes := map[types.KnowledgeSearchScope]bool{}
	for _, source := range result.SourceLineage.Sources {
		scopes[types.KnowledgeSearchScope{TenantID: source.TenantID, KBID: source.KnowledgeBaseID}] = true
	}
	for scope := range scopes {
		result.Scopes = append(result.Scopes, scope)
	}
	sort.Slice(result.Scopes, func(i, j int) bool {
		if result.Scopes[i].TenantID != result.Scopes[j].TenantID {
			return result.Scopes[i].TenantID < result.Scopes[j].TenantID
		}
		return result.Scopes[i].KBID < result.Scopes[j].KBID
	})
	return result, nil
}

// AcquireSteerOriginalBodyLeases acquires steer original body leases.
func (s *KnowledgeInputProvenanceStore) AcquireSteerOriginalBodyLeases(
	ctx context.Context,
	plan *types.SteerMaterialPlan,
	owner string,
	ttl time.Duration,
) ([]string, error) {
	if plan == nil || owner == "" || ttl <= 0 {
		return nil, ErrOriginalBodyUnavailable
	}
	ids := []string{}
	err := s.steerWriteTransaction(ctx, func(tx *gorm.DB) error {
		current, err := s.peekSteerMaterialPlanInTx(tx, &plan.Request)
		if err != nil {
			return err
		}
		if current.MetadataDigest != plan.MetadataDigest {
			return ErrKnowledgeInputChanged
		}
		if !originalBodySchemaPresent(tx) {
			return ErrOriginalBodyUnavailable
		}
		retention := NewOriginalBodyRetentionStore(tx)
		if err := retention.checkJournalInTx(tx); err != nil {
			return err
		}
		selected := map[string]bool{}
		for _, id := range plan.SelectedSteerIDs {
			selected[id] = true
		}
		for _, proof := range plan.OriginalProofs {
			if !selected[proof.SteerID] {
				continue
			}
			var heads []originalBodyFieldHead
			if err := bodyInternal(
				tx,
			).Where("source_table='steer_authored_origins' AND row_key=?", proof.OriginID).
				Find(&heads).
				Error; err != nil {
				return err
			}
			if len(heads) == 0 {
				return ErrOriginalBodyUnavailable
			}
			for _, head := range heads {
				id, err := retention.acquireLeaseInTx(tx, head.BodyID, "read", owner, ttl)
				if err != nil {
					return err
				}
				ids = append(ids, id)
			}
		}
		return nil
	})
	return ids, err
}

// CaptureSteerMaterial captures steer material.
func (s *KnowledgeInputProvenanceStore) CaptureSteerMaterial(
	ctx context.Context,
	plan *types.SteerMaterialPlan,
) (*types.SteerMaterialSnapshot, error) {
	var result *types.SteerMaterialSnapshot
	err := s.collectionTransaction(
		ctx,
		func(tx *gorm.DB) error { var err error; result, err = s.captureSteerMaterialInTx(tx, plan); return err },
	)
	return result, err
}

func (s *KnowledgeInputProvenanceStore) captureSteerMaterialInTx(
	tx *gorm.DB,
	expected *types.SteerMaterialPlan,
) (*types.SteerMaterialSnapshot, error) {
	boundary, ok := tx.Statement.Context.Value(steerReadBoundaryKey{}).(steerReadBoundary)
	if !ok || expected == nil || boundary.Plan.MetadataDigest != expected.MetadataDigest {
		return nil, ErrOriginalBodyUnavailable
	}
	current, err := s.peekSteerMaterialPlanInTx(tx, &expected.Request)
	if err != nil {
		return nil, err
	}
	if current.MetadataDigest != expected.MetadataDigest {
		return nil, ErrKnowledgeInputChanged
	}
	if len(current.Scopes) > 0 {
		if err := validateFAQReadLeaseCoverageInTx(tx, current.Scopes, current.SourceLineage); err != nil {
			return nil, err
		}
	}
	now, err := nextcloudLeaseNowMS(tx)
	if err != nil {
		return nil, err
	}
	result := &types.SteerMaterialSnapshot{Plan: current, Inputs: []*types.SteerMaterialInput{}}
	selected := map[string]bool{}
	for _, id := range current.SelectedSteerIDs {
		selected[id] = true
	}
	for _, proof := range current.OriginalProofs {
		if !selected[proof.SteerID] {
			continue
		}
		var head originalBodyFieldHead
		if err := bodyInternal(
			tx,
		).Where("source_table='steer_authored_origins' AND row_key=? AND column_name='input_body'", proof.OriginID).
			Take(&head).
			Error; err != nil {
			return nil, err
		}
		var count int64
		if err := bodyInternal(
			tx,
		).Model(&originalBodyLease{}).
			Where(("lease_id IN ? AND body_id=? AND kind='read' AND released_at_ms IS NULL AND" +
				" expires_at_ms>?"), boundary.BodyLeases, head.BodyID, now).
			Count(&count).
			Error; err != nil {
			return nil, err
		}
		if count == 0 {
			return nil, ErrOriginalBodyUnavailable
		}
		if err := CheckOriginalBodyAvailableInTx(
			tx.Statement.Context,
			tx,
			"steer_authored_origins",
			proof.OriginID,
		); err != nil {
			return nil, err
		}
		var origin steerAuthoredOrigin
		if err := tx.Where("id=?", proof.OriginID).Take(&origin).Error; err != nil {
			return nil, err
		}
		var body steerAuthoredBody
		if err := json.Unmarshal([]byte(origin.InputBody), &body); err != nil {
			return nil, err
		}
		digest, err := KnowledgeInputDigest(body)
		if err != nil {
			return nil, err
		}
		if digest != origin.BodyDigest {
			return nil, ErrKnowledgeInputChanged
		}
		control, err := s.steerCurrentControlInTx(tx, origin.ID)
		if err != nil {
			return nil, err
		}
		input, err := steerInputFromOriginal(&origin, control, &body)
		if err != nil {
			return nil, err
		}
		result.Inputs = append(result.Inputs, input)
	}
	return result, nil
}

// CheckSteerControl checks steer control.
func (s *KnowledgeInputProvenanceStore) CheckSteerControl(
	ctx context.Context,
	expected *types.SteerCandidateControl,
) error {
	if expected == nil {
		return types.ErrSourceLineageUnknown
	}
	return s.collectionTransaction(ctx, func(tx *gorm.DB) error {
		var origin steerAuthoredOrigin
		if err := tx.Select(steerOriginMetadataFields).
			Where("id=?", expected.OriginalAdmissionID).
			Take(&origin).
			Error; err != nil {
			return err
		}
		_, session, revision, err := s.currentMessageOwnerInTx(tx, origin.SessionID)
		if err != nil {
			return err
		}
		if session.UserID != origin.OwnerID || revision != origin.OwnerRevision {
			return ErrKnowledgeInputChanged
		}
		control, err := s.steerCurrentControlInTx(tx, origin.ID)
		if err != nil {
			return err
		}
		if !faqInputEqual(steerCandidate(control, &origin), expected) {
			return ErrKnowledgeInputChanged
		}
		if err := checkSteerOriginalWindow(tx, &origin); err != nil {
			return err
		}
		return CheckOriginalBodyAvailableInTx(tx.Statement.Context, tx, "steer_authored_origins", origin.ID)
	})
}
