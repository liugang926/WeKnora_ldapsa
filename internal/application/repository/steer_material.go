package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"strings"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

type steerAuthoredOrigin struct {
	ID                                     string
	SteerID                                string
	SessionID                              string
	OwnerID                                string
	OriginRunMessageID                     string
	OriginRunGenerationID                  string
	OriginRunRequestID                     string
	Actor                                  string
	BodyDigest                             string
	CanonicalDigest                        string
	SourceLineage                          string
	ExecutionContext                       string
	AgentID                                string
	InputBody                              string
	TenantID, OwnerRevision, AgentTenantID uint64
	CreatedAtMS                            int64
}

func (steerAuthoredOrigin) TableName() string { return "steer_authored_origins" }

type steerControlAdmission struct {
	ID                           string
	OriginID                     string
	SessionID                    string
	SteerID                      string
	PreviousHeadID               string
	Operation                    string
	RunMessageID                 string
	RunGenerationID              string
	RunRequestID                 string
	Delivery                     string
	UserMessageID                string
	TenantID, Revision           uint64
	Consumed, Discarded, Deleted bool
	CreatedAtMS                  int64
}

func (steerControlAdmission) TableName() string { return "steer_control_admissions" }

type steerControlHead struct {
	OriginID string `gorm:"primaryKey"`
	HeadID   string
	Revision uint64
}

func (steerControlHead) TableName() string { return "steer_control_heads" }

type steerAuthoredBody struct {
	RawInput       json.RawMessage
	RawQuery       string
	Query          string
	MentionedItems types.MentionedItems
	Channel        string
	Delivery       types.SteerDelivery
}
type (
	steerAuthoredProducerKey      struct{}
	steerAuthoredProducerBoundary struct {
		Store      *KnowledgeInputProvenanceStore
		ID         string
		BodyDigest string
		Lineage    *types.SourceLineage
	}
)

const steerOriginMetadataFields = ("id,steer_id,tenant_id,session_id,owner_id,owner_revision,origin_run_messag" +
	"e_id,origin_run_generation_id,origin_run_request_id,actor,body_digest,cano" +
	"nical_digest,source_lineage,execution_context,agent_id,agent_tenant_id,cre" +
	"ated_at_ms")

// Authorship, control transitions, body leases and consumption commit facts.
// Their authorization reads share the same write transaction; ordinary Steer
// captures and rechecks continue to use the read-only collection boundary.
func (s *KnowledgeInputProvenanceStore) steerWriteTransaction(ctx context.Context, fn func(*gorm.DB) error) error {
	if ctx == nil || s == nil || s.db == nil {
		return types.ErrSourceLineageUnknown
	}
	options := &sql.TxOptions{}
	switch s.db.Name() {
	case "postgres":
		options.Isolation = sql.LevelSerializable
	case "sqlite":
	default:
		return ErrKnowledgeInputChanged
	}
	if _, nested := s.db.Statement.ConnPool.(gorm.TxCommitter); nested {
		return ErrKnowledgeInputChanged
	}
	return s.db.WithContext(ctx).Transaction(fn, options)
}

func canonicalSteerBody(req *types.SteerAuthoredRequest) (steerAuthoredBody, error) {
	if req == nil {
		return steerAuthoredBody{}, types.ErrSourceLineageUnknown
	}
	delivery := req.Delivery
	if delivery == "" {
		delivery = types.SteerDeliveryAfter
	}
	if delivery != types.SteerDeliveryAfter && delivery != types.SteerDeliveryInject {
		return steerAuthoredBody{}, types.ErrSourceLineageUnknown
	}
	channel := strings.TrimSpace(req.Channel)
	if channel == "" {
		channel = "web"
	}
	query := types.SteerCanonicalQuery(req.Query)
	rawQuery := req.Query
	if len(req.RawInput) > 0 {
		var raw struct {
			Query string `json:"query"`
		}
		if err := json.Unmarshal(req.RawInput, &raw); err != nil {
			return steerAuthoredBody{}, err
		}
		if types.SteerCanonicalQuery(raw.Query) != query {
			return steerAuthoredBody{}, types.ErrSourceLineageUnknown
		}
		rawQuery = raw.Query
	}
	if query == "" {
		return steerAuthoredBody{}, types.ErrSourceLineageUnknown
	}
	return steerAuthoredBody{
		RawInput:       append(json.RawMessage(nil), req.RawInput...),
		RawQuery:       rawQuery,
		Query:          query,
		MentionedItems: req.MentionedItems,
		Channel:        channel,
		Delivery:       delivery,
	}, nil
}

// AdmitSteerAuthoredInput admits steer authored input.
func (s *KnowledgeInputProvenanceStore) AdmitSteerAuthoredInput(
	ctx context.Context,
	req *types.SteerAuthoredRequest,
) (*types.SteerMaterialInput, error) {
	body, err := canonicalSteerBody(req)
	if err != nil {
		return nil, err
	}
	if req.TenantID == 0 || req.SessionID == "" || req.ClientSteerID == "" || req.RunMessageID == "" ||
		req.RunGenerationID == "" ||
		req.RunRequestID == "" {
		return nil, types.ErrSourceLineageUnknown
	}
	raw, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	digest, err := KnowledgeInputDigest(body)
	if err != nil {
		return nil, err
	}
	canonicalDigest, err := steerCanonicalDigest(&body)
	if err != nil {
		return nil, err
	}
	var result *types.SteerMaterialInput
	err = s.steerWriteTransaction(ctx, func(tx *gorm.DB) error {
		fresh, session, ownerRevision, err := s.currentMessageOwnerInTx(tx, req.SessionID)
		if err != nil {
			return err
		}
		if session.TenantID != req.TenantID {
			return ErrKnowledgeInputChanged
		}
		run, generation, err := s.messageLiveControlInTx(tx, req.SessionID, req.RunMessageID)
		if err != nil {
			return err
		}
		if generation != req.RunGenerationID || run.RequestID != req.RunRequestID || run.IsCompleted ||
			run.Role != "assistant" {
			return ErrKnowledgeInputChanged
		}
		var existing steerAuthoredOrigin
		err = tx.Select(steerOriginMetadataFields).
			Where("tenant_id=? AND session_id=? AND steer_id=?", req.TenantID, req.SessionID, req.ClientSteerID).
			Take(&existing).
			Error
		if err == nil {
			if err := checkSteerOriginalWindow(tx, &existing); err != nil {
				return err
			}
			if err := CheckOriginalBodyAvailableInTx(
				tx.Statement.Context,
				tx,
				"steer_authored_origins",
				existing.ID,
			); err != nil {
				return err
			}
			if existing.BodyDigest != digest || existing.OwnerID != session.UserID {
				return ErrKnowledgeInputChanged
			}
			control, err := s.steerCurrentControlInTx(tx, existing.ID)
			if err != nil {
				return err
			}
			result, err = steerInputFromOriginal(&existing, control, &body)
			return err
		}
		if err != gorm.ErrRecordNotFound {
			return err
		}
		var pending []struct{ OriginID string }
		if err := tx.Table("steer_control_heads h").
			Select("h.origin_id").
			Joins("JOIN steer_control_admissions c ON c.id=h.head_id AND c.origin_id=h.origin_id").
			Where(
				("c.tenant_id=? AND c.session_id=? AND c.run_message_id=? AND c.run_generati" +
					"on_id=? AND c.consumed=false AND c.discarded=false AND c.deleted=false"),
				req.TenantID,
				req.SessionID,
				run.ID,
				generation,
			).
			Scan(&pending).
			Error; err != nil {
			return err
		}
		for _, candidate := range pending {
			if _, err := s.steerCurrentControlInTx(tx, candidate.OriginID); err != nil {
				return err
			}
		}
		if len(pending) >= 10 {
			return ErrKnowledgeInputChanged
		}
		actor, err := captureKnowledgeActorSelector(fresh, "")
		if err != nil {
			return err
		}
		actorJSON, err := json.Marshal(actor)
		if err != nil {
			return err
		}
		lineage, _ := types.NewCompleteSourceLineage()
		sourceJSON, _ := json.Marshal(lineage)
		executionJSON, err := json.Marshal(run.ExecutionContext)
		if err != nil {
			return err
		}
		now, err := nextcloudLeaseNowMS(tx)
		if err != nil {
			return err
		}
		origin := steerAuthoredOrigin{
			ID:                    uuid.NewString(),
			SteerID:               req.ClientSteerID,
			TenantID:              req.TenantID,
			SessionID:             req.SessionID,
			OwnerID:               session.UserID,
			OwnerRevision:         ownerRevision,
			OriginRunMessageID:    run.ID,
			OriginRunGenerationID: generation,
			OriginRunRequestID:    run.RequestID,
			Actor:                 string(actorJSON),
			BodyDigest:            digest,
			CanonicalDigest:       canonicalDigest,
			SourceLineage:         string(sourceJSON),
			ExecutionContext:      string(executionJSON),
			AgentID:               run.AgentID,
			AgentTenantID:         run.AgentTenantID,
			InputBody:             string(raw),
			CreatedAtMS:           now,
		}
		marked := context.WithValue(
			tx.Statement.Context,
			steerAuthoredProducerKey{},
			steerAuthoredProducerBoundary{s, origin.ID, digest, lineage},
		)
		if err := tx.WithContext(marked).Create(&origin).Error; err != nil {
			return err
		}
		control := steerControlAdmission{
			ID:              uuid.NewString(),
			OriginID:        origin.ID,
			TenantID:        req.TenantID,
			SessionID:       req.SessionID,
			SteerID:         req.ClientSteerID,
			Revision:        1,
			Operation:       "enqueue",
			RunMessageID:    run.ID,
			RunGenerationID: generation,
			RunRequestID:    run.RequestID,
			Delivery:        string(body.Delivery),
			CreatedAtMS:     now,
		}
		if err := tx.Create(&control).Error; err != nil {
			return err
		}
		if err := tx.Create(&steerControlHead{origin.ID, control.ID, 1}).Error; err != nil {
			return err
		}
		result, err = steerInputFromOriginal(&origin, &control, &body)
		return err
	})
	return result, err
}

func (s *KnowledgeInputProvenanceStore) steerCurrentControlInTx(
	tx *gorm.DB,
	origin string,
) (*steerControlAdmission, error) {
	var head steerControlHead
	if err := tx.Where("origin_id=?", origin).Take(&head).Error; err != nil {
		return nil, err
	}
	revision, err := messageVersion(tx, "steer_control_versions", "origin_id", origin)
	if err != nil {
		return nil, err
	}
	if revision != head.Revision {
		return nil, ErrKnowledgeInputChanged
	}
	var control steerControlAdmission
	if err := tx.Where("id=? AND origin_id=?", head.HeadID, origin).Take(&control).Error; err != nil {
		return nil, err
	}
	if control.Revision != head.Revision {
		return nil, ErrKnowledgeInputChanged
	}
	return &control, nil
}

func steerCandidate(control *steerControlAdmission, origin *steerAuthoredOrigin) *types.SteerCandidateControl {
	return &types.SteerCandidateControl{
		SteerID:             origin.SteerID,
		OriginalAdmissionID: origin.ID,
		BodyDigest:          origin.BodyDigest,
		HeadID:              control.ID,
		Revision:            control.Revision,
		RunMessageID:        control.RunMessageID,
		RunGenerationID:     control.RunGenerationID,
		RunRequestID:        control.RunRequestID,
		Delivery:            types.SteerDelivery(control.Delivery),
		Consumed:            control.Consumed,
		Discarded:           control.Discarded,
		Deleted:             control.Deleted,
		UserMessageID:       control.UserMessageID,
		CreatedAt:           time.UnixMilli(origin.CreatedAtMS),
	}
}

func steerInputFromOriginal(
	origin *steerAuthoredOrigin,
	control *steerControlAdmission,
	body *steerAuthoredBody,
) (*types.SteerMaterialInput, error) {
	var source types.SourceLineage
	if err := json.Unmarshal([]byte(origin.SourceLineage), &source); err != nil {
		return nil, err
	}
	if err := source.RequireComplete(); err != nil {
		return nil, err
	}
	var execution types.MessageExecutionContext
	if err := json.Unmarshal([]byte(origin.ExecutionContext), &execution); err != nil {
		return nil, err
	}
	proof := &types.SteerOriginalProof{
		AdmissionID:           origin.ID,
		OriginID:              origin.ID,
		SteerID:               origin.SteerID,
		SessionID:             origin.SessionID,
		OwnerID:               origin.OwnerID,
		OriginRunMessageID:    origin.OriginRunMessageID,
		OriginRunGenerationID: origin.OriginRunGenerationID,
		OriginRunRequestID:    origin.OriginRunRequestID,
		TenantID:              origin.TenantID,
		OwnerRevision:         origin.OwnerRevision,
		BodyDigest:            origin.BodyDigest,
		SourceLineage:         &source,
		ExecutionContext:      execution,
		AgentID:               origin.AgentID,
		AgentTenantID:         origin.AgentTenantID,
		CreatedAt:             time.UnixMilli(origin.CreatedAtMS),
	}
	return &types.SteerMaterialInput{
		Proof:          proof,
		Control:        steerCandidate(control, origin),
		Query:          body.Query,
		MentionedItems: body.MentionedItems,
		Channel:        body.Channel,
	}, nil
}

// ApplySteerControl applies steer control.
func (s *KnowledgeInputProvenanceStore) ApplySteerControl(
	ctx context.Context,
	req *types.SteerControlRequest,
) (*types.SteerCandidateControl, error) {
	if req == nil {
		return nil, types.ErrSourceLineageUnknown
	}
	var result *types.SteerCandidateControl
	err := s.steerWriteTransaction(ctx, func(tx *gorm.DB) error {
		_, session, ownerRev, err := s.currentMessageOwnerInTx(tx, req.SessionID)
		if err != nil {
			return err
		}
		if session.TenantID != req.TenantID {
			return ErrKnowledgeInputChanged
		}
		var origin steerAuthoredOrigin
		if err := tx.Select(steerOriginMetadataFields).
			Where(
				"id=? AND tenant_id=? AND session_id=? AND steer_id=?",
				req.OriginalAdmissionID,
				req.TenantID,
				req.SessionID,
				req.SteerID,
			).
			Take(&origin).
			Error; err != nil {
			return err
		}
		if origin.OwnerID != session.UserID || origin.OwnerRevision != ownerRev {
			return ErrKnowledgeInputChanged
		}
		if err := checkSteerOriginalWindow(tx, &origin); err != nil {
			return err
		}
		control, err := s.steerCurrentControlInTx(tx, origin.ID)
		if err != nil {
			return err
		}
		if control.ID != req.ExpectedHeadID || control.Revision != req.ExpectedRevision || control.Deleted {
			return ErrKnowledgeInputChanged
		}
		next := *control
		next.ID = uuid.NewString()
		next.PreviousHeadID = control.ID
		next.Revision++
		next.Operation = string(req.Operation)
		next.CreatedAtMS, err = nextcloudLeaseNowMS(tx)
		if err != nil {
			return err
		}
		switch req.Operation {
		case types.SteerControlPromote:
			if control.Consumed || control.Discarded {
				return ErrKnowledgeInputChanged
			}
			next.Delivery = string(types.SteerDeliveryInject)
		case types.SteerControlDiscard:
			next.Discarded = true
		case types.SteerControlClaimFailed:
			if !control.Consumed || control.UserMessageID == "" {
				return ErrKnowledgeInputChanged
			}
			next.Discarded = true
		case types.SteerControlDelete:
			next.Deleted = true
		case types.SteerControlRebind:
			if control.Consumed || control.Discarded {
				return ErrKnowledgeInputChanged
			}
			run, generation, err := s.messageLiveControlInTx(tx, req.SessionID, req.TargetRunMessageID)
			if err != nil {
				return err
			}
			if run.IsCompleted || generation != req.TargetRunGenerationID || run.RequestID != req.TargetRunRequestID {
				return ErrKnowledgeInputChanged
			}
			next.RunMessageID = run.ID
			next.RunGenerationID = generation
			next.RunRequestID = run.RequestID
		default:
			return types.ErrSourceLineageUnknown
		}
		if err := tx.Create(&next).Error; err != nil {
			return err
		}
		changed := tx.Model(&steerControlHead{}).
			Where("origin_id=? AND head_id=? AND revision=?", origin.ID, control.ID, control.Revision).
			Updates(map[string]any{"head_id": next.ID, "revision": next.Revision})
		if changed.Error != nil {
			return changed.Error
		}
		if changed.RowsAffected != 1 {
			return ErrKnowledgeInputChanged
		}
		result = steerCandidate(&next, &origin)
		return nil
	})
	return result, err
}

func steerCanonicalDigest(body *steerAuthoredBody) (string, error) {
	return KnowledgeInputDigest(struct {
		Query    string
		Mentions types.MentionedItems
		Channel  string
		Delivery types.SteerDelivery
	}{body.Query, body.MentionedItems, body.Channel, body.Delivery})
}

func checkSteerOriginalWindow(tx *gorm.DB, origin *steerAuthoredOrigin) error {
	now, err := nextcloudLeaseNowMS(tx)
	if err != nil {
		return err
	}
	if origin == nil || origin.CreatedAtMS <= 0 || now >= origin.CreatedAtMS+7*86400000 {
		return ErrOriginalBodyUnavailable
	}
	return nil
}
