package repository

import (
	"context"
	"encoding/json"
	"sort"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

func (s *KnowledgeInputProvenanceStore) MessageForkMaterialRequest(
	ctx context.Context,
	tenant uint64,
	sourceID, pointID string,
) (*types.MessageMaterialCandidateControl, types.MessageMaterialReadRequest, error) {
	request := types.MessageMaterialReadRequest{TenantID: tenant, SessionID: sourceID, IncludeOriginalUserFields: true}
	var point *types.MessageMaterialCandidateControl
	err := s.collectionTransaction(ctx, func(tx *gorm.DB) error {
		if _, _, _, err := s.currentMessageOwnerInTx(tx, sourceID); err != nil {
			return err
		}
		var current types.MessageMaterialCandidateControl
		if err := tx.Table("messages").
			Select("id,request_id,role,is_completed AS complete,created_at").
			Where("id=? AND session_id=? AND deleted_at IS NULL", pointID, sourceID).
			Take(&current).
			Error; err != nil {
			return err
		}
		if current.Role != "assistant" && current.Role != "user" {
			return types.ErrSourceLineageUnknown
		}
		var rows []struct{ ID string }
		if err := tx.Table("messages").
			Select("id").
			Where("session_id=? AND deleted_at IS NULL AND (created_at < ? OR (created_at = ? AND id <= ?))", sourceID, current.CreatedAt, current.CreatedAt, current.ID).
			Order("created_at,id").
			Scan(&rows).
			Error; err != nil {
			return err
		}
		for _, row := range rows {
			request.MessageIDs = append(request.MessageIDs, row.ID)
		}
		sort.Strings(request.MessageIDs)
		point = &current
		return nil
	})
	return point, request, err
}

type messageForkParentOrigin struct {
	GenerationID    string `gorm:"primaryKey"`
	ParentMaterials string
}

func (messageForkParentOrigin) TableName() string { return "message_fork_parent_origins" }

func (r *sessionRepository) CreateForkedFromMaterial(
	ctx context.Context,
	session *types.Session,
	copies []*types.Message,
	parent *types.MessageMaterialSnapshot,
) error {
	if session == nil || parent == nil {
		return types.ErrSourceLineageUnknown
	}
	holder, ok := ctx.Value(messageForkStoreKey{}).(messageForkMaterialHolder)
	store := holder.Store
	if !ok || store == nil || store.db != r.db || holder.Prewrite == nil {
		return types.ErrSourceLineageUnknown
	}
	if err := holder.Prewrite(); err != nil {
		return err
	}
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		actual, err := store.captureMessageMaterialInTx(tx, parent.Request)
		if err != nil {
			return err
		}
		a, b := *actual, *parent
		a.Messages = nil
		b.Messages = nil
		if !faqInputEqual(a, b) {
			return ErrKnowledgeInputChanged
		}
		fresh, source, _, err := store.currentMessageOwnerInTx(tx, parent.Request.SessionID)
		if err != nil {
			return err
		}
		if session.UserID != source.UserID || session.TenantID != source.TenantID || session.ID == source.ID ||
			session.ID == "" ||
			session.ForkedFromMessageID == "" {
			return ErrKnowledgeInputChanged
		}
		var point *types.Message
		for _, message := range actual.Messages {
			if message.ID == session.ForkedFromMessageID {
				point = message
			}
		}
		if point == nil {
			return ErrKnowledgeInputChanged
		}
		selected := []*types.Message{}
		for _, message := range actual.Messages {
			if message.CreatedAt.After(point.CreatedAt) ||
				message.CreatedAt.Equal(point.CreatedAt) && message.ID > point.ID {
				return ErrKnowledgeInputChanged
			}
			if point.Role == "user" && message.ID == point.ID {
				continue
			}
			selected = append(selected, message)
		}
		if len(selected) != len(copies) {
			return ErrKnowledgeInputChanged
		}
		mapping := map[string]string{}
		ids := map[string]bool{}
		for i, copy := range copies {
			original := selected[i]
			if copy == nil || copy.ID == "" || copy.ID == original.ID || ids[copy.ID] || copy.SessionID != session.ID ||
				copy.KnowledgeID != "" ||
				len(copy.Artifacts) > 0 {
				return ErrKnowledgeInputChanged
			}
			ids[copy.ID] = true
			if request, ok := mapping[original.RequestID]; ok && request != copy.RequestID {
				return ErrKnowledgeInputChanged
			}
			mapping[original.RequestID] = copy.RequestID
			normalized := *copy
			normalized.ID = original.ID
			normalized.SessionID = original.SessionID
			normalized.RequestID = original.RequestID
			normalized.KnowledgeID = original.KnowledgeID
			normalized.Artifacts = nil
			if !messageForkCoveredEqual(&normalized, original) {
				return ErrKnowledgeInputChanged
			}
		}
		now := time.Now()
		session.CreatedAt = now
		session.UpdatedAt = now
		if err := tx.Session(&gorm.Session{SkipHooks: true}).Create(session).Error; err != nil {
			return err
		}
		ownerRevision, err := messageVersion(tx, "message_session_owner_versions", "session_id", session.ID)
		if err != nil {
			return err
		}
		actor, err := captureKnowledgeActorSelector(fresh, "")
		if err != nil {
			return err
		}
		actorJSON, err := json.Marshal(actor)
		if err != nil {
			return err
		}
		// The immutable parent manifest is metadata/proofs only, never copied body.
		metadata := *parent
		metadata.Messages = nil
		parentJSON, err := json.Marshal(&metadata)
		if err != nil {
			return err
		}
		for copyIndex, copy := range copies {
			if err := tx.Session(&gorm.Session{SkipHooks: true}).Create(copy).Error; err != nil {
				return err
			}
			initial, err := messageOriginalInputJSON(copy)
			if err != nil {
				return err
			}
			origin := messageGenerationOrigin{
				ID:            uuid.NewString(),
				MessageID:     copy.ID,
				SessionID:     copy.SessionID,
				TenantID:      session.TenantID,
				OwnerID:       session.UserID,
				OwnerRevision: ownerRevision,
				RequestID:     copy.RequestID,
				Role:          copy.Role,
				Actor:         string(actorJSON),
				InitialInput:  string(initial),
			}
			if err := tx.Create(&origin).Error; err != nil {
				return err
			}
			if err := tx.Create(&messageForkParentOrigin{origin.ID, string(parentJSON)}).Error; err != nil {
				return err
			}
			if err := store.sealMessageMaterialInTx(tx, &origin, copy); err != nil {
				return err
			}
			if err := store.copyForkFieldOriginsInTx(tx, &origin, copy, selected[copyIndex]); err != nil {
				return err
			}
		}
		return nil
	})
}

type messageForkStoreKey struct{}

type messageForkMaterialHolder struct {
	Store    *KnowledgeInputProvenanceStore
	Prewrite func() error
}

func WithMessageForkMaterialStore(
	ctx context.Context,
	store *KnowledgeInputProvenanceStore,
	checks ...func() error,
) context.Context {
	var check func() error
	if len(checks) == 1 {
		check = checks[0]
	}
	return context.WithValue(ctx, messageForkStoreKey{}, messageForkMaterialHolder{store, check})
}

func messageForkCoveredEqual(a, b *types.Message) bool {
	encodedA, _ := json.Marshal(struct {
		Body        *types.Message
		Lineage     *types.SourceLineage
		Execution   types.MessageExecutionContext
		AgentTenant uint64
		Rendered    string
		Checkpoint  *types.ContextCheckpoint
	}{a, a.SourceLineage, a.ExecutionContext, a.AgentTenantID, a.RenderedContent, a.ContextCheckpoint})
	encodedB, _ := json.Marshal(struct {
		Body        *types.Message
		Lineage     *types.SourceLineage
		Execution   types.MessageExecutionContext
		AgentTenant uint64
		Rendered    string
		Checkpoint  *types.ContextCheckpoint
	}{b, b.SourceLineage, b.ExecutionContext, b.AgentTenantID, b.RenderedContent, b.ContextCheckpoint})
	return string(encodedA) == string(encodedB)
}

func (s *KnowledgeInputProvenanceStore) verifyMessageForkParentsInTx(tx *gorm.DB, generation string) error {
	var origin messageForkParentOrigin
	err := tx.Where("generation_id=?", generation).Take(&origin).Error
	if err == gorm.ErrRecordNotFound {
		return nil
	}
	if err != nil {
		return err
	}
	var parent types.MessageMaterialSnapshot
	if err := json.Unmarshal([]byte(origin.ParentMaterials), &parent); err != nil {
		return err
	}
	if err := s.checkMessageMaterialPlanInTx(tx, parent.Request); err != nil {
		return err
	}
	_, session, ownerRevision, err := s.currentMessageOwnerInTx(tx, parent.Request.SessionID)
	if err != nil {
		return err
	}
	if session.UserID != parent.SessionOwnerID || ownerRevision != parent.SessionOwnerRevision {
		return ErrKnowledgeInputChanged
	}
	membership, err := messageVersion(tx, "message_session_membership_versions", "session_id", parent.Request.SessionID)
	if err != nil {
		return err
	}
	if membership != parent.MembershipRevision {
		return ErrKnowledgeInputChanged
	}
	for _, proof := range parent.Proofs {
		if err := s.checkMessageOriginalBodyAvailability(tx, proof.GenerationID); err != nil {
			return err
		}
		var current messageMaterialAdmission
		if err := tx.Table("message_material_admissions a").
			Select("a.*").
			Joins("JOIN message_material_receipts r ON r.admission_id=a.id AND r.message_id=a.message_id").
			Where("a.message_id=? AND a.session_id=? AND a.tenant_id=?", proof.MessageID, proof.SessionID, proof.TenantID).
			Take(&current).
			Error; err != nil {
			return err
		}
		revision, err := messageVersion(tx, "message_material_versions", "message_id", proof.MessageID)
		if err != nil {
			return err
		}
		if current.ID != proof.AdmissionID || current.Revision != proof.Revision || revision != proof.Revision ||
			current.BodyDigest != proof.BodyDigest {
			return ErrKnowledgeInputChanged
		}
	}
	return nil
}

func (s *KnowledgeInputProvenanceStore) copyForkFieldOriginsInTx(
	tx *gorm.DB,
	origin *messageGenerationOrigin,
	copy, source *types.Message,
) error {
	var fields []messageGeneratedFieldAdmission
	if err := tx.Table("message_generated_field_admissions a").
		Select("a.*").
		Joins("JOIN message_generated_field_receipts r ON r.admission_id=a.id AND r.message_id=a.message_id AND r.field=a.field").
		Where("a.message_id=? AND a.session_id=?", source.ID, source.SessionID).
		Find(&fields).
		Error; err != nil {
		return err
	}
	var host messageMaterialAdmission
	if err := tx.Table("message_material_admissions a").
		Select("a.*").
		Joins("JOIN message_material_receipts r ON r.admission_id=a.id AND r.message_id=a.message_id").
		Where("a.message_id=?", copy.ID).
		Take(&host).
		Error; err != nil {
		return err
	}
	membership, err := messageVersion(tx, "message_session_membership_versions", "session_id", copy.SessionID)
	if err != nil {
		return err
	}
	for _, old := range fields {
		revision, err := messageGeneratedFieldVersion(tx, copy.ID, old.Field)
		if err != nil {
			return err
		}
		entry := old
		entry.ID = uuid.NewString()
		entry.MessageID = copy.ID
		entry.SessionID = copy.SessionID
		entry.GenerationID = origin.ID
		entry.Actor = origin.Actor
		entry.PreviousAdmissionID = host.ID
		entry.PreviousRevision = host.Revision
		entry.PreviousMembership = membership
		entry.ResultRevision = host.Revision
		entry.FieldRevision = revision
		// Fork's independent immutable parent manifest guards original source
		// generation and all fields; no current-row inference is written here.
		entry.ParentMaterials = "[]"
		if err := tx.Create(&entry).Error; err != nil {
			return err
		}
		if err := tx.Create(&messageGeneratedFieldReceipt{copy.ID, entry.Field, entry.ID}).Error; err != nil {
			return err
		}
	}
	return nil
}
