package repository

import (
	"context"
	"encoding/json"
	stderrors "errors"
	"fmt"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type messageGenerationOrigin struct {
	PriorSources                                                        *types.SourceLineage `gorm:"-"`
	PreviousAdmissionID                                                 string               `gorm:"-"`
	PreviousRevision, PreviousMembership                                uint64               `gorm:"-"`
	ID                                                                  string               `gorm:"primaryKey"`
	MessageID, SessionID, OwnerID, RequestID, Role, Actor, InitialInput string
	TenantID, OwnerRevision                                             uint64
}

func (messageGenerationOrigin) TableName() string { return "message_generation_origins" }

type messageMaterialAdmission struct {
	ID                                                                                                string `gorm:"primaryKey"`
	GenerationID, MessageID, SessionID, OwnerID, BodyDigest, SourceLineage, ExecutionContext, AgentID string
	TenantID, OwnerRevision, Revision, AgentTenantID                                                  uint64
}

func (messageMaterialAdmission) TableName() string { return "message_material_admissions" }

type messageMaterialReceipt struct {
	MessageID   string `gorm:"primaryKey"`
	AdmissionID string
}

func (messageMaterialReceipt) TableName() string { return "message_material_receipts" }

type messageProducerBoundaryKey struct{}
type messageProducerBoundary struct {
	Store  *KnowledgeInputProvenanceStore
	Digest string
	Create bool
}

// This marker is minted only by the actual authenticated message service over
// the new producer's exact arguments. A JSON field cannot carry it.
func (s *KnowledgeInputProvenanceStore) WithMessageProducerInput(ctx context.Context, message *types.Message, create bool) (context.Context, error) {
	if message != nil && create {
		if err := message.BeforeCreate(nil); err != nil {
			return ctx, err
		}
	}
	if message == nil || message.SessionID == "" || message.RequestID == "" || message.Role != "user" && message.Role != "assistant" {
		return ctx, types.ErrSourceLineageUnknown
	}
	digest, err := messageMaterialDigest(message)
	if err != nil {
		return ctx, err
	}
	return context.WithValue(ctx, messageProducerBoundaryKey{}, messageProducerBoundary{s, digest, create}), nil
}
func messageMaterialDigest(message *types.Message) (string, error) {
	if message == nil {
		return "", types.ErrSourceLineageUnknown
	}
	if message.Role == "user" {
		// Only this actual producer's immutable raw authored field list is covered.
		// Rendered context, images, attachments, memories and checkpoints remain
		// separate unknown material and are never loaded or emitted by this read.
		return KnowledgeInputDigest(struct {
			ID, Session, Request, Role, Content, Agent string
			AgentTenant                                uint64
			Complete                                   bool
			Created                                    time.Time
			Execution                                  types.MessageExecutionContext
		}{message.ID, message.SessionID, message.RequestID, message.Role, message.Content, message.AgentID, message.AgentTenantID, message.IsCompleted, message.CreatedAt, message.ExecutionContext})
	}
	body := *message
	body.KnowledgeID = ""
	body.UpdatedAt = time.Time{}
	body.Artifacts = nil
	return KnowledgeInputDigest(struct {
		Body        *types.Message
		Lineage     *types.SourceLineage
		Execution   types.MessageExecutionContext
		AgentTenant uint64
		Rendered    string
		Context     *types.ContextCheckpoint
	}{&body, body.SourceLineage, body.ExecutionContext, body.AgentTenantID, body.RenderedContent, body.ContextCheckpoint})
}
func messageVersion(tx *gorm.DB, table, column, id string) (uint64, error) {
	var row struct{ Revision uint64 }
	if err := tx.Table(table).Where(column+"=?", id).Take(&row).Error; err != nil {
		return 0, err
	}
	if row.Revision == 0 {
		return 0, ErrKnowledgeInputChanged
	}
	return row.Revision, nil
}
func (s *KnowledgeInputProvenanceStore) messageAuthorityInTx(tx *gorm.DB) (*KnowledgeInputProvenanceStore, error) {
	if s.db != nil && s.db.Statement.ConnPool == tx.Statement.ConnPool && s.actorGroups != nil {
		return s, nil
	}
	if s.displayAuthorityFactory == nil {
		return nil, types.ErrSourceLineageUnknown
	}
	actual := s.displayAuthorityFactory(tx)
	if actual == nil || actual.db != tx || actual.actorGroups == nil {
		return nil, types.ErrSourceLineageUnknown
	}
	return actual, nil
}
func (s *KnowledgeInputProvenanceStore) currentMessageOwnerInTx(tx *gorm.DB, sessionID string) (context.Context, *types.Session, uint64, error) {
	var session types.Session
	if err := tx.Select("id,user_id,tenant_id,deleted_at").Where("id=?", sessionID).Take(&session).Error; err != nil {
		return nil, nil, 0, err
	}
	authority, err := s.messageAuthorityInTx(tx)
	if err != nil {
		return nil, nil, 0, err
	}
	fresh, err := authority.RefreshCurrentReadActor(types.WithExecutionTenant(tx.Statement.Context, session.TenantID))
	if err != nil {
		return nil, nil, 0, err
	}
	owner := types.SessionOwnerIDFromContext(fresh)
	principal, _ := types.PrincipalFromContext(fresh)
	if principal.Type == types.PrincipalWebUser {
		owner = principal.ID
	}
	if owner == "" || session.UserID != owner {
		return nil, nil, 0, fmt.Errorf("%w: session owner does not match current actor", ErrKnowledgeInputChanged)
	}
	revision, err := messageVersion(tx, "message_session_owner_versions", "session_id", session.ID)
	return fresh, &session, revision, err
}
func (s *KnowledgeInputProvenanceStore) beginMessageProducerInTx(tx *gorm.DB, message *types.Message, create bool) (*messageGenerationOrigin, error) {
	boundary, ok := tx.Statement.Context.Value(messageProducerBoundaryKey{}).(messageProducerBoundary)
	digest, err := messageMaterialDigest(message)
	if err != nil {
		return nil, err
	}
	if advance, present := tx.Statement.Context.Value(messageProducerAdvanceKey{}).(messageProducerAdvanceBoundary); present {
		if advance.Store != s || advance.MessageID != message.ID || advance.SessionID != message.SessionID || advance.Digest != digest {
			return nil, ErrKnowledgeInputChanged
		}
	}
	if !ok || boundary.Store != s || boundary.Create != create || boundary.Digest != digest {
		return nil, fmt.Errorf("%w: exact message producer arguments changed", types.ErrSourceLineageUnknown)
	}
	fresh, session, ownerRevision, err := s.currentMessageOwnerInTx(tx, message.SessionID)
	if err != nil {
		return nil, err
	}
	actor, err := captureKnowledgeActorSelector(fresh, "")
	if err != nil {
		return nil, err
	}
	actorJSON, err := json.Marshal(actor)
	if err != nil {
		return nil, err
	}
	if create {
		if message.Role == "assistant" && (message.Content != "" || message.IsCompleted) {
			return nil, types.ErrSourceLineageUnknown
		}
		input, err := messageOriginalInputJSON(message)
		if err != nil {
			return nil, err
		}
		row := &messageGenerationOrigin{ID: uuid.NewString(), MessageID: message.ID, SessionID: message.SessionID, TenantID: session.TenantID, OwnerID: session.UserID, OwnerRevision: ownerRevision, RequestID: message.RequestID, Role: message.Role, Actor: string(actorJSON), InitialInput: string(input)}
		if err := tx.Create(row).Error; err != nil {
			return nil, err
		}
		return row, nil
	}
	var row messageGenerationOrigin
	if err := tx.Select(messageGenerationMetadataFields).Where("message_id=? AND session_id=?", message.ID, message.SessionID).Take(&row).Error; err != nil {
		return nil, err
	}
	if row.Actor != string(actorJSON) || row.RequestID != message.RequestID || row.Role != message.Role || row.OwnerID != session.UserID || row.OwnerRevision != ownerRevision || row.TenantID != session.TenantID {
		return nil, ErrKnowledgeInputChanged
	}
	// A full producer update can advance only its own last immutable material.
	// An unrelated partial write or ABA never becomes admitted by loading it.
	var prior messageMaterialAdmission
	if err := tx.Table("message_material_admissions a").Select("a.*").Joins("JOIN message_material_receipts r ON r.admission_id=a.id AND r.message_id=a.message_id").Where("a.message_id=? AND a.generation_id=?", message.ID, row.ID).Take(&prior).Error; err != nil {
		return nil, err
	}
	original, err := readMessageMaterialBodyInTx(tx, message.ID, message.SessionID, message.Role)
	if err != nil {
		return nil, err
	}
	row.PreviousAdmissionID = prior.ID
	row.PreviousRevision = prior.Revision
	row.PreviousMembership, err = messageVersion(tx, "message_session_membership_versions", "session_id", message.SessionID)
	if err != nil {
		return nil, err
	}
	priorDigest, err := messageMaterialDigest(original)
	if err != nil {
		return nil, err
	}
	priorRevision, err := messageVersion(tx, "message_material_versions", "message_id", message.ID)
	if err != nil {
		return nil, err
	}
	if prior.BodyDigest != priorDigest || prior.Revision != priorRevision {
		return nil, ErrKnowledgeInputChanged
	}
	var originalSources types.SourceLineage
	if err := json.Unmarshal([]byte(prior.SourceLineage), &originalSources); err != nil {
		return nil, err
	}
	if originalSources.RequireComplete() != nil {
		// Only this generation's actual initial empty placeholder has no consumed
		// body. A partial unknown completion cannot be certified by a later update.
		if original.IsCompleted || original.Content != "" || len(original.AgentSteps) > 0 || len(original.KnowledgeReferences) > 0 || len(original.Images) > 0 || len(original.Attachments) > 0 || original.ContextCheckpoint != nil {
			return nil, types.ErrSourceLineageUnknown
		}
		empty, _ := types.NewCompleteSourceLineage()
		row.PriorSources = empty
	} else {
		row.PriorSources = &originalSources
	}

	if err := s.checkMessageOriginalBodyAvailability(tx, row.ID); err != nil {
		return nil, err
	}
	return &row, nil
}
func (s *KnowledgeInputProvenanceStore) sealMessageMaterialInTx(tx *gorm.DB, origin *messageGenerationOrigin, message *types.Message) error {
	// Incomplete or unknown material is recorded for this producer continuity,
	// but saved-body readers still require completed, complete original lineage.
	digest, err := messageMaterialDigest(message)
	if err != nil {
		return err
	}
	revision, err := messageVersion(tx, "message_material_versions", "message_id", message.ID)
	if err != nil {
		return err
	}
	source, err := json.Marshal(message.SourceLineage)
	if err != nil {
		return err
	}
	execution, err := json.Marshal(message.ExecutionContext)
	if err != nil {
		return err
	}
	row := messageMaterialAdmission{ID: uuid.NewString(), GenerationID: origin.ID, MessageID: message.ID, SessionID: message.SessionID, TenantID: origin.TenantID, OwnerID: origin.OwnerID, OwnerRevision: origin.OwnerRevision, Revision: revision, BodyDigest: digest, SourceLineage: string(source), ExecutionContext: string(execution), AgentID: message.AgentID, AgentTenantID: message.AgentTenantID}
	if err := tx.Create(&row).Error; err != nil {
		return err
	}
	if err := tx.Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "message_id"}}, DoUpdates: clause.AssignmentColumns([]string{"admission_id"})}).Create(&messageMaterialReceipt{message.ID, row.ID}).Error; err != nil {
		return err
	}
	if origin.PreviousAdmissionID != "" {
		if err := s.sealMessageProducerAdvanceInTx(tx, origin, &row); err != nil {
			return err
		}
	}
	return nil
}

func ConfigureMessageMaterialWriters(repo interface{}, store *KnowledgeInputProvenanceStore) {
	if r, ok := repo.(*messageRepository); ok {
		r.materialStore = store
	}
}

func ConfigureMessageMaterialAgentShares(store *KnowledgeInputProvenanceStore, agents interface{}) {
	if actual, ok := agents.(interfaces.AgentShareService); ok {
		store.messageActorAgents = actual
	}
}

// The retention owner supplies the actual vault and external purge-journal
// checker. A digest or current message body never substitutes for unavailable
// original generation material.
type MessageOriginalBodyAvailabilityCheck func(context.Context, *gorm.DB, string, string) error

func ConfigureMessageOriginalBodyAvailability(store *KnowledgeInputProvenanceStore, check MessageOriginalBodyAvailabilityCheck) {
	store.messageBodyAvailability = check
}
func (s *KnowledgeInputProvenanceStore) checkMessageOriginalBodyAvailability(tx *gorm.DB, generationID string) error {
	if generationID == "" {
		return types.ErrSourceLineageUnknown
	}
	if s.messageBodyAvailability != nil {
		if err := s.messageBodyAvailability(tx.Statement.Context, tx, "message_generation_origins", generationID); err != nil {
			return stderrors.Join(types.ErrSourceLineageUnknown, err)
		}
		return nil
	}
	return nil // Retention is not installed in this module's parent; this is not purge authorization.
}

const messageRawAuthorFields = "id,session_id,request_id,role,content,is_completed,created_at,agent_id,agent_tenant_id,execution_context"

func readMessageMaterialBodyInTx(tx *gorm.DB, id, session, role string) (*types.Message, error) {
	var message types.Message
	q := tx.Where("id=? AND session_id=? AND role=?", id, session, role)
	if role == "user" {
		q = q.Select(messageRawAuthorFields)
	}
	if err := q.Take(&message).Error; err != nil {
		return nil, err
	}
	return &message, nil
}

func messageOriginalInputJSON(message *types.Message) ([]byte, error) {
	return json.Marshal(struct {
		Body        *types.Message
		Lineage     *types.SourceLineage
		Execution   types.MessageExecutionContext
		AgentTenant uint64
		Rendered    string
		Checkpoint  *types.ContextCheckpoint
	}{message, message.SourceLineage, message.ExecutionContext, message.AgentTenantID, message.RenderedContent, message.ContextCheckpoint})
}

const messageGenerationMetadataFields = "id,message_id,session_id,tenant_id,owner_id,owner_revision,request_id,role,actor"
