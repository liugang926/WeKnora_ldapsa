package repository

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"strings"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type knowledgeInputHistoryParents struct {
	AdmissionID        string `gorm:"primaryKey"`
	TenantID           uint64
	SessionID          string
	AssistantMessageID string
	UserMessageID      string
	RequestID          string
	AssistantRevision  uint64
	UserRevision       uint64
	AssistantDigest    string
	UserDigest         string
}

func (knowledgeInputHistoryParents) TableName() string { return "knowledge_input_history_parents" }

func ConfigureKnowledgeInputHistoryVerifier(store *KnowledgeInputProvenanceStore, verifier interfaces.MessageSourceLineageVerifier) {
	store.messageVerifier = verifier
}
func historyMessageInputDigest(message *types.Message) (string, error) {
	return KnowledgeInputDigest(struct {
		ID, Session, Request, Role, Content, Agent, Model string
		AgentTenant                                       uint64
		Complete                                          bool
		Lineage                                           *types.SourceLineage
		Execution                                         types.MessageExecutionContext
	}{message.ID, message.SessionID, message.RequestID, message.Role, message.Content, message.AgentID, message.ModelID, message.AgentTenantID, message.IsCompleted, message.SourceLineage, message.ExecutionContext})
}
func historyMessageInputRevision(tx *gorm.DB, id string) (uint64, error) {
	var identity struct{ Role string }
	if err := tx.Table("messages").Select("role").Where("id=? AND deleted_at IS NULL", id).Take(&identity).Error; err != nil {
		return 0, err
	}
	if identity.Role == "user" {
		return messageVersion(tx, "message_material_versions", "message_id", id)
	}
	var row struct{ Revision uint64 }
	if err := tx.Table("knowledge_input_message_versions").Where("message_id = ?", id).Take(&row).Error; err != nil {
		return 0, err
	}
	if row.Revision == 0 {
		return 0, ErrKnowledgeInputChanged
	}
	return row.Revision, nil
}

func (s *KnowledgeInputProvenanceStore) verifyCommittedHistoryParents(tx *gorm.DB, k *types.Knowledge, a *knowledgeInputAdmission) error {
	var parent knowledgeInputHistoryParents
	if err := tx.Where("admission_id = ? AND tenant_id = ?", a.AdmissionID, k.TenantID).Take(&parent).Error; err != nil {
		return err
	}
	var session types.Session
	if err := tx.Select("id,user_id,tenant_id,deleted_at").Where("id = ? AND tenant_id = ?", parent.SessionID, parent.TenantID).Take(&session).Error; err != nil {
		return err
	}
	req := types.MessageMaterialReadRequest{TenantID: parent.TenantID, SessionID: parent.SessionID, MessageIDs: []string{parent.AssistantMessageID, parent.UserMessageID}}
	if err := s.checkMessageMaterialPlanInTx(tx, req); err != nil {
		return err
	}
	messages := make([]*types.Message, 0, 2)
	for _, item := range []struct {
		ID, Role, Digest string
		Revision         uint64
	}{{parent.AssistantMessageID, "assistant", parent.AssistantDigest, parent.AssistantRevision}, {parent.UserMessageID, "user", parent.UserDigest, parent.UserRevision}} {
		message, err := readMessageMaterialBodyInTx(tx, item.ID, parent.SessionID, item.Role)
		if err != nil {
			return err
		}
		if message.RequestID != parent.RequestID {
			return ErrKnowledgeInputChanged
		}
		if !message.IsCompleted || (item.Role == "assistant" && message.SourceLineage.RequireComplete() != nil) || (item.Role == "assistant" && message.KnowledgeID != k.ID) {
			return ErrKnowledgeInputChanged
		}
		if item.Role == "user" {
			rawProof, err := s.verifyMessageMaterialInTx(tx, message)
			if err != nil {
				return err
			}
			message.SourceLineage = rawProof.SourceLineage
		}
		digest, err := historyMessageInputDigest(message)
		if err != nil {
			return err
		}
		revision, err := historyMessageInputRevision(tx, message.ID)
		if err != nil {
			return err
		}
		if digest != item.Digest || revision != item.Revision {
			return ErrKnowledgeInputChanged
		}
		messages = append(messages, message)
	}
	for _, message := range messages {
		if _, err := s.verifyMessageMaterialInTx(tx, message); err != nil {
			return err
		}
	}
	return nil
}

var historyInputThinkBlocks = regexp.MustCompile(`(?s)<think>.*?</think>`)

// CreateCommittedHistoryKnowledgeInput reads the complete assistant and its
// unique original paired user, binds their exact envelopes/revisions, creates
// the child and links the message in one transaction. No caller text is used.
func (s *KnowledgeInputProvenanceStore) CreateCommittedHistoryKnowledgeInput(ctx context.Context, request types.CommittedHistoryKnowledgeInputRequest) (*types.CommittedHistoryKnowledgeInput, error) {
	if request.TenantID == 0 || request.KnowledgeBaseID == "" || request.SessionID == "" || request.AssistantMessageID == "" {
		return nil, types.ErrSourceLineageUnknown
	}
	options := &sql.TxOptions{}
	if s.db.Dialector.Name() == "postgres" {
		options.Isolation = sql.LevelRepeatableRead
	}
	var result *types.CommittedHistoryKnowledgeInput
	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		plan := findMessageMaterialReadPlan(ctx, types.MessageMaterialReadRequest{TenantID: request.TenantID, SessionID: request.SessionID, MessageIDs: []string{request.AssistantMessageID}})
		if plan == nil {
			return ErrNextcloudContentGCUncovered
		}
		if err := s.checkMessageMaterialPlanInTx(tx, plan.Request); err != nil {
			return err
		}

		var session types.Session
		if err := tx.Select("id,user_id,tenant_id,deleted_at").Where("id = ? AND tenant_id = ?", request.SessionID, request.TenantID).Take(&session).Error; err != nil {
			return err
		}
		var assistant types.Message
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("id = ? AND session_id = ? AND role = 'assistant'", request.AssistantMessageID, request.SessionID).Take(&assistant).Error; err != nil {
			return err
		}
		if !assistant.IsCompleted || assistant.RequestID == "" || assistant.KnowledgeID != "" || assistant.SourceLineage.RequireComplete() != nil {
			return nilHistoryInputError()
		}
		var users []*types.Message
		if err := tx.Select(messageRawAuthorFields).Clauses(clause.Locking{Strength: "UPDATE"}).Where("session_id = ? AND request_id = ? AND role = 'user'", request.SessionID, assistant.RequestID).Limit(2).Find(&users).Error; err != nil {
			return err
		}
		if len(users) != 1 || !users[0].IsCompleted {
			return nilHistoryInputError()
		}
		user := users[0]
		for _, parent := range []*types.Message{&assistant, user} {
			if _, err := s.verifyMessageMaterialInTx(tx, parent); err != nil {
				return err
			}
		}
		userProof, err := s.verifyMessageMaterialInTx(tx, user)
		if err != nil {
			return err
		}
		user.SourceLineage = userProof.SourceLineage
		lineage, err := types.UnionSourceLineage(assistant.SourceLineage, userProof.SourceLineage)
		if err != nil {
			return err
		}
		var kb types.KnowledgeBase
		if err := tx.Where("id = ? AND tenant_id = ?", request.KnowledgeBaseID, request.TenantID).Take(&kb).Error; err != nil {
			return err
		}
		authority, err := s.messageAuthorityInTx(tx)
		if err != nil {
			return err
		}
		current, err := authority.AuthorizeCurrentKnowledgeInputWrite(ctx, &kb)
		if err != nil {
			return err
		}
		ctx = current
		answer := strings.TrimSpace(historyInputThinkBlocks.ReplaceAllString(assistant.Content, ""))
		passages := []string{fmt.Sprintf("[Session: %s]\nQ: %s\nA: %s", session.ID, user.Content, answer)}
		knowledge := &types.Knowledge{ID: uuid.NewString(), TenantID: request.TenantID, KnowledgeBaseID: kb.ID, Type: "passage", Channel: "internal_history", ParseStatus: types.ParseStatusPending, EnableStatus: "disabled", EmbeddingModelID: kb.EmbeddingModelID, CustomMetadata: types.JSON(`{}`)}
		if err := tx.Create(knowledge).Error; err != nil {
			return err
		}
		proof, err := s.admitInTx(ctx, tx, knowledge, &kb, passages, knowledgeInputOrigin{lineage, "history"})
		if err != nil {
			return err
		}
		link := tx.Model(&types.Message{}).Where("id = ? AND session_id = ? AND knowledge_id = ?", assistant.ID, session.ID, assistant.KnowledgeID).Update("knowledge_id", knowledge.ID)
		if link.Error != nil {
			return link.Error
		}
		if link.RowsAffected != 1 {
			return ErrKnowledgeInputChanged
		}
		assistant.KnowledgeID = knowledge.ID
		assistantDigest, err := historyMessageInputDigest(&assistant)
		if err != nil {
			return err
		}
		userDigest, err := historyMessageInputDigest(user)
		if err != nil {
			return err
		}
		assistantVersion, err := historyMessageInputRevision(tx, assistant.ID)
		if err != nil {
			return err
		}
		userVersion, err := historyMessageInputRevision(tx, user.ID)
		if err != nil {
			return err
		}
		parent := knowledgeInputHistoryParents{proof.AdmissionID, request.TenantID, session.ID, assistant.ID, user.ID, assistant.RequestID, assistantVersion, userVersion, assistantDigest, userDigest}
		if err := tx.Create(&parent).Error; err != nil {
			return err
		}
		if err := s.verifyCommittedHistoryParents(tx, knowledge, &knowledgeInputAdmission{AdmissionID: proof.AdmissionID}); err != nil {
			return err
		}
		result = &types.CommittedHistoryKnowledgeInput{Knowledge: knowledge, Passages: passages, Proof: proof, Assistant: &assistant, User: user}
		return nil
	}, options)
	if err != nil {
		return nil, err
	}
	return result, nil
}
func nilHistoryInputError() error { return ErrKnowledgeInputChanged }

func (s *KnowledgeInputProvenanceStore) KnowledgeHistoryMaterialRequest(ctx context.Context, proof *types.KnowledgeBuildInputProof) (*types.MessageMaterialReadRequest, error) {
	if proof == nil {
		return nil, types.ErrSourceLineageUnknown
	}
	var row knowledgeInputAdmission
	if err := s.db.WithContext(ctx).Where("admission_id=? AND knowledge_id=? AND tenant_id=?", proof.AdmissionID, proof.KnowledgeID, proof.TenantID).Take(&row).Error; err != nil {
		return nil, err
	}
	if row.OriginKind != "history" {
		return nil, nil
	}
	if row.InputDigest != proof.InputDigest || row.BuildGeneration != proof.BuildGeneration {
		return nil, ErrKnowledgeInputChanged
	}
	var parent knowledgeInputHistoryParents
	if err := s.db.WithContext(ctx).Where("admission_id=? AND tenant_id=?", proof.AdmissionID, proof.TenantID).Take(&parent).Error; err != nil {
		return nil, err
	}
	return &types.MessageMaterialReadRequest{TenantID: parent.TenantID, SessionID: parent.SessionID, MessageIDs: []string{parent.AssistantMessageID, parent.UserMessageID}}, nil
}

// Current source verification is deliberately outside the material SQL tx.
// This method grants no original-body admission and cannot replace the tx-local
// receipts, versions, owner and read-lease checks.
func (s *KnowledgeInputProvenanceStore) CheckKnowledgeHistoryOriginalSources(ctx context.Context, messages []*types.Message) error {
	verifier, ok := s.messageVerifier.(interfaces.MessageSourceLineageBatchVerifier)
	if !ok {
		return nil
	}
	return verifier.CheckMessagePublications(ctx, messages)
}
