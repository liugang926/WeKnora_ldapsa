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
	batch, ok := s.messageVerifier.(interfaces.MessageSourceLineageBatchVerifier)
	if !ok {
		return types.ErrSourceLineageUnknown
	}
	var parent knowledgeInputHistoryParents
	if err := tx.Where("admission_id = ? AND tenant_id = ?", a.AdmissionID, k.TenantID).Take(&parent).Error; err != nil {
		return err
	}
	var session types.Session
	if err := tx.Where("id = ? AND tenant_id = ?", parent.SessionID, parent.TenantID).Take(&session).Error; err != nil {
		return err
	}
	messages := make([]*types.Message, 0, 2)
	for _, item := range []struct {
		ID, Role, Digest string
		Revision         uint64
	}{{parent.AssistantMessageID, "assistant", parent.AssistantDigest, parent.AssistantRevision}, {parent.UserMessageID, "user", parent.UserDigest, parent.UserRevision}} {
		var message types.Message
		if err := tx.Where("id = ? AND session_id = ? AND request_id = ? AND role = ?", item.ID, parent.SessionID, parent.RequestID, item.Role).Take(&message).Error; err != nil {
			return err
		}
		if !message.IsCompleted || message.SourceLineage.RequireComplete() != nil || (item.Role == "assistant" && message.KnowledgeID != k.ID) {
			return ErrKnowledgeInputChanged
		}
		digest, err := historyMessageInputDigest(&message)
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
		messages = append(messages, &message)
	}
	return batch.CheckMessagePublications(tx.Statement.Context, messages)
}

var historyInputThinkBlocks = regexp.MustCompile(`(?s)<think>.*?</think>`)

// CreateCommittedHistoryKnowledgeInput reads the complete assistant and its
// unique original paired user, binds their exact envelopes/revisions, creates
// the child and links the message in one transaction. No caller text is used.
func (s *KnowledgeInputProvenanceStore) CreateCommittedHistoryKnowledgeInput(ctx context.Context, request types.CommittedHistoryKnowledgeInputRequest) (*types.CommittedHistoryKnowledgeInput, error) {
	batch, ok := s.messageVerifier.(interfaces.MessageSourceLineageBatchVerifier)
	if !ok || request.TenantID == 0 || request.KnowledgeBaseID == "" || request.SessionID == "" || request.AssistantMessageID == "" {
		return nil, types.ErrSourceLineageUnknown
	}
	options := &sql.TxOptions{}
	if s.db.Dialector.Name() == "postgres" {
		options.Isolation = sql.LevelRepeatableRead
	}
	var result *types.CommittedHistoryKnowledgeInput
	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var session types.Session
		if err := tx.Where("id = ? AND tenant_id = ?", request.SessionID, request.TenantID).Take(&session).Error; err != nil {
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
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("session_id = ? AND request_id = ? AND role = 'user'", request.SessionID, assistant.RequestID).Limit(2).Find(&users).Error; err != nil {
			return err
		}
		if len(users) != 1 || !users[0].IsCompleted || users[0].SourceLineage.RequireComplete() != nil {
			return nilHistoryInputError()
		}
		user := users[0]
		if err := batch.CheckMessagePublications(ctx, []*types.Message{&assistant, user}); err != nil {
			return err
		}
		lineage, err := types.UnionSourceLineage(assistant.SourceLineage, user.SourceLineage)
		if err != nil {
			return err
		}
		var kb types.KnowledgeBase
		if err := tx.Where("id = ? AND tenant_id = ?", request.KnowledgeBaseID, request.TenantID).Take(&kb).Error; err != nil {
			return err
		}
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
