package service

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/tracing/langfuse"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/hibiken/asynq"
)

func (s *knowledgeService) CreateCommittedHistoryKnowledgeInput(
	ctx context.Context,
	request types.CommittedHistoryKnowledgeInputRequest,
) (*types.CommittedHistoryKnowledgeInput, error) {
	writer, ok := s.knowledgeInputWriter.(interfaces.CommittedHistoryKnowledgeInputWriter)
	if !ok || s.knowledgeInputs == nil {
		return nil, types.ErrSourceLineageUnknown
	}
	kb, err := s.kbService.GetKnowledgeBaseByID(ctx, request.KnowledgeBaseID)
	if err != nil {
		return nil, err
	}
	if kb == nil || kb.TenantID != request.TenantID {
		return nil, repository.ErrKnowledgeInputChanged
	}
	if err := access.RequireKBWrite(ctx, kb); err != nil {
		return nil, err
	}

	if s.historyMaterialMessages == nil || s.historyMaterialMessages.materialStore == nil {
		return nil, types.ErrSourceLineageUnknown
	}
	parentRequest, err := s.historyMaterialMessages.materialStore.CommittedMessageParentRequest(
		ctx,
		request.TenantID,
		request.SessionID,
		request.AssistantMessageID,
	)
	if err != nil {
		return nil, err
	}
	material, err := s.historyMaterialMessages.beginMessageMaterialRead(ctx, parentRequest)
	if err != nil {
		return nil, err
	}
	defer material.Close()
	ctx = material.Context()
	result, err := writer.CreateCommittedHistoryKnowledgeInput(ctx, request)
	if err != nil {
		return nil, err
	}
	if err := material.Recheck(); err != nil {
		return nil, err
	}
	ctx = repository.WithKnowledgeBuildInputProof(ctx, s.knowledgeInputs, result.Proof)
	if err := requireActualKnowledgeBuildInput(ctx); err != nil {
		return nil, err
	}
	eff := ResolveProcessConfig(kb, nil)
	payload := types.DocumentProcessPayload{
		OriginalInput:            result.Proof,
		TenantID:                 request.TenantID,
		KnowledgeBaseID:          kb.ID,
		KnowledgeID:              result.Knowledge.ID,
		Passages:                 result.Passages,
		EnableQuestionGeneration: eff.QuestionGenerationConfig.Enabled,
		QuestionCount:            eff.QuestionGenerationConfig.QuestionCount,
		Language:                 types.LanguageFromContextOrDefault(ctx),
	}
	langfuse.InjectTracing(ctx, &payload)
	data, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	if s.task == nil {
		return nil, fmt.Errorf("history processing queue unavailable")
	}
	_, err = s.task.Enqueue(asynq.NewTask(types.TypeDocumentProcess, data, asynq.Queue(types.QueueDefault)))
	if err != nil {
		return nil, err
	}
	return result, nil
}
