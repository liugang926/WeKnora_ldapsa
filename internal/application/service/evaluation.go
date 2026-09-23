package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"runtime"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/config"
	"github.com/Tencent/WeKnora/internal/logger"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/Tencent/WeKnora/internal/utils"
	"golang.org/x/sync/errgroup"
)

/*
corpus: pid -> content
queries: qid -> content
answers: aid -> content
qrels: qid -> pid
arels: qid -> aid
*/

// EvaluationService handles evaluation tasks for knowledge base and chat models
type EvaluationService struct {
	config               *config.Config                  // Application configuration
	dataset              interfaces.DatasetService       // Service for dataset operations
	knowledgeBaseService interfaces.KnowledgeBaseService // Service for knowledge base operations
	knowledgeService     interfaces.KnowledgeService     // Service for knowledge operations
	sessionService       interfaces.SessionService       // Service for chat sessions
	modelService         interfaces.ModelService         // Service for model operations
	runRepository        *repository.EvaluationRunRepository

	evaluationMemoryStorage *evaluationMemoryStorage // In-memory storage for evaluation tasks
}

func NewEvaluationService(
	config *config.Config,
	dataset interfaces.DatasetService,
	knowledgeBaseService interfaces.KnowledgeBaseService,
	knowledgeService interfaces.KnowledgeService,
	sessionService interfaces.SessionService,
	modelService interfaces.ModelService,
	runRepository *repository.EvaluationRunRepository,
) interfaces.EvaluationService {
	evaluationMemoryStorage := newEvaluationMemoryStorage()
	return &EvaluationService{
		config:                  config,
		dataset:                 dataset,
		knowledgeBaseService:    knowledgeBaseService,
		knowledgeService:        knowledgeService,
		sessionService:          sessionService,
		modelService:            modelService,
		runRepository:           runRepository,
		evaluationMemoryStorage: evaluationMemoryStorage,
	}
}

// evaluationMemoryStorage stores evaluation tasks in memory with thread-safe access
type evaluationMemoryStorage struct {
	store map[string]*types.EvaluationDetail // Map of taskID to evaluation details
	mu    *sync.RWMutex                      // Read-write lock for concurrent access
}

func newEvaluationMemoryStorage() *evaluationMemoryStorage {
	res := &evaluationMemoryStorage{
		store: make(map[string]*types.EvaluationDetail),
		mu:    &sync.RWMutex{},
	}
	return res
}

func (e *evaluationMemoryStorage) register(params *types.EvaluationDetail) {
	e.mu.Lock()
	defer e.mu.Unlock()
	logger.Infof(context.Background(), "Registering evaluation task: %s", params.Task.ID)
	e.store[params.Task.ID] = params
}

func (e *evaluationMemoryStorage) get(taskID string) (*types.EvaluationDetail, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	logger.Infof(context.Background(), "Getting evaluation task: %s", taskID)
	res, ok := e.store[taskID]
	if !ok {
		return nil, errors.New("task not found")
	}
	return cloneEvaluationDetail(res)
}

func cloneEvaluationDetail(detail *types.EvaluationDetail) (*types.EvaluationDetail, error) {
	data, err := json.Marshal(detail)
	if err != nil {
		return nil, err
	}
	var cloned types.EvaluationDetail
	if err := json.Unmarshal(data, &cloned); err != nil {
		return nil, err
	}
	return &cloned, nil
}

func (e *EvaluationService) persistUpdate(ctx context.Context, taskID string,
	fn func(*types.EvaluationDetail),
) error {
	// Serialize the durable write with the in-memory mutation so concurrent QA
	// workers cannot persist an older progress snapshot after a newer one.
	e.evaluationMemoryStorage.mu.Lock()
	defer e.evaluationMemoryStorage.mu.Unlock()
	params, ok := e.evaluationMemoryStorage.store[taskID]
	if !ok {
		return errors.New("task not found")
	}
	fn(params)
	params.Task.UpdatedAt = time.Now().UTC()
	updated, err := cloneEvaluationDetail(params)
	if err != nil {
		return err
	}
	return e.runRepository.Save(ctx, updated)
}

func (e *EvaluationService) EvaluationResult(ctx context.Context, taskID string) (*types.EvaluationDetail, error) {
	logger.Info(ctx, "Start getting evaluation result")
	logger.Infof(ctx, "Task ID: %s", taskID)

	detail, err := e.runRepository.Get(ctx, types.MustTenantIDFromContext(ctx), taskID)
	if err != nil {
		logger.Errorf(ctx, "Failed to get evaluation task: %v", err)
		return nil, err
	}

	tenantID := types.MustTenantIDFromContext(ctx)
	logger.Infof(
		ctx,
		"Checking tenant ID match, task tenant ID: %d, current tenant ID: %d",
		detail.Task.TenantID, tenantID,
	)

	if tenantID != detail.Task.TenantID {
		logger.Error(ctx, "Tenant ID mismatch")
		return nil, errors.New("tenant ID does not match")
	}

	logger.Info(ctx, "Evaluation result retrieved successfully")
	return detail, nil
}

// ListEvaluationResults returns recent results for the current tenant.
func (e *EvaluationService) ListEvaluationResults(ctx context.Context, limit int) ([]*types.EvaluationDetail, error) {
	return e.runRepository.List(ctx, types.MustTenantIDFromContext(ctx), limit)
}

// Evaluation starts a new evaluation task with given parameters
// datasetID: ID of the dataset to evaluate against
// knowledgeBaseID: ID of the knowledge base to use (empty to create new)
// chatModelID: ID of the chat model to evaluate
// rerankModelID: ID of the rerank model to evaluate
func (e *EvaluationService) Evaluation(ctx context.Context,
	datasetID string, knowledgeBaseID string, chatModelID string, rerankModelID string, embeddingModelID string,
) (*types.EvaluationDetail, error) {
	logger.Info(ctx, "Start evaluation")
	logger.Infof(ctx, "Dataset ID: %s, Knowledge Base ID: %s, Chat Model ID: %s, Rerank Model ID: %s",
		datasetID, knowledgeBaseID, chatModelID, rerankModelID)

	// Get tenant ID from context for multi-tenancy support
	tenantID := types.MustTenantIDFromContext(ctx)
	logger.Infof(ctx, "Tenant ID: %d", tenantID)
	referenceKnowledgeBaseID := knowledgeBaseID
	var selectedEmbeddingModelID string
	createdKBID := ""
	started := false
	defer func() {
		if started || createdKBID == "" {
			return
		}
		if err := e.knowledgeBaseService.DeleteKnowledgeBase(ctx, createdKBID); err != nil {
			logger.Errorf(ctx, "Failed to clean up unused evaluation knowledge base %s: %v", createdKBID, err)
		}
	}()
	if embeddingModelID != "" {
		model, err := e.modelService.GetModelByID(ctx, embeddingModelID)
		if err != nil || model == nil || model.Type != types.ModelTypeEmbedding {
			return nil, errors.New("invalid embedding model for evaluation")
		}
	}
	if rerankModelID != "" {
		model, err := e.modelService.GetModelByID(ctx, rerankModelID)
		if err != nil || model == nil || model.Type != types.ModelTypeRerank {
			return nil, errors.New("invalid rerank model for evaluation")
		}
	}
	if chatModelID != "" {
		model, err := e.modelService.GetModelByID(ctx, chatModelID)
		if err != nil || model == nil || model.Type != types.ModelTypeKnowledgeQA {
			return nil, errors.New("invalid chat model for evaluation")
		}
	}

	// Handle knowledge base creation if not provided
	if knowledgeBaseID == "" {
		logger.Info(ctx, "No knowledge base ID provided, creating new knowledge base")
		// Create new knowledge base with default evaluation settings
		// 获取默认的嵌入模型和LLM模型
		models, err := e.modelService.ListModels(ctx)
		if err != nil {
			logger.Errorf(ctx, "Failed to list models: %v", err)
			return nil, err
		}

		var llmModelID string
		for _, model := range models {
			if model == nil {
				continue
			}
			if model.Type == types.ModelTypeEmbedding && embeddingModelID == "" {
				embeddingModelID = model.ID
			}
			if model.Type == types.ModelTypeKnowledgeQA {
				llmModelID = model.ID
			}
		}

		if embeddingModelID == "" || llmModelID == "" {
			return nil, fmt.Errorf("no default models found for evaluation")
		}

		kb, err := e.knowledgeBaseService.CreateKnowledgeBase(ctx, &types.KnowledgeBase{
			Name:             "evaluation",
			Description:      "evaluation",
			EmbeddingModelID: embeddingModelID,
			SummaryModelID:   llmModelID,
		})
		if err != nil {
			logger.Errorf(ctx, "Failed to create knowledge base: %v", err)
			return nil, err
		}
		knowledgeBaseID = kb.ID
		createdKBID = kb.ID
		selectedEmbeddingModelID = kb.EmbeddingModelID
		logger.Infof(ctx, "Created new knowledge base with ID: %s", knowledgeBaseID)
	} else {
		logger.Infof(ctx, "Using existing knowledge base ID: %s", knowledgeBaseID)
		// Create evaluation-specific knowledge base based on existing one
		kb, err := e.knowledgeBaseService.GetKnowledgeBaseByID(ctx, knowledgeBaseID)
		if err != nil {
			logger.Errorf(ctx, "Failed to get knowledge base: %v", err)
			return nil, err
		}
		if embeddingModelID != "" && embeddingModelID != kb.EmbeddingModelID {
			return nil, errors.New("evaluation embedding model differs from reference knowledge base")
		}

		kb, err = e.knowledgeBaseService.CreateKnowledgeBase(ctx, &types.KnowledgeBase{
			Name:             "evaluation",
			Description:      "evaluation",
			EmbeddingModelID: kb.EmbeddingModelID,
			SummaryModelID:   kb.SummaryModelID,
		})
		if err != nil {
			logger.Errorf(ctx, "Failed to create knowledge base: %v", err)
			return nil, err
		}
		knowledgeBaseID = kb.ID
		createdKBID = kb.ID
		selectedEmbeddingModelID = kb.EmbeddingModelID
		logger.Infof(ctx, "Created new knowledge base with ID: %s based on existing one", knowledgeBaseID)
	}

	// Set default values for optional parameters
	if datasetID == "" {
		datasetID = "default"
		logger.Info(ctx, "Using default dataset")
	}

	if rerankModelID == "" {
		// 获取默认的重排模型
		models, err := e.modelService.ListModels(ctx)
		if err == nil {
			for _, model := range models {
				if model == nil {
					continue
				}
				if model.Type == types.ModelTypeRerank {
					rerankModelID = model.ID
					break
				}
			}
		}
		if rerankModelID == "" {
			logger.Warnf(ctx, "No rerank model found, skipping rerank")
		} else {
			logger.Infof(ctx, "Using default rerank model: %s", rerankModelID)
		}
	}

	if chatModelID == "" {
		// 获取默认的LLM模型
		models, err := e.modelService.ListModels(ctx)
		if err == nil {
			for _, model := range models {
				if model == nil {
					continue
				}
				if model.Type == types.ModelTypeKnowledgeQA {
					chatModelID = model.ID
					break
				}
			}
		}
		if chatModelID == "" {
			return nil, fmt.Errorf("no default chat model found")
		}
		logger.Infof(ctx, "Using default chat model: %s", chatModelID)
	}

	// Create evaluation task with unique ID
	logger.Info(ctx, "Creating evaluation task")
	taskID := utils.GenerateTaskID("evaluation", tenantID, datasetID)
	logger.Infof(ctx, "Generated task ID: %s", taskID)

	// Prepare evaluation detail with all parameters
	detail := &types.EvaluationDetail{
		Task: &types.EvaluationTask{
			ID:                       taskID,
			TenantID:                 tenantID,
			DatasetID:                datasetID,
			ReferenceKnowledgeBaseID: referenceKnowledgeBaseID,
			EmbeddingModelID:         selectedEmbeddingModelID,
			ChatModelID:              chatModelID,
			RerankModelID:            rerankModelID,
			BuildRevision:            os.Getenv("WEKNORA_BUILD_COMMIT"),
			Status:                   types.EvaluationStatuePending,
			StartTime:                time.Now(),
			UpdatedAt:                time.Now(),
		},
		Params: &types.ChatManage{
			PipelineRequest: types.PipelineRequest{
				VectorThreshold:  e.config.Conversation.VectorThreshold,
				KeywordThreshold: e.config.Conversation.KeywordThreshold,
				EmbeddingTopK:    e.config.Conversation.EmbeddingTopK,
				MaxRounds:        e.config.Conversation.MaxRounds,
				RerankModelID:    rerankModelID,
				RerankTopK:       e.config.Conversation.RerankTopK,
				RerankThreshold:  e.config.Conversation.RerankThreshold,
				ChatModelID:      chatModelID,
				SummaryConfig: types.SummaryConfig{
					MaxTokens:           e.config.Conversation.Summary.MaxTokens,
					RepeatPenalty:       e.config.Conversation.Summary.RepeatPenalty,
					TopK:                e.config.Conversation.Summary.TopK,
					TopP:                e.config.Conversation.Summary.TopP,
					Prompt:              e.config.Conversation.Summary.Prompt,
					ContextTemplate:     e.config.Conversation.Summary.ContextTemplate,
					FrequencyPenalty:    e.config.Conversation.Summary.FrequencyPenalty,
					PresencePenalty:     e.config.Conversation.Summary.PresencePenalty,
					NoMatchPrefix:       e.config.Conversation.Summary.NoMatchPrefix,
					Temperature:         e.config.Conversation.Summary.Temperature,
					Seed:                e.config.Conversation.Summary.Seed,
					MaxCompletionTokens: e.config.Conversation.Summary.MaxCompletionTokens,
				},
				FallbackResponse:    e.config.Conversation.FallbackResponse,
				RewritePromptSystem: e.config.Conversation.RewritePromptSystem,
				RewritePromptUser:   e.config.Conversation.RewritePromptUser,
			},
		},
	}

	if err := e.runRepository.Save(ctx, detail); err != nil {
		return nil, fmt.Errorf("persist evaluation task: %w", err)
	}
	// Only publish an in-memory handle after the durable task exists.
	logger.Info(ctx, "Registering evaluation task")
	e.evaluationMemoryStorage.register(detail)

	// Start evaluation in background goroutine
	logger.Info(ctx, "Starting evaluation in background")
	started = true
	go func() {
		// Create new context with logger for background task
		newCtx := logger.CloneContext(context.WithoutCancel(ctx))
		logger.Infof(newCtx, "Background evaluation started for task ID: %s", taskID)

		// Update task status to running
		if err := e.persistUpdate(newCtx, taskID, func(params *types.EvaluationDetail) {
			params.Task.Status = types.EvaluationStatueRunning
		}); err != nil {
			logger.Errorf(newCtx, "Failed to persist evaluation running state: %v", err)
			if cleanupErr := e.knowledgeBaseService.DeleteKnowledgeBase(newCtx, knowledgeBaseID); cleanupErr != nil {
				logger.Errorf(newCtx, "Failed to clean up evaluation knowledge base: %v", cleanupErr)
			}
			return
		}
		logger.Info(newCtx, "Evaluation task status set to running")

		// Execute actual evaluation
		if err := e.EvalDataset(newCtx, detail, knowledgeBaseID); err != nil {
			if saveErr := e.persistUpdate(newCtx, taskID, func(params *types.EvaluationDetail) {
				params.Task.Status = types.EvaluationStatueFailed
				params.Task.ErrMsg = err.Error()
			}); saveErr != nil {
				logger.Errorf(newCtx, "Failed to persist evaluation failure: %v", saveErr)
			}
			logger.Errorf(newCtx, "Evaluation task failed: %v, task ID: %s", err, taskID)
			return
		}

		// Mark task as completed successfully
		logger.Infof(newCtx, "Evaluation task completed successfully, task ID: %s", taskID)
		if err := e.persistUpdate(newCtx, taskID, func(params *types.EvaluationDetail) {
			params.Task.Status = types.EvaluationStatueSuccess
		}); err != nil {
			logger.Errorf(newCtx, "Failed to persist evaluation completion: %v", err)
		}
	}()

	logger.Infof(ctx, "Evaluation task created successfully, task ID: %s", taskID)
	return detail, nil
}

// EvalDataset performs the actual evaluation of a dataset
// Processes each QA pair in parallel and records metrics
func (e *EvaluationService) EvalDataset(ctx context.Context, detail *types.EvaluationDetail, knowledgeBaseID string) error {
	logger.Info(ctx, "Start evaluating dataset")
	logger.Infof(ctx, "Task ID: %s, Dataset ID: %s", detail.Task.ID, detail.Task.DatasetID)
	defer func() {
		logger.Infof(ctx, "Cleaning up evaluation knowledge base: %s", knowledgeBaseID)
		if err := e.knowledgeBaseService.DeleteKnowledgeBase(ctx, knowledgeBaseID); err != nil {
			logger.Errorf(ctx, "Failed to delete evaluation knowledge base %s: %v", knowledgeBaseID, err)
		}
	}()

	// Retrieve dataset from storage
	dataset, err := e.dataset.GetDatasetByID(ctx, detail.Task.DatasetID)
	if err != nil {
		logger.Errorf(ctx, "Failed to get dataset: %v", err)
		return err
	}
	logger.Infof(ctx, "Dataset retrieved successfully with %d QA pairs", len(dataset.QAPairs))
	maxQuestions, err := maxEvaluationQuestions()
	if err != nil {
		return err
	}
	if len(dataset.QAPairs) > maxQuestions {
		return fmt.Errorf(
			"evaluation dataset has %d questions; configured limit is %d", len(dataset.QAPairs), maxQuestions,
		)
	}
	datasetJSON, err := json.Marshal(dataset)
	if err != nil {
		return fmt.Errorf("fingerprint evaluation dataset: %w", err)
	}
	datasetDigest := sha256.Sum256(datasetJSON)

	// Update total QA pairs count in task details
	if err := e.persistUpdate(ctx, detail.Task.ID, func(params *types.EvaluationDetail) {
		params.Task.Total = len(dataset.QAPairs)
		params.Task.DatasetSHA256 = hex.EncodeToString(datasetDigest[:])
		params.Cases = make([]*types.EvaluationCaseResult, len(dataset.QAPairs))
		logger.Infof(ctx, "Updated task total to %d QA pairs", params.Task.Total)
	}); err != nil {
		return err
	}

	// Extract and organize passages from dataset
	passages := dataset.Corpus
	logger.Infof(ctx, "Creating knowledge from %d passages", len(passages))

	// Create knowledge base from passages (sync: wait for indexing to complete before querying)
	knowledge, err := e.knowledgeService.CreateKnowledgeFromPassageSync(ctx, knowledgeBaseID, passages, "")
	if err != nil {
		logger.Errorf(ctx, "Failed to create knowledge from passages: %v", err)
		return err
	}
	logger.Infof(ctx, "Knowledge created and indexed successfully, ID: %s", knowledge.ID)

	// Setup cleanup of temporary resources
	defer func() {
		logger.Infof(ctx, "Cleaning up resources - deleting knowledge: %s", knowledge.ID)
		if err := deleteReferencedKnowledge(ctx,
			e.knowledgeService,
			knowledgeBaseID,
			[]string{knowledge.ID}); err != nil {
			logger.Errorf(ctx, "Failed to delete knowledge: %v, knowledge ID: %s", err, knowledge.ID)
		}

	}()

	// Initialize parallel evaluation metrics
	var finished int
	var latencies []int64
	var promptTokens, completionTokens int64
	var mu sync.Mutex
	var g errgroup.Group
	metricHook := NewHookMetric(len(dataset.QAPairs), passages, knowledge.ID)

	// Set worker limit based on available CPUs
	g.SetLimit(max(runtime.GOMAXPROCS(0)-1, 1))
	logger.Infof(ctx, "Starting evaluation with %d parallel workers", max(runtime.GOMAXPROCS(0)-1, 1))

	// Process each QA pair in parallel
	for i, qaPair := range dataset.QAPairs {
		qaPair := qaPair
		i := i
		g.Go(func() error {
			startedAt := time.Now()
			logger.Infof(ctx, "Processing QA pair %d", i)

			// Prepare chat management parameters for this QA pair
			chatManage := detail.Params.Clone()
			chatManage.Query = qaPair.Question
			chatManage.RewriteQuery = qaPair.Question
			// Set knowledge base ID and search targets for this evaluation
			chatManage.KnowledgeBaseIDs = []string{knowledgeBaseID}
			chatManage.SearchTargets = types.SearchTargets{
				&types.SearchTarget{
					Type:            types.SearchTargetTypeKnowledgeBase,
					KnowledgeBaseID: knowledgeBaseID,
				},
			}

			// Execute knowledge QA pipeline
			logger.Infof(ctx, "Running knowledge QA for pair %d", i)
			qaErr := e.sessionService.KnowledgeQAByEvent(ctx, chatManage, types.Pipline["rag"])
			if qaErr != nil {
				logger.Errorf(ctx, "Failed to process question %d: %v", i, qaErr)
				return qaErr
			}

			// Record evaluation metrics
			logger.Infof(ctx, "Recording metrics for QA pair %d", i)
			metricHook.recordInit(i)
			metricHook.recordQaPair(i, qaPair)
			metricHook.recordSearchResult(i, chatManage.SearchResult)
			metricHook.recordRerankResult(i, chatManage.RerankResult)
			metricHook.recordChatResponse(i, chatManage.ChatResponse)
			metricHook.recordFinish(i)
			caseResult := &types.EvaluationCaseResult{
				QuestionID:          qaPair.QID,
				Question:            qaPair.Question,
				ReferenceAnswer:     qaPair.Answer,
				RelevantPassageIDs:  slices.Clone(qaPair.PIDs),
				RetrievedPassageIDs: matchRetrievedPassageIDs(passages, chatManage.SearchResult, knowledge.ID),
				RerankedPassageIDs:  matchRetrievedPassageIDs(passages, chatManage.RerankResult, knowledge.ID),
				LatencyMs:           time.Since(startedAt).Milliseconds(),
			}
			if chatManage.ChatResponse != nil {
				caseResult.GeneratedAnswer = chatManage.ChatResponse.Content
				caseResult.PromptTokens = int64(chatManage.ChatResponse.Usage.PromptTokens)
				caseResult.CompletionTokens = int64(chatManage.ChatResponse.Usage.CompletionTokens)
			}

			// Update progress metrics
			mu.Lock()
			finished += 1
			done := finished
			latencies = append(latencies, time.Since(startedAt).Milliseconds())
			if chatManage.ChatResponse != nil {
				promptTokens += int64(chatManage.ChatResponse.Usage.PromptTokens)
				completionTokens += int64(chatManage.ChatResponse.Usage.CompletionTokens)
			}
			execution := evaluationExecutionMetrics(latencies, promptTokens, completionTokens)
			mu.Unlock()
			metricResult := metricHook.MetricResult()
			metricResult.ExecutionMetrics = execution
			if err := e.persistUpdate(ctx, detail.Task.ID, func(params *types.EvaluationDetail) {
				params.Cases[i] = caseResult
				if done > params.Task.Finished {
					params.Metric = metricResult
					params.Task.Finished = done
				}
				logger.Infof(ctx, "Updated task progress: %d/%d completed", done, params.Task.Total)
			}); err != nil {
				return err
			}
			return nil
		})
	}

	// Wait for all parallel evaluations to complete
	logger.Info(ctx, "Waiting for all evaluation tasks to complete")
	if err := g.Wait(); err != nil {
		logger.Errorf(ctx, "Evaluation error: %v", err)
		return err
	}

	// Final update of evaluation metrics
	if err := e.persistUpdate(ctx, detail.Task.ID, func(params *types.EvaluationDetail) {
		params.Metric = metricHook.MetricResult()
		params.Metric.ExecutionMetrics = evaluationExecutionMetrics(latencies, promptTokens, completionTokens)
		params.Task.Finished = finished
	}); err != nil {
		return err
	}

	logger.Infof(ctx, "Dataset evaluation completed successfully, task ID: %s", detail.Task.ID)
	return nil
}

func maxEvaluationQuestions() (int, error) {
	configured := os.Getenv("EVALUATION_MAX_QUESTIONS")
	if configured == "" {
		return 100, nil
	}
	parsed, err := strconv.Atoi(configured)
	if err != nil || parsed < 1 || parsed > 10000 {
		return 0, errors.New("EVALUATION_MAX_QUESTIONS must be between 1 and 10000")
	}
	return parsed, nil
}

func evaluationExecutionMetrics(latencies []int64, promptTokens, completionTokens int64) types.ExecutionMetrics {
	result := types.ExecutionMetrics{PromptTokens: promptTokens, CompletionTokens: completionTokens}
	if len(latencies) == 0 {
		return result
	}
	ordered := slices.Clone(latencies)
	slices.Sort(ordered)
	result.LatencyP50Ms = ordered[(len(ordered)-1)/2]
	result.LatencyP95Ms = ordered[(95*len(ordered)+99)/100-1]
	return result
}
