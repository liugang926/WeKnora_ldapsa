package handler

import (
	stderrors "errors"
	"net/http"
	"strconv"
	"time"

	"github.com/Tencent/WeKnora/internal/errors"
	"github.com/Tencent/WeKnora/internal/logger"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	secutils "github.com/Tencent/WeKnora/internal/utils"
	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

// EvaluationHandler handles evaluation related HTTP requests
type EvaluationHandler struct {
	evaluationService interfaces.EvaluationService // Service for evaluation operations
	groupAccess       interfaces.GroupAccessService
	kbService         interfaces.KnowledgeBaseService
}

// ReviewEvaluationCase persists a human verdict for one completed evaluation
// question. This route is intentionally JWT-only: an API key may run and read
// evaluations, but cannot impersonate a named human reviewer.
func (e *EvaluationHandler) ReviewEvaluationCase(c *gin.Context) {
	taskID := c.Param("taskId")
	questionID, err := strconv.Atoi(c.Param("questionId"))
	if taskID == "" || err != nil || questionID < 0 {
		_ = c.Error(errors.NewBadRequestError("Invalid evaluation case"))
		return
	}
	var input types.EvaluationCaseReviewInput
	if err := c.ShouldBindJSON(&input); err != nil {
		_ = c.Error(errors.NewBadRequestError("Invalid review labels"))
		return
	}
	detail, err := e.evaluationService.ReviewEvaluationCase(c.Request.Context(), taskID, questionID, input)
	if err != nil {
		switch {
		case stderrors.Is(err, types.ErrEvaluationReviewInvalid):
			_ = c.Error(errors.NewBadRequestError("Review labels do not match case type"))
		case stderrors.Is(err, types.ErrEvaluationReviewNotReady),
			stderrors.Is(err, types.ErrEvaluationReviewConflict):
			_ = c.Error(errors.NewConflictError("Evaluation review is not ready; refresh and retry"))
		case stderrors.Is(err, types.ErrEvaluationCaseNotFound), stderrors.Is(err, gorm.ErrRecordNotFound):
			_ = c.Error(errors.NewNotFoundError("Evaluation case not found"))
		default:
			logger.Errorf(c.Request.Context(), "Failed to persist evaluation review: %v", err)
			_ = c.Error(errors.NewInternalServerError("Evaluation review unavailable"))
		}
		return
	}
	c.JSON(http.StatusOK, gin.H{"success": true, "data": detail})
}

// SetEvaluationChatCost persists an operator tariff snapshot for a completed
// run. It is JWT-only so an API key cannot impersonate a named operator.
func (e *EvaluationHandler) SetEvaluationChatCost(c *gin.Context) {
	taskID := c.Param("taskId")
	if taskID == "" {
		_ = c.Error(errors.NewBadRequestError("Invalid evaluation task"))
		return
	}
	var request struct {
		Currency         string   `json:"currency"`
		TariffVersion    string   `json:"tariff_version"`
		InputPerMillion  *float64 `json:"input_per_million"`
		OutputPerMillion *float64 `json:"output_per_million"`
	}
	if err := c.ShouldBindJSON(&request); err != nil ||
		request.InputPerMillion == nil || request.OutputPerMillion == nil {
		_ = c.Error(errors.NewBadRequestError("Invalid chat tariff"))
		return
	}
	input := types.EvaluationChatCostInput{
		Currency: request.Currency, TariffVersion: request.TariffVersion,
		InputPerMillion: *request.InputPerMillion, OutputPerMillion: *request.OutputPerMillion,
	}
	detail, err := e.evaluationService.SetEvaluationChatCost(c.Request.Context(), taskID, input)
	if err != nil {
		switch {
		case stderrors.Is(err, types.ErrEvaluationChatCostInvalid):
			_ = c.Error(errors.NewBadRequestError("Invalid chat tariff"))
		case stderrors.Is(err, types.ErrEvaluationChatCostNotReady),
			stderrors.Is(err, types.ErrEvaluationChatCostConflict):
			_ = c.Error(errors.NewConflictError("Evaluation chat cost is not ready; refresh and retry"))
		case stderrors.Is(err, gorm.ErrRecordNotFound):
			_ = c.Error(errors.NewNotFoundError("Evaluation run not found"))
		default:
			logger.Errorf(c.Request.Context(), "Failed to persist evaluation chat tariff: %v", err)
			_ = c.Error(errors.NewInternalServerError("Evaluation chat tariff unavailable"))
		}
		return
	}
	c.JSON(http.StatusOK, gin.H{"success": true, "data": detail})
}

// NewEvaluationHandler creates a new EvaluationHandler instance
func NewEvaluationHandler(evaluationService interfaces.EvaluationService) *EvaluationHandler {
	return &EvaluationHandler{evaluationService: evaluationService}
}

// ConfigureEvaluationGroupAccess installs the KB policy overlay while keeping
// the existing constructor stable for downstream compositions.
func ConfigureEvaluationGroupAccess(
	h *EvaluationHandler,
	groupAccess interfaces.GroupAccessService,
	kbService interfaces.KnowledgeBaseService,
) {
	if h != nil {
		h.groupAccess = groupAccess
		h.kbService = kbService
	}
}

// EvaluationRequest contains parameters for evaluation request
type EvaluationRequest struct {
	DatasetID        string `json:"dataset_id"`        // ID of dataset to evaluate
	KnowledgeBaseID  string `json:"knowledge_base_id"` // ID of knowledge base to use
	ChatModelID      string `json:"chat_id"`           // ID of chat model to use
	RerankModelID    string `json:"rerank_id"`         // ID of rerank model to use
	EmbeddingModelID string `json:"embedding_id"`      // Explicit embedding model for an isolated evaluation KB
}

// Evaluation godoc
// @Summary      执行评估
// @Description  对知识库进行评估测试
// @Tags         评估
// @Accept       json
// @Produce      json
// @Param        request  body      EvaluationRequest  true  "评估请求参数"
// @Success      200      {object}  map[string]interface{}  "评估任务"
// @Failure      400      {object}  errors.AppError         "请求参数错误"
// @Security     Bearer
// @Security     ApiKeyAuth
// @Router       /evaluation/ [post]
func (e *EvaluationHandler) Evaluation(c *gin.Context) {
	ctx := c.Request.Context()

	logger.Info(ctx, "Start processing evaluation request")

	var request EvaluationRequest
	if err := c.ShouldBind(&request); err != nil {
		logger.Error(ctx, "Failed to parse request parameters", err)
		c.Error(errors.NewBadRequestError("Invalid request parameters").WithDetails(err.Error()))
		return
	}

	tenantID, exists := c.Get(string(types.TenantIDContextKey))
	if !exists {
		logger.Error(ctx, "Failed to get tenant ID")
		c.Error(errors.NewUnauthorizedError("Unauthorized"))
		return
	}
	if request.KnowledgeBaseID != "" && e.groupAccess != nil {
		tenantIDValue, ok := tenantID.(uint64)
		if !ok || tenantIDValue == 0 {
			logger.Error(ctx, "Invalid tenant ID in evaluation authorization context")
			_ = c.Error(errors.NewUnauthorizedError("Unauthorized"))
			return
		}
		if e.kbService == nil {
			_ = c.Error(errors.NewServiceUnavailableError("Cannot verify knowledge base ownership right now"))
			return
		}
		kb, kbErr := e.kbService.GetKnowledgeBaseByIDOnly(ctx, request.KnowledgeBaseID)
		if kbErr != nil || kb == nil {
			_ = c.Error(errors.NewNotFoundError("Knowledge base not found"))
			return
		}
		if kb.TenantID != tenantIDValue {
			_ = c.Error(errors.NewForbiddenError("Knowledge base belongs to another workspace"))
			return
		}
		permission, accessErr := e.groupAccess.EffectivePermission(
			ctx,
			kb.TenantID,
			types.GroupResourceTypeKnowledgeBase,
			request.KnowledgeBaseID,
			types.ResourceActionRead,
			time.Now().UTC(),
		)
		if accessErr != nil {
			logger.Errorf(ctx, "Failed to verify evaluation knowledge base access: %v", accessErr)
			_ = c.Error(errors.NewServiceUnavailableError("Cannot verify knowledge base access right now"))
			return
		}
		if !permission.Allowed {
			_ = c.Error(errors.NewForbiddenError("Directory group permission required for this knowledge base"))
			return
		}
	}

	logger.Infof(ctx, "Executing evaluation, tenant: %v, dataset: %s, knowledge_base: %s, chat: %s, rerank: %s",
		tenantID,
		secutils.SanitizeForLog(request.DatasetID),
		secutils.SanitizeForLog(request.KnowledgeBaseID),
		secutils.SanitizeForLog(request.ChatModelID),
		secutils.SanitizeForLog(request.RerankModelID),
	)

	task, err := e.evaluationService.Evaluation(ctx,
		secutils.SanitizeForLog(request.DatasetID),
		secutils.SanitizeForLog(request.KnowledgeBaseID),
		secutils.SanitizeForLog(request.ChatModelID),
		secutils.SanitizeForLog(request.RerankModelID),
		secutils.SanitizeForLog(request.EmbeddingModelID),
	)
	if err != nil {
		logger.ErrorWithFields(ctx, err, nil)
		c.Error(errors.NewInternalServerError(err.Error()))
		return
	}

	logger.Infof(ctx, "Evaluation task created successfully")
	c.JSON(http.StatusOK, gin.H{
		"success": true,
		"data":    task,
	})
}

// GetEvaluationRequest contains parameters for getting evaluation result
type GetEvaluationRequest struct {
	TaskID string `form:"task_id"` // Omit to list recent runs for this tenant
}

// GetEvaluationResult godoc
// @Summary      获取评估结果
// @Description  根据任务ID获取评估结果
// @Tags         评估
// @Accept       json
// @Produce      json
// @Param        task_id  query     string  true  "评估任务ID"
// @Success      200      {object}  map[string]interface{}  "评估结果"
// @Failure      400      {object}  errors.AppError         "请求参数错误"
// @Security     Bearer
// @Security     ApiKeyAuth
// @Router       /evaluation/ [get]
func (e *EvaluationHandler) GetEvaluationResult(c *gin.Context) {
	ctx := c.Request.Context()

	logger.Info(ctx, "Start retrieving evaluation result")

	var request GetEvaluationRequest
	if err := c.ShouldBind(&request); err != nil {
		logger.Error(ctx, "Failed to parse request parameters", err)
		c.Error(errors.NewBadRequestError("Invalid request parameters").WithDetails(err.Error()))
		return
	}

	if request.TaskID == "" {
		results, err := e.evaluationService.ListEvaluationResults(ctx, 20)
		if err != nil {
			logger.ErrorWithFields(ctx, err, nil)
			_ = c.Error(errors.NewInternalServerError("Evaluation history unavailable"))
			return
		}
		c.JSON(http.StatusOK, gin.H{"success": true, "data": results})
		return
	}

	result, err := e.evaluationService.EvaluationResult(ctx, secutils.SanitizeForLog(request.TaskID))
	if err != nil {
		logger.ErrorWithFields(ctx, err, nil)
		c.Error(errors.NewInternalServerError(err.Error()))
		return
	}

	logger.Info(ctx, "Retrieved evaluation result successfully")
	c.JSON(http.StatusOK, gin.H{
		"success": true,
		"data":    result,
	})
}
