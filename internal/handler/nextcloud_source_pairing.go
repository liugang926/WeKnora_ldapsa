package handler

import (
	"context"
	"errors"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/datasource/connector/nextcloud"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

var (
	sourcePairKeyPattern     = regexp.MustCompile(`^pair_[0-9a-f]{32}$`)
	sourceRotationKeyPattern = regexp.MustCompile(`^rot_[0-9a-f]{32}$`)
)

// NextcloudSourcePairingHandler manages trusted source-pairing transitions.
type NextcloudSourcePairingHandler struct {
	repo             *repository.NextcloudSourcePairingRepository
	dataSource       *DataSourceHandler
	inspect          func(context.Context, *types.DataSourceConfig) (nextcloud.PairingIdentity, error)
	commit           func(context.Context, *types.DataSourceConfig, nextcloud.SourcePairingCommit) error
	abort            func(context.Context, *types.DataSourceConfig, nextcloud.SourcePairingCommit) error
	rotateCommit     func(context.Context, *types.DataSourceConfig, nextcloud.SourceRotation) error
	rotateFinalize   func(context.Context, *types.DataSourceConfig, nextcloud.SourceRotation) error
	rotateAbort      func(context.Context, *types.DataSourceConfig, nextcloud.SourceRotation) error
	readDecommission func(context.Context, *types.DataSourceConfig, string,
		string) (nextcloud.SourceDecommission, error)
	ackDecommission func(context.Context, *types.DataSourceConfig, nextcloud.SourceDecommission) error
}

// NewNextcloudSourcePairingHandler constructs the trusted source-pairing handler.
func NewNextcloudSourcePairingHandler(repo *repository.NextcloudSourcePairingRepository,
	dataSource *DataSourceHandler,
) *NextcloudSourcePairingHandler {
	return &NextcloudSourcePairingHandler{
		repo: repo, dataSource: dataSource,
		inspect: nextcloud.InspectPairing, commit: nextcloud.CommitSourcePairing,
		abort:        nextcloud.AbortSourcePairing,
		rotateCommit: nextcloud.CommitSourceRotation, rotateFinalize: nextcloud.FinalizeSourceRotation,
		rotateAbort:      nextcloud.AbortSourceRotation,
		readDecommission: nextcloud.ReadSourceDecommission,
		ackDecommission:  nextcloud.AcknowledgeEmptySourceDecommission,
	}
}

func (h *NextcloudSourcePairingHandler) admin(c *gin.Context) (uint64, bool) {
	c.Header("Cache-Control", "no-store")
	if _, apiKey := types.TenantAPIKeyScopeFromContext(c.Request.Context()); apiKey {
		c.JSON(http.StatusForbidden, gin.H{"error": "admin_session_required"})
		return 0, false
	}
	if !types.TenantRoleFromContext(c.Request.Context()).HasPermission(types.TenantRoleAdmin) {
		c.JSON(http.StatusForbidden, gin.H{"error": "admin_required"})
		return 0, false
	}
	tenantID := h.dataSource.getTenantID(c)
	if tenantID == 0 {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "unauthorized"})
		return 0, false
	}
	return tenantID, true
}

type sourcePairingRequest struct {
	KnowledgeBaseID  string `json:"knowledge_base_id" binding:"required"`
	BaseURL          string `json:"base_url" binding:"required"`
	BindingID        string `json:"binding_id" binding:"required"`
	OperationID      string `json:"operation_id" binding:"required"`
	InstanceID       string `json:"instance_id" binding:"required"`
	PublicationEpoch *int64 `json:"publication_epoch" binding:"required"`
	KeyID            string `json:"key_id" binding:"required"`
	Token            string `json:"token" binding:"required"`
}

// Pair performs the second half of a Nextcloud administrator's one-time
// preparation. It never echoes the bearer secret. A lost remote ACK leaves
// the local source paused and exposes a retryable pending operation.
func (h *NextcloudSourcePairingHandler) Pair(c *gin.Context) {
	tenantID, ok := h.admin(c)
	if !ok {
		return
	}
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, 16<<10)
	var req sourcePairingRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid_source_pairing_request"})
		return
	}
	parsedOperation, parseErr := uuid.Parse(req.OperationID)
	if parseErr == nil {
		req.OperationID = parsedOperation.String()
	}
	if !sourcePairKeyPattern.MatchString(req.KeyID) ||
		parseErr != nil ||
		req.KeyID != "pair_"+strings.ReplaceAll(req.OperationID, "-", "") ||
		req.PublicationEpoch == nil || *req.PublicationEpoch < 0 || len(req.InstanceID) > 128 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid_source_pairing_request"})
		return
	}
	if _, status, message := h.dataSource.getOwnedKnowledgeBase(c.Request.Context(), tenantID,
		req.KnowledgeBaseID, types.ResourceActionEdit); status != http.StatusOK {
		c.JSON(status, gin.H{"error": message})
		return
	}
	config := &types.DataSourceConfig{
		Type:        types.ConnectorTypeNextcloud,
		Credentials: map[string]interface{}{"token": req.Token, "key_id": req.KeyID},
		ResourceIDs: []string{req.BindingID},
		Settings:    map[string]interface{}{"base_url": req.BaseURL},
	}
	identity, err := h.inspect(c.Request.Context(), config)
	if err != nil || identity.InstanceID != req.InstanceID || identity.BindingID != req.BindingID ||
		identity.PublicationState != "active" || identity.PublicationEpoch != *req.PublicationEpoch {
		c.JSON(http.StatusConflict, gin.H{"error": "nextcloud_prepared_binding_unavailable"})
		return
	}
	pair, created, err := h.repo.PrepareSourcePairing(c.Request.Context(), repository.NextcloudSourcePairing{
		OperationID: req.OperationID, TenantID: tenantID, KnowledgeBaseID: req.KnowledgeBaseID,
		InstanceID: identity.InstanceID, BindingID: identity.BindingID, BaseURL: identity.BaseURL,
		PublicationEpoch: *req.PublicationEpoch, KeyID: req.KeyID,
	}, req.Token)
	if err != nil {
		sourcePairingError(c, err)
		return
	}
	if pair.State == "active" {
		c.JSON(http.StatusOK, gin.H{"pairing": pair})
		return
	}
	h.commitPending(c, pair, config, created)
}

func sourcePairingError(c *gin.Context, err error) {
	switch {
	case errors.Is(err, repository.ErrNextcloudSourcePairingMissing):
		c.JSON(http.StatusNotFound, gin.H{"error": "source_pairing_not_found"})
	case errors.Is(err, repository.ErrNextcloudSourcePairingConflict):
		c.JSON(http.StatusConflict, gin.H{"error": "source_pairing_conflict"})
	default:
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "source_pairing_unavailable"})
	}
}

func (h *NextcloudSourcePairingHandler) authorizedPair(c *gin.Context) (repository.NextcloudSourcePairing,
	*types.DataSource, bool,
) {
	tenantID, ok := h.admin(c)
	if !ok {
		return repository.NextcloudSourcePairing{}, nil, false
	}
	pair, ds, err := h.repo.SourcePairing(c.Request.Context(), tenantID, c.Param("operation_id"))
	if err != nil {
		sourcePairingError(c, err)
		return pair, nil, false
	}
	if pair.State != "aborted" {
		if _, status, message := h.dataSource.getOwnedKnowledgeBase(c.Request.Context(), tenantID,
			pair.KnowledgeBaseID, types.ResourceActionEdit); status != http.StatusOK {
			c.JSON(status, gin.H{"error": message})
			return pair, nil, false
		}
	}
	return pair, ds, true
}

// Status returns the tenant-scoped pairing status.
func (h *NextcloudSourcePairingHandler) Status(c *gin.Context) {
	pair, _, ok := h.authorizedPair(c)
	if !ok {
		return
	}
	c.JSON(http.StatusOK, gin.H{"pairing": pair})
}

// Retry retries an eligible tenant-scoped source-pairing operation.
func (h *NextcloudSourcePairingHandler) Retry(c *gin.Context) {
	pair, ds, ok := h.authorizedPair(c)
	if !ok {
		return
	}
	if pair.State == "active" {
		c.JSON(http.StatusOK, gin.H{"pairing": pair})
		return
	}
	if pair.State != "pending" || ds == nil {
		c.JSON(http.StatusConflict, gin.H{"error": "source_pairing_closed"})
		return
	}
	config, err := ds.ParseConfig()
	if err != nil || config == nil {
		c.JSON(http.StatusConflict, gin.H{"error": "source_pairing_credential_unavailable"})
		return
	}
	identity, err := h.inspect(c.Request.Context(), config)
	if err != nil || identity.InstanceID != pair.InstanceID || identity.BindingID != pair.BindingID ||
		identity.BaseURL != pair.BaseURL || identity.PublicationState != "active" {
		c.JSON(http.StatusConflict, gin.H{"error": "nextcloud_prepared_binding_unavailable"})
		return
	}
	h.commitPending(c, pair, config, false)
}

type nextcloudFailedCandidateRetryRequest struct {
	SourceETag  string `json:"source_etag" binding:"required"`
	CandidateID string `json:"candidate_id" binding:"required"`
}

// FailedCandidates is the administrator's bounded entry point from one data
// source card. It returns neither candidate content nor pairing credentials.
func (h *NextcloudSourcePairingHandler) FailedCandidates(c *gin.Context) {
	tenantID, ok := h.admin(c)
	if !ok {
		return
	}
	limit := 25
	if raw := c.Query("limit"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 1 || parsed > 50 {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid_failed_candidate_page"})
			return
		}
		limit = parsed
	}
	cursor := c.Query("cursor")
	if len(cursor) > 512 || len(c.Param("id")) > 128 || c.Param("id") == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid_failed_candidate_page"})
		return
	}
	pair, _, err := h.repo.ActiveSourcePairingForDataSource(c.Request.Context(), tenantID, c.Param("id"))
	if err != nil {
		sourcePairingError(c, err)
		return
	}
	if _, status, message := h.dataSource.getOwnedKnowledgeBase(c.Request.Context(), tenantID,
		pair.KnowledgeBaseID, types.ResourceActionEdit); status != http.StatusOK {
		c.JSON(status, gin.H{"error": message})
		return
	}
	items, nextCursor, err := h.repo.ListFailedCandidates(c.Request.Context(), pair, limit, cursor)
	if err != nil {
		sourcePairingError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"operation_id": pair.OperationID, "candidates": items, "next_cursor": nextCursor})
}

// FailedCandidateRetryStatus returns the status of a failed-candidate retry.
func (h *NextcloudSourcePairingHandler) FailedCandidateRetryStatus(c *gin.Context) {
	pair, ds, ok := h.authorizedPair(c)
	if !ok {
		return
	}
	fileID, err := strconv.ParseInt(c.Param("file_id"), 10, 64)
	if ds == nil || pair.State != "active" || err != nil || fileID < 1 ||
		strconv.FormatInt(fileID, 10) != c.Param("file_id") {
		c.JSON(http.StatusConflict, gin.H{"error": "failed_candidate_retry_conflict"})
		return
	}
	status, err := h.repo.FailedCandidateRetryStatus(c.Request.Context(), pair, fileID)
	if err != nil {
		sourcePairingError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"retry": status})
}

// RetryFailedCandidate restarts the 24-hour retry window for one exact failed
// source generation. It cannot make the old knowledge ID visible or bypass a
// live Nextcloud fetch/publication check.
func (h *NextcloudSourcePairingHandler) RetryFailedCandidate(c *gin.Context) {
	pair, ds, ok := h.authorizedPair(c)
	if !ok {
		return
	}
	fileID, err := strconv.ParseInt(c.Param("file_id"), 10, 64)
	if ds == nil || pair.State != "active" || err != nil || fileID < 1 ||
		strconv.FormatInt(fileID, 10) != c.Param("file_id") {
		c.JSON(http.StatusConflict, gin.H{"error": "failed_candidate_retry_conflict"})
		return
	}
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, 4096)
	var req nextcloudFailedCandidateRetryRequest
	if err := c.ShouldBindJSON(&req); err != nil || len(req.SourceETag) > 256 || len(req.CandidateID) > 128 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid_failed_candidate_retry_request"})
		return
	}
	if err := h.repo.RetryFailedCandidate(c.Request.Context(), pair, fileID,
		req.SourceETag, req.CandidateID, time.Now().UTC()); err != nil {
		sourcePairingError(c, err)
		return
	}
	c.JSON(http.StatusAccepted, gin.H{
		"state": "retry_scheduled", "file_id": c.Param("file_id"),
		"source_etag": req.SourceETag, "candidate_id": req.CandidateID,
	})
}

// Abort keeps the paused source intact until Nextcloud confirms the exact
// intent is closed. A lost remote ACK leaves this operation pending for retry.
func (h *NextcloudSourcePairingHandler) Abort(c *gin.Context) {
	pair, ds, ok := h.authorizedPair(c)
	if !ok {
		return
	}
	if pair.State == "aborted" {
		c.JSON(http.StatusOK, gin.H{"pairing": pair})
		return
	}
	if pair.State != "pending" || ds == nil {
		c.JSON(http.StatusConflict, gin.H{"error": "active_source_pairing_cannot_abort"})
		return
	}
	config, err := ds.ParseConfig()
	if err != nil || config == nil {
		c.JSON(http.StatusConflict, gin.H{"error": "source_pairing_credential_unavailable"})
		return
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), 20*time.Second)
	defer cancel()
	err = h.abort(ctx, config, nextcloud.SourcePairingCommit{
		OperationID: pair.OperationID, InstanceID: pair.InstanceID,
		BindingID: pair.BindingID, TenantID: pair.TenantID,
		KnowledgeBaseID: pair.KnowledgeBaseID, DataSourceID: pair.DataSourceID,
	})
	if err != nil {
		code := "remote_abort_uncertain"
		status := http.StatusAccepted
		var remote *nextcloud.SourcePairingRemoteError
		if errors.As(err, &remote) && (remote.StatusCode == http.StatusUnauthorized ||
			remote.StatusCode == http.StatusForbidden || remote.StatusCode == http.StatusConflict) {
			code = "remote_abort_conflict"
			status = http.StatusConflict
		}
		_ = h.repo.RecordSourcePairingFailure(c.Request.Context(), pair, code)
		pair.LastErrorCode = code
		c.JSON(status, gin.H{"pairing": pair, "error": code})
		return
	}
	if err := h.repo.AbortPendingSourcePairing(c.Request.Context(), pair); err != nil {
		_ = h.repo.RecordSourcePairingFailure(c.Request.Context(), pair, "local_abort_pending")
		pair.LastErrorCode = "local_abort_pending"
		c.JSON(http.StatusAccepted, gin.H{"pairing": pair, "error": "local_abort_pending"})
		return
	}
	pair.State = "aborted"
	pair.LastErrorCode = ""
	c.JSON(http.StatusOK, gin.H{"pairing": pair})
}

func (h *NextcloudSourcePairingHandler) commitPending(c *gin.Context, pair repository.NextcloudSourcePairing,
	config *types.DataSourceConfig, created bool,
) {
	ctx, cancel := context.WithTimeout(c.Request.Context(), 20*time.Second)
	defer cancel()
	err := h.commit(ctx, config, nextcloud.SourcePairingCommit{
		OperationID: pair.OperationID,
		InstanceID:  pair.InstanceID, BindingID: pair.BindingID, TenantID: pair.TenantID,
		KnowledgeBaseID: pair.KnowledgeBaseID, DataSourceID: pair.DataSourceID,
	})
	if err != nil {
		code := "remote_outcome_uncertain"
		status := http.StatusAccepted
		var remote *nextcloud.SourcePairingRemoteError
		if errors.As(err, &remote) {
			switch remote.StatusCode {
			case http.StatusUnauthorized, http.StatusForbidden, http.StatusConflict:
				code = "remote_commit_conflict"
				status = http.StatusConflict
			case http.StatusLocked:
				code = "publication_stopped"
			}
		}
		if recordErr := h.repo.RecordSourcePairingFailure(c.Request.Context(), pair, code); recordErr != nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "source_pairing_status_unavailable"})
			return
		}
		pair.LastErrorCode = code
		c.JSON(status, gin.H{"pairing": pair, "error": code})
		return
	}
	if err := h.repo.ActivateSourcePairing(c.Request.Context(), pair); err != nil {
		_ = h.repo.RecordSourcePairingFailure(c.Request.Context(), pair, "local_activation_pending")
		pair.LastErrorCode = "local_activation_pending"
		c.JSON(http.StatusAccepted, gin.H{"pairing": pair, "error": "local_activation_pending"})
		return
	}
	pair.State = "active"
	pair.LastErrorCode = ""
	if created {
		c.JSON(http.StatusCreated, gin.H{"pairing": pair})
	} else {
		c.JSON(http.StatusOK, gin.H{"pairing": pair})
	}
}
