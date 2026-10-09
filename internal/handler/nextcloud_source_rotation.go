package handler

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/datasource/connector/nextcloud"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

type sourceRotationRequest struct {
	OperationID string `json:"operation_id" binding:"required"`
	NewKeyID    string `json:"new_key_id" binding:"required"`
	Token       string `json:"token" binding:"required"`
}

// Rotate accepts the one-time key from a Nextcloud administrator, then keeps
// it encrypted until the signed remote commit and local switch both complete.
func (h *NextcloudSourcePairingHandler) Rotate(c *gin.Context) {
	pair, _, ok := h.authorizedPair(c)
	if !ok {
		return
	}
	if pair.State != "active" {
		c.JSON(http.StatusConflict, gin.H{"error": "active_source_pairing_required"})
		return
	}
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, 16<<10)
	var req sourceRotationRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid_source_rotation_request"})
		return
	}
	parsed, err := uuid.Parse(req.OperationID)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid_source_rotation_request"})
		return
	}
	req.OperationID = parsed.String()
	if !sourceRotationKeyPattern.MatchString(req.NewKeyID) ||
		req.NewKeyID != "rot_"+strings.ReplaceAll(req.OperationID, "-", "") {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid_source_rotation_request"})
		return
	}
	config := &types.DataSourceConfig{
		Type:        types.ConnectorTypeNextcloud,
		Credentials: map[string]interface{}{"token": req.Token, "key_id": req.NewKeyID},
		ResourceIDs: []string{pair.BindingID},
		Settings:    map[string]interface{}{"base_url": pair.BaseURL},
	}
	identity, err := h.inspect(c.Request.Context(), config)
	if err != nil || identity.InstanceID != pair.InstanceID ||
		identity.BindingID != pair.BindingID || identity.BaseURL != pair.BaseURL ||
		identity.PublicationState != "active" {
		c.JSON(http.StatusConflict, gin.H{"error": "nextcloud_paired_binding_unavailable"})
		return
	}
	rotation, created, err := h.repo.PrepareSourceRotation(c.Request.Context(),
		repository.NextcloudSourceRotation{
			OperationID:     req.OperationID,
			PairOperationID: pair.OperationID, TenantID: pair.TenantID,
			KnowledgeBaseID: pair.KnowledgeBaseID, DataSourceID: pair.DataSourceID,
			InstanceID: pair.InstanceID, BindingID: pair.BindingID,
			NewKeyID: req.NewKeyID,
		}, req.Token)
	if err != nil {
		sourcePairingError(c, err)
		return
	}
	h.continueRotation(c, rotation, config, created)
}

func (h *NextcloudSourcePairingHandler) authorizedRotation(c *gin.Context) (repository.NextcloudSourceRotation, bool) {
	pair, _, ok := h.authorizedPair(c)
	if !ok {
		return repository.NextcloudSourceRotation{}, false
	}
	rotation, err := h.repo.SourceRotation(c.Request.Context(), pair.TenantID, c.Param("rotation_id"))
	if err != nil {
		sourcePairingError(c, err)
		return rotation, false
	}
	if rotation.PairOperationID != pair.OperationID || rotation.KnowledgeBaseID != pair.KnowledgeBaseID ||
		rotation.DataSourceID != pair.DataSourceID || rotation.InstanceID != pair.InstanceID ||
		rotation.BindingID != pair.BindingID {
		c.JSON(http.StatusConflict, gin.H{"error": "source_rotation_conflict"})
		return rotation, false
	}
	return rotation, true
}

// RotationStatus returns tenant-scoped source-credential rotation status.
func (h *NextcloudSourcePairingHandler) RotationStatus(c *gin.Context) {
	rotation, ok := h.authorizedRotation(c)
	if !ok {
		return
	}
	c.JSON(http.StatusOK, gin.H{"rotation": rotation})
}

// RetryRotation retries an eligible tenant-scoped source-credential rotation.
func (h *NextcloudSourcePairingHandler) RetryRotation(c *gin.Context) {
	rotation, ok := h.authorizedRotation(c)
	if !ok {
		return
	}
	if rotation.State == "finalized" {
		c.JSON(http.StatusOK, gin.H{"rotation": rotation})
		return
	}
	config, err := repository.SourceRotationConfig(rotation)
	if err != nil || config == nil {
		c.JSON(http.StatusConflict, gin.H{"error": "source_rotation_credential_unavailable"})
		return
	}
	identity, err := h.inspect(c.Request.Context(), config)
	if err != nil || identity.InstanceID != rotation.InstanceID ||
		identity.BindingID != rotation.BindingID || identity.PublicationState != "active" {
		c.JSON(http.StatusConflict, gin.H{"error": "nextcloud_paired_binding_unavailable"})
		return
	}
	h.continueRotation(c, rotation, config, false)
}

// AbortRotation is a signed remote compare-and-swap: only an uncommitted rotation can
// be discarded. The old source credential signs the request and stays active.
func (h *NextcloudSourcePairingHandler) AbortRotation(c *gin.Context) {
	rotation, ok := h.authorizedRotation(c)
	if !ok {
		return
	}
	if rotation.State == "aborted" {
		c.JSON(http.StatusOK, gin.H{"rotation": rotation})
		return
	}
	if rotation.State != "pending" {
		c.JSON(http.StatusConflict, gin.H{"error": "committed_rotation_cannot_be_aborted"})
		return
	}
	_, ds, err := h.repo.SourcePairing(c.Request.Context(), rotation.TenantID, rotation.PairOperationID)
	if err != nil || ds == nil {
		c.JSON(http.StatusConflict, gin.H{"error": "source_rotation_conflict"})
		return
	}
	oldConfig, err := ds.ParseConfig()
	if err != nil || oldConfig == nil || oldConfig.Credentials["key_id"] != rotation.OldKeyID {
		c.JSON(http.StatusConflict, gin.H{"error": "source_rotation_credential_unavailable"})
		return
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), 20*time.Second)
	defer cancel()
	tuple := nextcloud.SourceRotation{
		OperationID:     rotation.OperationID,
		PairOperationID: rotation.PairOperationID, InstanceID: rotation.InstanceID,
		BindingID: rotation.BindingID, TenantID: rotation.TenantID,
		KnowledgeBaseID: rotation.KnowledgeBaseID, DataSourceID: rotation.DataSourceID,
	}
	if err := h.rotateAbort(ctx, oldConfig, tuple); err != nil {
		h.rotationFailure(c, rotation, err, "remote_abort")
		return
	}
	if err := h.repo.AbortSourceRotation(c.Request.Context(), rotation); err != nil {
		_ = h.repo.RecordSourceRotationFailure(c.Request.Context(), rotation, "local_abort_pending")
		rotation.LastErrorCode = "local_abort_pending"
		c.JSON(http.StatusAccepted, gin.H{"rotation": rotation, "error": "local_abort_pending"})
		return
	}
	rotation.State = "aborted"
	rotation.LastErrorCode = ""
	c.JSON(http.StatusOK, gin.H{"rotation": rotation})
}

func (h *NextcloudSourcePairingHandler) continueRotation(c *gin.Context,
	rotation repository.NextcloudSourceRotation, config *types.DataSourceConfig, created bool,
) {
	ctx, cancel := context.WithTimeout(c.Request.Context(), 20*time.Second)
	defer cancel()
	tuple := nextcloud.SourceRotation{
		OperationID:     rotation.OperationID,
		PairOperationID: rotation.PairOperationID, InstanceID: rotation.InstanceID,
		BindingID: rotation.BindingID, TenantID: rotation.TenantID,
		KnowledgeBaseID: rotation.KnowledgeBaseID, DataSourceID: rotation.DataSourceID,
	}
	if rotation.State == "pending" {
		if err := h.rotateCommit(ctx, config, tuple); err != nil {
			h.rotationFailure(c, rotation, err, "remote_commit")
			return
		}
		if err := h.repo.SwitchSourceRotation(c.Request.Context(), rotation); err != nil {
			_ = h.repo.RecordSourceRotationFailure(c.Request.Context(), rotation, "local_switch_pending")
			rotation.LastErrorCode = "local_switch_pending"
			c.JSON(http.StatusAccepted, gin.H{"rotation": rotation, "error": "local_switch_pending"})
			return
		}
		rotation.State = "switched"
	}
	if rotation.State != "switched" {
		c.JSON(http.StatusConflict, gin.H{"error": "source_rotation_conflict"})
		return
	}
	if err := h.rotateFinalize(ctx, config, tuple); err != nil {
		h.rotationFailure(c, rotation, err, "remote_finalize")
		return
	}
	if err := h.repo.FinalizeSourceRotation(c.Request.Context(), rotation); err != nil {
		_ = h.repo.RecordSourceRotationFailure(c.Request.Context(), rotation, "local_finalize_pending")
		rotation.LastErrorCode = "local_finalize_pending"
		c.JSON(http.StatusAccepted, gin.H{"rotation": rotation, "error": "local_finalize_pending"})
		return
	}
	rotation.State = "finalized"
	rotation.LastErrorCode = ""
	if created {
		c.JSON(http.StatusCreated, gin.H{"rotation": rotation})
	} else {
		c.JSON(http.StatusOK, gin.H{"rotation": rotation})
	}
}

func (h *NextcloudSourcePairingHandler) rotationFailure(c *gin.Context,
	rotation repository.NextcloudSourceRotation, err error, stage string,
) {
	code := stage + "_outcome_uncertain"
	status := http.StatusAccepted
	var remote *nextcloud.SourcePairingRemoteError
	if errors.As(err, &remote) {
		switch remote.StatusCode {
		case http.StatusUnauthorized, http.StatusForbidden, http.StatusConflict:
			code = stage + "_conflict"
			status = http.StatusConflict
		case http.StatusLocked:
			code = "publication_stopped"
		}
	}
	if recordErr := h.repo.RecordSourceRotationFailure(c.Request.Context(), rotation, code); recordErr != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "source_rotation_status_unavailable"})
		return
	}
	rotation.LastErrorCode = code
	c.JSON(status, gin.H{"rotation": rotation, "error": code})
}
