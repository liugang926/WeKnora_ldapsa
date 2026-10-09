package handler

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/datasource/connector/nextcloud"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

type sourceDecommissionRequest struct {
	OperationID string `json:"operation_id" binding:"required"`
}

// Decommission is deliberately limited to a newly paired source with a
// permanent never-touched proof. A source with any sync or event history stays
// paused in Nextcloud and returns 409 until full inventory GC is implemented.
func (h *NextcloudSourcePairingHandler) Decommission(c *gin.Context) {
	pair, ds, ok := h.authorizedPair(c)
	if !ok {
		return
	}
	if pair.State != "active" || ds == nil {
		c.JSON(http.StatusConflict, gin.H{"error": "active_source_pairing_required"})
		return
	}
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, 16<<10)
	var req sourceDecommissionRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid_decommission_request"})
		return
	}
	operationID, err := uuid.Parse(req.OperationID)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid_decommission_request"})
		return
	}
	req.OperationID = operationID.String()
	config, err := ds.ParseConfig()
	if err != nil || config == nil || config.Credentials["key_id"] != pair.KeyID {
		c.JSON(http.StatusConflict, gin.H{"error": "paired_source_credential_unavailable"})
		return
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), 20*time.Second)
	defer cancel()
	intent, err := h.readDecommission(ctx, config, pair.BindingID, req.OperationID)
	if err != nil {
		decommissionRemoteFailure(c, err, "remote_intent")
		return
	}
	if intent.PairOperationID != pair.OperationID || intent.InstanceID != pair.InstanceID ||
		intent.BindingID != pair.BindingID || intent.TenantID != nextcloud.DecommissionTenantID(pair.TenantID) ||
		intent.KnowledgeBaseID != pair.KnowledgeBaseID || intent.DataSourceID != pair.DataSourceID ||
		intent.KeyID != pair.KeyID || intent.PublicationEpoch < 0 {
		c.JSON(http.StatusConflict, gin.H{"error": "decommission_tuple_conflict"})
		return
	}
	local, err := h.repo.BeginEmptySourceDecommission(c.Request.Context(), pair,
		repository.NextcloudSourceDecommission{
			OperationID: req.OperationID, PairOperationID: pair.OperationID,
			TenantID: pair.TenantID, KnowledgeBaseID: pair.KnowledgeBaseID,
			DataSourceID: pair.DataSourceID, InstanceID: pair.InstanceID,
			BindingID: pair.BindingID, KeyID: pair.KeyID,
			PublicationEpoch: intent.PublicationEpoch,
		})
	if err != nil {
		sourcePairingError(c, err)
		return
	}
	if local.State == "acknowledged" {
		c.JSON(http.StatusOK, gin.H{"decommission": local})
		return
	}
	if local.State != "prepared" {
		c.JSON(http.StatusConflict, gin.H{"error": "decommission_state_conflict"})
		return
	}
	if err := h.ackDecommission(ctx, config, intent); err != nil {
		decommissionRemoteFailure(c, err, "remote_ack")
		return
	}
	if err := h.repo.RecordEmptyDecommissionAck(c.Request.Context(), local); err != nil {
		c.JSON(http.StatusAccepted, gin.H{"decommission": local, "error": "local_ack_pending"})
		return
	}
	local.State = "acknowledged"
	local.InventorySHA256 = repository.EmptyNextcloudInventorySHA256
	c.JSON(http.StatusOK, gin.H{"decommission": local})
}

// DecommissionStatus returns tenant-scoped source-decommission status.
func (h *NextcloudSourcePairingHandler) DecommissionStatus(c *gin.Context) {
	pair, _, ok := h.authorizedPair(c)
	if !ok {
		return
	}
	row, err := h.repo.SourceDecommission(c.Request.Context(), pair)
	if err != nil {
		sourcePairingError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"decommission": row})
}

func decommissionRemoteFailure(c *gin.Context, err error, stage string) {
	var remote *nextcloud.SourcePairingRemoteError
	if errors.As(err, &remote) && (remote.StatusCode == http.StatusConflict ||
		remote.StatusCode == http.StatusUnauthorized || remote.StatusCode == http.StatusForbidden ||
		remote.StatusCode == http.StatusNotFound) {
		c.JSON(http.StatusConflict, gin.H{"error": stage + "_conflict"})
		return
	}
	c.JSON(http.StatusAccepted, gin.H{"error": stage + "_uncertain"})
}
