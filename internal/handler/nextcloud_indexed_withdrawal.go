package handler

import (
	"context"
	"net/http"
	"time"

	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/datasource/connector/nextcloud"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

// BeginIndexedWithdrawal is the deliberately non-final first stage for a
// source with indexing history. The source stays paired, paused and credentialed;
// Nextcloud receives no ACK and must keep its binding stopped.
func (h *NextcloudSourcePairingHandler) BeginIndexedWithdrawal(c *gin.Context) {
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
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid_withdrawal_request"})
		return
	}
	operationID, err := uuid.Parse(req.OperationID)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid_withdrawal_request"})
		return
	}
	config, err := ds.ParseConfig()
	if err != nil || config == nil || config.Credentials["key_id"] != pair.KeyID {
		c.JSON(http.StatusConflict, gin.H{"error": "paired_source_credential_unavailable"})
		return
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), 20*time.Second)
	defer cancel()
	intent, err := h.readDecommission(ctx, config, pair.BindingID, operationID.String())
	if err != nil {
		decommissionRemoteFailure(c, err, "remote_intent")
		return
	}
	if intent.State != "prepared" || intent.PairOperationID != pair.OperationID ||
		intent.InstanceID != pair.InstanceID ||
		intent.BindingID != pair.BindingID || intent.TenantID != nextcloud.DecommissionTenantID(pair.TenantID) ||
		intent.KnowledgeBaseID != pair.KnowledgeBaseID || intent.DataSourceID != pair.DataSourceID ||
		intent.KeyID != pair.KeyID || intent.PublicationEpoch < 0 {
		c.JSON(http.StatusConflict, gin.H{"error": "withdrawal_tuple_conflict"})
		return
	}
	row, err := h.repo.BeginIndexedSourceWithdrawal(c.Request.Context(), pair,
		repository.NextcloudIndexedWithdrawal{
			OperationID: operationID.String(), PairOperationID: pair.OperationID,
			TenantID: pair.TenantID, KnowledgeBaseID: pair.KnowledgeBaseID,
			DataSourceID: pair.DataSourceID, InstanceID: pair.InstanceID,
			BindingID: pair.BindingID, KeyID: pair.KeyID,
			PublicationEpoch: intent.PublicationEpoch,
		})
	if err != nil {
		sourcePairingError(c, err)
		return
	}
	status, err := h.repo.RefreshIndexedWithdrawalInventory(c.Request.Context(), row)
	if err != nil {
		c.JSON(http.StatusAccepted, gin.H{"withdrawal": repository.NextcloudIndexedWithdrawalStatus{
			NextcloudIndexedWithdrawal: row, LogicalWithdrawn: true,
		}, "error": "inventory_retry_required"})
		return
	}
	c.JSON(http.StatusAccepted, gin.H{"withdrawal": status})
}

// IndexedWithdrawalStatus returns tenant-scoped indexed-withdrawal status.
func (h *NextcloudSourcePairingHandler) IndexedWithdrawalStatus(c *gin.Context) {
	pair, _, ok := h.authorizedPair(c)
	if !ok {
		return
	}
	status, err := h.repo.IndexedWithdrawalStatus(c.Request.Context(), pair)
	if err != nil {
		sourcePairingError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"withdrawal": status})
}
