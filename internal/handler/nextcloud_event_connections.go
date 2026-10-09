package handler

import (
	"context"
	"errors"
	"net/http"

	"github.com/google/uuid"

	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/datasource/connector/nextcloud"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/gin-gonic/gin"
)

// NextcloudEventConnectionHandler is the administrator-facing provisioning
// boundary. The unauthenticated event receiver only accepts connections that
// have been committed through this boundary.
type NextcloudEventConnectionHandler struct {
	inbox      *repository.NextcloudEventInboxRepository
	dataSource *DataSourceHandler
	inspect    func(context.Context, *types.DataSourceConfig) (nextcloud.PairingIdentity, error)
}

// NewNextcloudEventConnectionHandler constructs the tenant-scoped event-connection handler.
func NewNextcloudEventConnectionHandler(
	inbox *repository.NextcloudEventInboxRepository,
	dataSource *DataSourceHandler,
) *NextcloudEventConnectionHandler {
	return &NextcloudEventConnectionHandler{
		inbox: inbox, dataSource: dataSource, inspect: nextcloud.InspectPairing,
	}
}

func (h *NextcloudEventConnectionHandler) authorize(c *gin.Context) (*types.DataSource, bool) {
	c.Header("Cache-Control", "no-store")
	if _, isAPIKey := types.TenantAPIKeyScopeFromContext(c.Request.Context()); isAPIKey {
		c.JSON(http.StatusForbidden, gin.H{"error": "admin_session_required"})
		return nil, false
	}
	// The rollout RBAC guard may be permissive on older tenants. Issuing an
	// HMAC secret must still require an authenticated Admin+ context.
	if !types.TenantRoleFromContext(c.Request.Context()).HasPermission(types.TenantRoleAdmin) {
		c.JSON(http.StatusForbidden, gin.H{"error": "admin_required"})
		return nil, false
	}
	tenantID := h.dataSource.getTenantID(c)
	if tenantID == 0 {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "unauthorized"})
		return nil, false
	}
	ds, status, message := h.dataSource.getOwnedDataSource(
		c.Request.Context(), tenantID, c.Param("id"), types.ResourceActionEdit)
	if status != http.StatusOK || ds == nil {
		c.JSON(status, gin.H{"error": message})
		return nil, false
	}
	if ds.Type != types.ConnectorTypeNextcloud {
		c.JSON(http.StatusConflict, gin.H{"error": "nextcloud_source_required"})
		return nil, false
	}
	return ds, true
}

func (h *NextcloudEventConnectionHandler) inspectCommitted(
	c *gin.Context, ds *types.DataSource,
) (repository.NextcloudEventPairingIdentity, bool) {
	baseURL, hash, bindingID, err := repository.NextcloudEventDataSourceIdentity(ds.Config)
	if err != nil {
		c.JSON(http.StatusConflict, gin.H{"error": "source_configuration_unavailable"})
		return repository.NextcloudEventPairingIdentity{}, false
	}
	config, err := ds.ParseConfig()
	if err != nil || config == nil {
		c.JSON(http.StatusConflict, gin.H{"error": "source_configuration_unavailable"})
		return repository.NextcloudEventPairingIdentity{}, false
	}
	inspected, err := h.inspect(c.Request.Context(), config)
	if err != nil {
		c.JSON(http.StatusConflict, gin.H{"error": "nextcloud_binding_unavailable"})
		return repository.NextcloudEventPairingIdentity{}, false
	}
	if inspected.BindingID != bindingID || inspected.BaseURL != baseURL ||
		!validNextcloudOpaqueID(inspected.InstanceID, 1, 128) {
		c.JSON(http.StatusConflict, gin.H{"error": "nextcloud_binding_mismatch"})
		return repository.NextcloudEventPairingIdentity{}, false
	}
	return repository.NextcloudEventPairingIdentity{
		TenantID: ds.TenantID, KnowledgeBaseID: ds.KnowledgeBaseID,
		DatasourceID: ds.ID, NextcloudInstanceID: inspected.InstanceID,
		BindingID: bindingID, DatasourceBaseURL: baseURL,
		DatasourceConfigSHA: hash, PublicationEpoch: inspected.PublicationEpoch,
		PublicationState: inspected.PublicationState,
	}, true
}

// Status lets an administrator recover from a lost one-time response: the
// secret cannot be read back, so a lost secret must be rotated.
func (h *NextcloudEventConnectionHandler) Status(c *gin.Context) {
	ds, ok := h.authorize(c)
	if !ok {
		return
	}
	status, err := h.inbox.ConnectionStatus(c.Request.Context(), ds.TenantID, ds.ID)
	if err != nil {
		nextcloudConnectionError(c, err)
		return
	}
	// An active row may have become unusable after an endpoint or credential
	// edit. Surface that state without revealing or reissuing its secret.
	if status.Status == "active" {
		baseURL, configSHA, bindingID, identityErr := repository.NextcloudEventDataSourceIdentity(ds.Config)
		if identityErr != nil || bindingID != status.BindingID ||
			baseURL != status.DatasourceBaseURL || configSHA != status.DatasourceConfigSHA {
			status.Status = "source_changed"
		}
	}
	c.JSON(http.StatusOK, gin.H{
		"connection_id":               status.ConnectionID,
		"nextcloud_instance_id":       status.NextcloudInstanceID,
		"binding_id":                  status.BindingID,
		"key_id":                      status.KeyID,
		"status":                      status.Status,
		"received_through_event_id":   status.ReceivedThroughEventID,
		"dispatched_through_event_id": status.DispatchedThroughEventID,
		"applied_through_event_id":    status.AppliedThroughEventID,
		"backlog_count":               status.BacklogCount,
		"undispatched_count":          status.UndispatchedCount,
		"dispatch_state":              status.DispatchState,
		"last_error_code":             status.LastErrorCode,
		"last_sync_log_id":            status.LastSyncLogID,
		"receiver_url":                nextcloudEventPath,
	})
}

func nextcloudConnectionError(c *gin.Context, err error) {
	switch {
	case errors.Is(err, repository.ErrNextcloudEventConnectionExists):
		c.JSON(http.StatusConflict, gin.H{"error": "connection_already_active"})
	case errors.Is(err, repository.ErrNextcloudEventConnectionMissing):
		c.JSON(http.StatusNotFound, gin.H{"error": "connection_not_active"})
	case errors.Is(err, repository.ErrNextcloudEventRebindUnsafe):
		c.JSON(http.StatusConflict, gin.H{"error": "event_dispatch_manual_review_required"})
	case errors.Is(err, repository.ErrNextcloudEventScope):
		c.JSON(http.StatusConflict, gin.H{"error": "source_changed_repair_required"})
	default:
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "connection_unavailable"})
	}
}

func nextcloudConnectionIssued(c *gin.Context, status int, credential repository.NextcloudEventConnectionCredential) {
	c.JSON(status, gin.H{
		"connection_id":         credential.ConnectionID,
		"nextcloud_instance_id": credential.NextcloudInstanceID,
		"binding_id":            credential.BindingID,
		"key_id":                credential.KeyID,
		"secret":                credential.Secret,
		// An absolute path is stable behind the deployment's reverse proxy.
		// The Nextcloud administrator resolves it against WeKnora's public
		// origin; an untrusted Host/X-Forwarded-Host is never reflected here.
		"receiver_url": nextcloudEventPath,
		"status":       "active",
	})
}

// Pair issues a one-time random secret for the verified live binding.
func (h *NextcloudEventConnectionHandler) Pair(c *gin.Context) {
	ds, ok := h.authorize(c)
	if !ok {
		return
	}
	identity, ok := h.inspectCommitted(c, ds)
	if !ok {
		return
	}
	credential, err := h.inbox.Pair(c.Request.Context(), identity)
	if err != nil {
		nextcloudConnectionError(c, err)
		return
	}
	nextcloudConnectionIssued(c, http.StatusCreated, credential)
}

// Rotate invalidates the old signing key at transaction commit.
func (h *NextcloudEventConnectionHandler) Rotate(c *gin.Context) {
	ds, ok := h.authorize(c)
	if !ok {
		return
	}
	identity, ok := h.inspectCommitted(c, ds)
	if !ok {
		return
	}
	credential, err := h.inbox.Rotate(c.Request.Context(), identity)
	if err != nil {
		nextcloudConnectionError(c, err)
		return
	}
	nextcloudConnectionIssued(c, http.StatusOK, credential)
}

// Rebind accepts only the exact finalized source machine-key rotation. It
// never returns or changes the event HMAC secret, connection ID, or watermark.
func (h *NextcloudEventConnectionHandler) Rebind(c *gin.Context) {
	ds, ok := h.authorize(c)
	if !ok {
		return
	}
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, 2<<10)
	var request struct {
		OperationID string `json:"operation_id" binding:"required"`
	}
	if err := c.ShouldBindJSON(&request); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid_source_rotation_request"})
		return
	}
	parsed, err := uuid.Parse(request.OperationID)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid_source_rotation_request"})
		return
	}
	identity, ok := h.inspectCommitted(c, ds)
	if !ok {
		return
	}
	if identity.PublicationState != "active" || identity.PublicationEpoch < 0 {
		c.JSON(http.StatusConflict, gin.H{"error": "nextcloud_paired_binding_unavailable"})
		return
	}
	changed, err := h.inbox.RebindFinalizedSourceRotation(c.Request.Context(), identity, parsed.String())
	if err != nil {
		nextcloudConnectionError(c, err)
		return
	}
	status, err := h.inbox.ConnectionStatus(c.Request.Context(), ds.TenantID, ds.ID)
	if err != nil {
		nextcloudConnectionError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"connection_id": status.ConnectionID,
		"rotation_id":   parsed.String(), "rebound": changed,
		"status":                      status.Status,
		"key_id":                      status.KeyID,
		"received_through_event_id":   status.ReceivedThroughEventID,
		"dispatched_through_event_id": status.DispatchedThroughEventID,
		"applied_through_event_id":    status.AppliedThroughEventID,
		"dispatch_state":              status.DispatchState,
		"last_error_code":             status.LastErrorCode,
	})
}

// Revoke requires no live Nextcloud read, so it works after credential drift.
func (h *NextcloudEventConnectionHandler) Revoke(c *gin.Context) {
	ds, ok := h.authorize(c)
	if !ok {
		return
	}
	if err := h.inbox.Revoke(c.Request.Context(), ds.TenantID, ds.ID); err != nil {
		nextcloudConnectionError(c, err)
		return
	}
	c.Status(http.StatusNoContent)
}
