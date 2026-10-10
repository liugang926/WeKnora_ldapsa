package handler

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/datasource/connector/nextcloud"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/gin-gonic/gin"
)

type NextcloudOnboardingHandler struct {
	store    *repository.NextcloudOnboardingStore
	pairing  *NextcloudSourcePairingHandler
	inbox    *repository.NextcloudEventInboxRepository
	readRoot func(context.Context, *types.DataSourceConfig, string, string) (nextcloud.RootMigration, error)
	ackRoot  func(context.Context, *types.DataSourceConfig, nextcloud.RootMigration) (nextcloud.RootMigration, error)
}

func NewNextcloudOnboardingHandler(store *repository.NextcloudOnboardingStore, pairing *NextcloudSourcePairingHandler, inbox *repository.NextcloudEventInboxRepository) *NextcloudOnboardingHandler {
	return &NextcloudOnboardingHandler{store: store, pairing: pairing, inbox: inbox, readRoot: nextcloud.ReadRootMigration, ackRoot: nextcloud.AcknowledgeRootMigration}
}
func (h *NextcloudOnboardingHandler) credential(c *gin.Context) (types.NextcloudOnboardingCredential, bool) {
	c.Header("Cache-Control", "no-store")
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, 1<<20)
	auth := c.GetHeader("Authorization")
	if !strings.HasPrefix(auth, "Bearer ") {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "onboarding_credential_required"})
		return types.NextcloudOnboardingCredential{}, false
	}
	cred, err := h.store.Credential(c.Request.Context(), strings.TrimPrefix(auth, "Bearer "))
	if err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "onboarding_credential_required"})
		return cred, false
	}
	return cred, true
}
func onboardingError(c *gin.Context, err error) {
	status := http.StatusServiceUnavailable
	if errors.Is(err, repository.ErrNextcloudOnboardingDenied) {
		status = http.StatusForbidden
	}
	if errors.Is(err, repository.ErrNextcloudOnboardingNotFound) {
		status = http.StatusNotFound
	}
	if errors.Is(err, repository.ErrNextcloudOnboardingConflict) || errors.Is(err, repository.ErrNextcloudKBBlankUnverified) {
		status = http.StatusConflict
	}
	code := "onboarding_unavailable"
	if errors.Is(err, repository.ErrNextcloudOnboardingDenied) {
		code = "nextcloud_onboarding_denied"
	}
	if errors.Is(err, repository.ErrNextcloudOnboardingNotFound) {
		code = "nextcloud_onboarding_not_found"
	}
	if errors.Is(err, repository.ErrNextcloudOnboardingConflict) {
		code = "nextcloud_onboarding_conflict"
	}
	if errors.Is(err, repository.ErrNextcloudKBBlankUnverified) {
		code = "nextcloud_kb_blank_unverified"
	}
	c.JSON(status, gin.H{"error": code})
}
func (h *NextcloudOnboardingHandler) Candidates(c *gin.Context) {
	cred, ok := h.credential(c)
	if !ok {
		return
	}
	tenant, err := strconv.ParseUint(c.Query("tenant_id"), 10, 64)
	if err != nil || tenant != cred.TenantID || (c.Query("processing_template_id") != "" && c.Query("processing_template_id") != cred.TemplateKBID) {
		onboardingError(c, repository.ErrNextcloudOnboardingDenied)
		return
	}
	rows, err := h.store.Candidates(c.Request.Context(), cred, c.Query("actor_directory_id"), c.Query("actor_object_guid"))
	if err != nil {
		onboardingError(c, err)
		return
	}
	descriptor, err := h.store.TemplateDescriptor(c.Request.Context(), cred)
	if err != nil {
		onboardingError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"candidates": rows, "processing_template_id": cred.TemplateKBID, "processing_template_name": descriptor["name"], "graph_mode": descriptor["graph_mode"], "tenant_id": cred.TenantID})
}
func (h *NextcloudOnboardingHandler) Create(c *gin.Context) {
	cred, ok := h.credential(c)
	if !ok {
		return
	}
	var req types.NextcloudOnboardingRequest
	if c.ShouldBindJSON(&req) != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid_onboarding_request"})
		return
	}
	if req.ProcessingTemplateID == "" {
		req.ProcessingTemplateID = cred.TemplateKBID
	}
	op, err := h.store.Reserve(c.Request.Context(), cred, req)
	if err != nil {
		onboardingError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"operation": op})
}
func (h *NextcloudOnboardingHandler) operation(c *gin.Context) (types.NextcloudOnboardingCredential, types.NextcloudOnboardingOperation, bool) {
	cred, ok := h.credential(c)
	if !ok {
		return cred, types.NextcloudOnboardingOperation{}, false
	}
	op, err := h.store.Operation(c.Request.Context(), cred, c.Param("operation_id"))
	if err != nil {
		onboardingError(c, err)
		return cred, op, false
	}
	// A new delegated manager can manage a source originally created by an
	// actor who has since left. The trusted NC server supplies its current
	// session actor; grant reconciliation itself uses the scoped credential.
	actorDir, actorGUID := c.Query("actor_directory_id"), c.Query("actor_object_guid")
	if c.Request.Body != nil && c.Request.Method != http.MethodGet {
		b, readErr := io.ReadAll(c.Request.Body)
		if readErr != nil {
			onboardingError(c, repository.ErrNextcloudOnboardingDenied)
			return cred, op, false
		}
		c.Request.Body = io.NopCloser(bytes.NewReader(b))
		var actor struct {
			DirectoryID string `json:"actor_directory_id"`
			ObjectGUID  string `json:"actor_object_guid"`
		}
		_ = json.Unmarshal(b, &actor)
		if actor.DirectoryID != "" || actor.ObjectGUID != "" {
			actorDir, actorGUID = actor.DirectoryID, actor.ObjectGUID
		}
	}
	if actorDir == "" && actorGUID == "" {
		actorDir, actorGUID = op.ActorDirectoryID, op.ActorObjectGUID
	}
	if !strings.HasSuffix(c.FullPath(), "/grants") {
		if _, err = h.store.Actor(c.Request.Context(), cred, actorDir, actorGUID); err != nil {
			onboardingError(c, err)
			return cred, op, false
		}
	}
	return cred, op, true
}
func (h *NextcloudOnboardingHandler) Status(c *gin.Context) {
	_, op, ok := h.operation(c)
	if !ok {
		return
	}
	result := gin.H{"operation": op}
	if enabled, err := h.store.SyncEnabled(c.Request.Context(), op); err == nil {
		result["sync_enabled"] = enabled
	}
	if pair, _, err := h.pairing.repo.SourcePairing(c.Request.Context(), op.TenantID, op.OperationID); err == nil {
		result["pairing"] = pair
	}
	if credential, err := h.store.EventCredential(c.Request.Context(), op); err == nil {
		result["event_connection"] = gin.H{"connection_id": credential.ConnectionID, "key_id": credential.KeyID, "secret": credential.Secret, "receiver_url": nextcloudEventPath}
	}
	c.JSON(http.StatusOK, result)
}
func (h *NextcloudOnboardingHandler) Pair(c *gin.Context) {
	cred, op, ok := h.operation(c)
	if !ok {
		return
	}
	var req sourcePairingRequest
	if c.ShouldBindJSON(&req) != nil || req.OperationID != op.OperationID || req.InstanceID != op.InstanceID || req.BindingID != op.BindingID || req.KnowledgeBaseID != op.KnowledgeBaseID || req.BaseURL != cred.BaseURL || req.PublicationEpoch == nil || *req.PublicationEpoch < 0 || !sourcePairKeyPattern.MatchString(req.KeyID) || req.KeyID != "pair_"+strings.ReplaceAll(op.OperationID, "-", "") {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid_source_pairing_request"})
		return
	}
	config := &types.DataSourceConfig{Type: types.ConnectorTypeNextcloud, Credentials: map[string]interface{}{"token": req.Token, "key_id": req.KeyID}, ResourceIDs: []string{req.BindingID}, Settings: map[string]interface{}{"base_url": req.BaseURL}}
	identity, err := h.pairing.inspect(c.Request.Context(), config)
	currentPair, _, pairErr := h.pairing.repo.SourcePairing(c.Request.Context(), op.TenantID, op.OperationID)
	activeRetry := pairErr == nil && currentPair.State == "active" && currentPair.PublicationEpoch == *req.PublicationEpoch
	if err != nil || identity.InstanceID != op.InstanceID || identity.BindingID != op.BindingID || identity.BaseURL != cred.BaseURL || identity.PublicationState != "active" || (identity.PublicationEpoch != *req.PublicationEpoch && !(activeRetry && identity.PublicationEpoch >= *req.PublicationEpoch)) {
		c.JSON(http.StatusConflict, gin.H{"error": "nextcloud_prepared_binding_unavailable"})
		return
	}
	pair, _, err := h.pairing.repo.PrepareSourcePairing(c.Request.Context(), repository.NextcloudSourcePairing{OperationID: op.OperationID, TenantID: op.TenantID, KnowledgeBaseID: op.KnowledgeBaseID, InstanceID: op.InstanceID, BindingID: op.BindingID, BaseURL: cred.BaseURL, PublicationEpoch: *req.PublicationEpoch, KeyID: req.KeyID}, req.Token)
	if err != nil {
		sourcePairingError(c, err)
		return
	}
	if pair.State != "active" {
		err = h.pairing.commit(c.Request.Context(), config, nextcloud.SourcePairingCommit{OperationID: pair.OperationID, InstanceID: pair.InstanceID, BindingID: pair.BindingID, TenantID: pair.TenantID, KnowledgeBaseID: pair.KnowledgeBaseID, DataSourceID: pair.DataSourceID})
		if err == nil {
			err = h.pairing.repo.ActivateSourcePairing(c.Request.Context(), pair)
		}
		if err != nil {
			_ = h.pairing.repo.RecordSourcePairingFailure(c.Request.Context(), pair, "remote_outcome_uncertain")
			c.JSON(http.StatusAccepted, gin.H{"operation": op, "pairing": pair, "error": "remote_outcome_uncertain"})
			return
		}
		pair.State = "active"
	}
	if err = h.store.MarkActive(c.Request.Context(), op); err != nil {
		onboardingError(c, err)
		return
	}
	op.State = "active"
	credential, err := h.store.EventCredential(c.Request.Context(), op)
	if err != nil {
		credential, err = h.inbox.Pair(c.Request.Context(), repository.NextcloudEventPairingIdentity{TenantID: op.TenantID, KnowledgeBaseID: op.KnowledgeBaseID, DatasourceID: pair.DataSourceID, NextcloudInstanceID: op.InstanceID, BindingID: op.BindingID, DatasourceBaseURL: pair.BaseURL, DatasourceConfigSHA: pair.ConfigSHA, PublicationEpoch: pair.PublicationEpoch, PublicationState: "active"})
		if errors.Is(err, repository.ErrNextcloudEventConnectionExists) {
			credential, err = h.store.EventCredential(c.Request.Context(), op)
		}
	}
	if err != nil {
		c.JSON(http.StatusAccepted, gin.H{"operation": op, "pairing": pair, "error": "event_connection_pending"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"operation": op, "pairing": pair, "event_connection": gin.H{"connection_id": credential.ConnectionID, "key_id": credential.KeyID, "secret": credential.Secret, "receiver_url": nextcloudEventPath}})
}
func (h *NextcloudOnboardingHandler) Grants(c *gin.Context) {
	_, op, ok := h.operation(c)
	if !ok {
		return
	}
	var req types.NextcloudGrantSnapshot
	if c.ShouldBindJSON(&req) != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid_grant_snapshot"})
		return
	}
	// Pairing receipts intentionally retain their initial epoch. Stop/resume and
	// root migration require a fresh signed binding authority before grants.
	pair, ds, err := h.pairing.repo.SourcePairing(c.Request.Context(), op.TenantID, op.OperationID)
	if err != nil || ds == nil || pair.State != "active" {
		onboardingError(c, repository.ErrNextcloudOnboardingConflict)
		return
	}
	cfg, err := ds.ParseConfig()
	if err != nil {
		onboardingError(c, repository.ErrNextcloudOnboardingConflict)
		return
	}
	identity, err := h.pairing.inspect(c.Request.Context(), cfg)
	if err != nil || identity.PublicationState != "active" || identity.PublicationEpoch != req.PublicationEpoch || identity.InstanceID != op.InstanceID || identity.BindingID != op.BindingID {
		onboardingError(c, repository.ErrNextcloudOnboardingConflict)
		return
	}
	if err = h.store.RefreshGrantEpoch(c.Request.Context(), op); err != nil {
		onboardingError(c, repository.ErrNextcloudOnboardingConflict)
		return
	}
	if err := h.store.ApplyGrantSnapshot(c.Request.Context(), op, req); err != nil {
		onboardingError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"permission_version": req.PermissionVersion, "updated_at": time.Now().UTC()})
}
func (h *NextcloudOnboardingHandler) Sync(c *gin.Context) {
	_, op, ok := h.operation(c)
	if !ok {
		return
	}
	var req struct {
		SyncEnabled      *bool  `json:"sync_enabled"`
		ExpectedRevision uint64 `json:"expected_revision"`
	}
	if c.ShouldBindJSON(&req) != nil || req.SyncEnabled == nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid_sync_request"})
		return
	}
	out, err := h.store.SetSyncEnabled(c.Request.Context(), op, *req.SyncEnabled, req.ExpectedRevision)
	if err != nil {
		onboardingError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"operation": out, "sync_enabled": *req.SyncEnabled})
}

func (h *NextcloudOnboardingHandler) RootMigration(c *gin.Context) {
	_, op, ok := h.operation(c)
	if !ok {
		return
	}
	var req struct {
		OperationID string `json:"operation_id"`
	}
	if c.ShouldBindJSON(&req) != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid_root_migration"})
		return
	}
	pair, ds, err := h.pairing.repo.SourcePairing(c.Request.Context(), op.TenantID, op.OperationID)
	if err != nil || ds == nil || pair.State != "active" {
		onboardingError(c, repository.ErrNextcloudOnboardingConflict)
		return
	}
	cfg, err := ds.ParseConfig()
	if err != nil {
		onboardingError(c, repository.ErrNextcloudOnboardingConflict)
		return
	}
	proof, err := h.readRoot(c.Request.Context(), cfg, op.BindingID, req.OperationID)
	if err != nil {
		onboardingError(c, repository.ErrNextcloudOnboardingConflict)
		return
	}
	row, err := h.store.PrepareRootMigration(c.Request.Context(), op, proof)
	if err != nil {
		onboardingError(c, err)
		return
	}
	if row.State == "finalized" {
		c.JSON(http.StatusOK, gin.H{"migration": proof})
		return
	}
	ack, err := h.ackRoot(c.Request.Context(), cfg, proof)
	if err != nil {
		c.JSON(http.StatusAccepted, gin.H{"migration": proof, "error": "root_migration_ack_unknown"})
		return
	}
	if err = h.store.FinalizeRootMigration(c.Request.Context(), op, row, ack); err != nil {
		c.JSON(http.StatusAccepted, gin.H{"migration": ack, "error": "root_migration_local_finalize_pending"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"migration": ack})
}

func (h *NextcloudOnboardingHandler) TargetMigration(c *gin.Context) {
	_, op, ok := h.operation(c)
	if !ok {
		return
	}
	var req struct {
		OperationID    string `json:"operation_id"`
		NewOperationID string `json:"new_operation_id"`
		ActorDir       string `json:"actor_directory_id"`
		ActorGUID      string `json:"actor_object_guid"`
	}
	if c.ShouldBindJSON(&req) != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid_target_migration"})
		return
	}
	if req.ActorDir == "" {
		req.ActorDir, req.ActorGUID = op.ActorDirectoryID, op.ActorObjectGUID
	}
	// The immutable retirement record permits ACK-loss retries even after NC
	// removed the old binding. New handoffs require the signed stopped source.
	if op.State == "retired" {
		row, err := h.store.PrepareTargetMigration(c.Request.Context(), op, req.OperationID, req.NewOperationID, req.ActorDir, req.ActorGUID)
		if err != nil {
			onboardingError(c, err)
			return
		}
		c.JSON(http.StatusOK, gin.H{"migration": row})
		return
	}
	pair, ds, err := h.pairing.repo.SourcePairing(c.Request.Context(), op.TenantID, op.OperationID)
	if err != nil || ds == nil || pair.State != "active" {
		onboardingError(c, repository.ErrNextcloudOnboardingConflict)
		return
	}
	cfg, err := ds.ParseConfig()
	if err != nil {
		onboardingError(c, repository.ErrNextcloudOnboardingConflict)
		return
	}
	identity, err := h.pairing.inspect(c.Request.Context(), cfg)
	if err != nil || identity.PublicationState != "stopped" || identity.InstanceID != op.InstanceID || identity.BindingID != op.BindingID {
		onboardingError(c, repository.ErrNextcloudOnboardingConflict)
		return
	}
	if req.ActorDir == "" {
		req.ActorDir = op.ActorDirectoryID
		req.ActorGUID = op.ActorObjectGUID
	}
	row, err := h.store.PrepareTargetMigration(c.Request.Context(), op, req.OperationID, req.NewOperationID, req.ActorDir, req.ActorGUID)
	if err != nil {
		onboardingError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"migration": row})
}
func (h *NextcloudOnboardingHandler) FinalizeTargetMigration(c *gin.Context) {
	_, op, ok := h.operation(c)
	if !ok {
		return
	}
	row, ds, err := h.store.TargetMigrationSource(c.Request.Context(), op, c.Param("migration_id"))
	if err != nil {
		onboardingError(c, err)
		return
	}
	cfg, err := ds.ParseConfig()
	if err != nil {
		onboardingError(c, repository.ErrNextcloudOnboardingConflict)
		return
	}
	multimodal, err := h.store.TargetMigrationMultimodal(c.Request.Context(), row)
	if err != nil {
		onboardingError(c, err)
		return
	}
	manifest, err := nextcloud.InspectImportableRecoveryManifest(c.Request.Context(), cfg, multimodal)
	if err != nil {
		c.JSON(http.StatusAccepted, gin.H{"migration": row, "error": "target_manifest_unverified"})
		return
	}
	finalized, err := h.store.FinalizeTargetMigration(c.Request.Context(), op, row.OperationID, manifest)
	if err != nil {
		c.JSON(http.StatusAccepted, gin.H{"migration": row, "error": "target_build_pending"})
		return
	}
	if len(manifest.Files) > 0 {
		if err = h.pairing.dataSource.PublishTargetMigrationCandidates(c.Request.Context(), finalized); err != nil {
			c.JSON(http.StatusAccepted, gin.H{"migration": finalized, "error": "target_publication_pending"})
			return
		}
	}
	c.JSON(http.StatusOK, gin.H{"migration": finalized})
}
