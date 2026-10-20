package repository

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

const (
	autoTagCompletionOp       = "auto_tag_completion"
	autoTagCompletionDone     = "auto_tag_done"
	autoTagCompletionRejected = "auto_tag_rejected"
	autoTagCompletionRunning  = "auto_tag_running"
)

// SealKnowledgeAutoTagCompletionInTx is only called by the completion winner.
// The exact final material, candidates, source controls, model/tenant revisions,
// queue receipt and intent commit together. Recovery never captures new input.
func (s *KnowledgeInputProvenanceStore) SealKnowledgeAutoTagCompletionInTx(
	ctx context.Context,
	tx *gorm.DB,
	id string,
	payload types.KnowledgeAutoTagPayload,
) error {
	var k types.Knowledge
	if err := tx.Where("id = ?", id).Take(&k).Error; err != nil {
		return err
	}
	var kb types.KnowledgeBase
	if err := tx.Where("id = ? AND tenant_id = ?", k.KnowledgeBaseID, k.TenantID).Take(&kb).Error; err != nil {
		return err
	}
	if kb.Type != types.KnowledgeBaseTypeDocument || kb.AutoTagConfig == nil || !kb.AutoTagConfig.Enabled {
		return nil
	}
	original := KnowledgeBuildInputProofFromContext(ctx)
	if original == nil || original.KnowledgeID != id || original.TenantID != k.TenantID ||
		original.KnowledgeBaseID != kb.ID {
		return types.ErrSourceLineageUnknown
	}
	root := cloneKnowledgeInputProof(original)
	root.BodyDigest, root.Projection = "", ""
	root.ChunkIDs = nil
	var chunks []*types.Chunk
	if err := tx.Where(("tenant_id = ? AND knowledge_base_id = ? AND knowledge_id = ? AND chunk_typ" +
		"e IN ?"), k.TenantID, kb.ID, id, []types.ChunkType{
		types.ChunkTypeText,
		types.ChunkTypeImageOCR,
		types.ChunkTypeImageCaption,
	}).
		Find(&chunks).
		Error; err != nil {
		return err
	}
	if len(chunks) == 0 {
		return nil
	}
	ids := make([]string, 0, len(chunks))
	for _, chunk := range chunks {
		ids = append(ids, chunk.ID)
	}
	actual, err := s.captureKnowledgeBuildSnapshotInTx(
		tx,
		types.KnowledgeBuildInputCaptureRequest{
			TenantID:        k.TenantID,
			KnowledgeBaseID: kb.ID,
			KnowledgeID:     id,
			ChunkIDs:        ids,
			Expected:        root,
		},
		false,
	)
	if err != nil {
		return err
	}
	snapshot, err := s.captureKnowledgeAutoTagInTx(tx, actual.Proof)
	if err != nil {
		return err
	}
	if s.displayAuthorityFactory == nil {
		return types.ErrSourceLineageUnknown
	}
	authority := s.displayAuthorityFactory(tx)
	if authority == nil || authority.db != tx {
		return types.ErrSourceLineageUnknown
	}
	if _, err := authority.authorizeOriginalKnowledgeProducerWrite(ctx, &kb, actual.Proof); err != nil {
		return err
	}
	snapshot.ManifestID = uuid.NewString()
	snapshot.InputDigest, err = autoTagSnapshotDigest(snapshot)
	if err != nil {
		return err
	}
	data, err := json.Marshal(snapshot)
	if err != nil {
		return err
	}
	lineage, err := json.Marshal(snapshot.SourceLineage)
	if err != nil {
		return err
	}
	if err := tx.Create(
		&knowledgeAutoTagManifest{
			ManifestID:      snapshot.ManifestID,
			TenantID:        k.TenantID,
			KnowledgeBaseID: kb.ID,
			KnowledgeID:     id,
			AdmissionID:     actual.Proof.AdmissionID,
			InputDigest:     snapshot.InputDigest,
			ActualInputs:    string(data),
			SourceLineage:   string(lineage),
		},
	).Error; err != nil {
		return err
	}
	payload.TenantID, payload.KnowledgeBaseID, payload.KnowledgeID = k.TenantID, kb.ID, id
	payload.OriginalInput, payload.CompletionInput = actual.Proof, snapshot.ManifestID
	// Span state belongs to the same SQL connection; do not query a second
	// connection while the completion transaction owns a SQLite pool of one.
	if err := tx.Model(&types.KnowledgeProcessingSpan{}).
		Select("COALESCE(MAX(attempt), 0)").
		Where("knowledge_id = ?", id).
		Scan(&payload.Attempt).
		Error; err != nil {
		return err
	}
	envelope := autoTagActualTaskInput{Kind: "actual_queue_input", Payload: payload}
	digest, err := KnowledgeInputDigest(envelope)
	if err != nil {
		return err
	}
	data, err = json.Marshal(envelope)
	if err != nil {
		return err
	}
	queueID := uuid.NewString()
	if err := tx.Create(
		&knowledgeAutoTagManifest{
			ManifestID:      queueID,
			TenantID:        k.TenantID,
			KnowledgeBaseID: kb.ID,
			KnowledgeID:     id,
			AdmissionID:     actual.Proof.AdmissionID,
			InputDigest:     digest,
			ActualInputs:    string(data),
			SourceLineage:   string(lineage),
		},
	).Error; err != nil {
		return err
	}
	payload.TaskInput = &types.KnowledgeAutoTagTaskInputProof{ID: queueID, Digest: digest}
	data, err = json.Marshal(payload)
	if err != nil {
		return err
	}
	return tx.Create(
		&types.TaskPendingOp{
			TenantID:   k.TenantID,
			TaskType:   types.TypeKnowledgeAutoTag,
			Scope:      types.TaskScopeKnowledge,
			ScopeID:    id,
			Op:         autoTagCompletionOp,
			DedupKey:   queueID,
			Payload:    data,
			EnqueuedAt: time.Now().UTC(),
		},
	).Error
}

// LoadKnowledgeAutoTagCompletionInput loads the sealed snapshot and restores
// runtime-only tenant credentials only after checking its original digest.
func (s *KnowledgeInputProvenanceStore) LoadKnowledgeAutoTagCompletionInput(
	ctx context.Context,
	p types.KnowledgeAutoTagPayload,
) (*types.KnowledgeAutoTagInputSnapshot, error) {
	if p.CompletionInput == "" || p.OriginalInput == nil {
		return nil, types.ErrSourceLineageUnknown
	}
	var row knowledgeAutoTagManifest
	if err := s.db.WithContext(ctx).
		Where(
			("manifest_id = ? AND tenant_id = ? AND knowledge_base_id = ? AND knowledge_" +
				"id = ? AND admission_id = ?"),
			p.CompletionInput,
			p.TenantID,
			p.KnowledgeBaseID,
			p.KnowledgeID,
			p.OriginalInput.AdmissionID,
		).
		Take(&row).
		Error; err != nil {
		return nil, err
	}
	var snapshot types.KnowledgeAutoTagInputSnapshot
	if json.Unmarshal([]byte(row.ActualInputs), &snapshot) != nil || snapshot.ManifestID != row.ManifestID ||
		snapshot.InputDigest != row.InputDigest {
		return nil, fmt.Errorf("completion model envelope: %w", ErrKnowledgeInputChanged)
	}
	digest, err := autoTagSnapshotDigest(&snapshot)
	if err != nil || digest != row.InputDigest {
		return nil, fmt.Errorf("completion model digest: %w", ErrKnowledgeInputChanged)
	}
	err = s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if _, err := s.recheckKnowledgeAutoTagInTx(ctx, tx, &snapshot); err != nil {
			return err
		}
		_, tenant, err := knowledgeAutoTagModelInputs(tx, snapshot.Original.KnowledgeBase)
		snapshot.Tenant = tenant
		return err
	}, knowledgeDisplayWriteOptions(s.db))
	return &snapshot, err
}

// PendingKnowledgeAutoTagCompletions returns pending knowledge auto tag completions.
func (s *KnowledgeInputProvenanceStore) PendingKnowledgeAutoTagCompletions(
	ctx context.Context,
	knowledgeID string,
	limit int,
) ([]*types.TaskPendingOp, error) {
	var rows []*types.TaskPendingOp
	query := s.db.WithContext(ctx).
		Where(
			"task_type = ? AND scope = ? AND (op = ? OR (op = ? AND claimed_at < ?))",
			types.TypeKnowledgeAutoTag,
			types.TaskScopeKnowledge,
			autoTagCompletionOp,
			autoTagCompletionRunning,
			time.Now().UTC().Add(
				-3*time.Minute,
			),
		)
	if knowledgeID != "" {
		query = query.Where("scope_id = ?", knowledgeID)
	}
	err := query.Order("CASE WHEN claimed_at IS NULL THEN 0 ELSE 1 END ASC, claimed_at ASC, id ASC").
		Limit(limit).
		Find(&rows).
		Error
	return rows, err
}

// NoteKnowledgeAutoTagCompletionDelivery records knowledge auto tag completion delivery.
// Rotate delivered pending rows so an old blocked wrapper cannot starve later
// intents. For pending rows claimed_at is the last wake-up attempt; consuming
// changes op and replaces it with the exact consumer claim timestamp.
func (s *KnowledgeInputProvenanceStore) NoteKnowledgeAutoTagCompletionDelivery(ctx context.Context, id int64) error {
	return s.db.WithContext(ctx).
		Model(&types.TaskPendingOp{}).
		Where("id = ? AND task_type = ? AND op = ?", id, types.TypeKnowledgeAutoTag, autoTagCompletionOp).
		Update("claimed_at", time.Now().UTC()).
		Error
}

// ClaimKnowledgeAutoTagCompletion claims knowledge auto tag completion.
// Claim serializes duplicate deliveries. A crashed consumer is reclaimable;
// terminal rows stay in SQL and cannot be awakened by startup or redelivery.
func (s *KnowledgeInputProvenanceStore) ClaimKnowledgeAutoTagCompletion(
	ctx context.Context,
	p types.KnowledgeAutoTagPayload,
) (*time.Time, error) {
	if p.TaskInput == nil || p.CompletionInput == "" {
		return nil, types.ErrSourceLineageUnknown
	}
	now := time.Now().UTC().Truncate(time.Microsecond)
	res := s.db.WithContext(ctx).
		Model(&types.TaskPendingOp{}).
		Where(
			("task_type = ? AND scope = ? AND scope_id = ? AND tenant_id = ? AND dedup_k" +
				"ey = ? AND (op = ? OR (op = ? AND claimed_at < ?))"),
			types.TypeKnowledgeAutoTag,
			types.TaskScopeKnowledge,
			p.KnowledgeID,
			p.TenantID,
			p.TaskInput.ID,
			autoTagCompletionOp,
			autoTagCompletionRunning,
			now.Add(
				-3*time.Minute,
			),
		).
		Updates(map[string]any{"op": autoTagCompletionRunning, "claimed_at": now})
	if res.Error != nil {
		return nil, res.Error
	}
	if res.RowsAffected > 0 {
		return &now, nil
	}
	var row types.TaskPendingOp
	if err := s.db.WithContext(ctx).
		Where(("task_type = ? AND scope = ? AND scope_id = ? AND tenant_id = ? AND dedup_k" +
			"ey = ?"), types.TypeKnowledgeAutoTag, types.TaskScopeKnowledge, p.KnowledgeID, p.TenantID, p.TaskInput.ID).
		Take(&row).
		Error; err != nil {
		return nil, err
	}
	if row.Op == autoTagCompletionDone || row.Op == autoTagCompletionRejected || row.Op == autoTagCompletionRunning {
		return nil, nil
	}
	return nil, ErrKnowledgeInputChanged
}

// FinishKnowledgeAutoTagCompletion finishes knowledge auto tag completion.
func (s *KnowledgeInputProvenanceStore) FinishKnowledgeAutoTagCompletion(
	ctx context.Context,
	p types.KnowledgeAutoTagPayload,
	claim time.Time,
	success, rejected bool,
) error {
	op := autoTagCompletionOp
	if success {
		op = autoTagCompletionDone
	} else if rejected {
		op = autoTagCompletionRejected
	}
	updates := map[string]any{"op": op, "claimed_at": nil}
	if !success {
		updates["fail_count"] = gorm.Expr("fail_count + 1")
	}
	return s.db.WithContext(ctx).
		Model(&types.TaskPendingOp{}).
		Where(
			("task_type = ? AND scope = ? AND scope_id = ? AND tenant_id = ? AND dedup_k" +
				"ey = ? AND op = ? AND claimed_at = ?"),
			types.TypeKnowledgeAutoTag,
			types.TaskScopeKnowledge,
			p.KnowledgeID,
			p.TenantID,
			p.TaskInput.ID,
			autoTagCompletionRunning,
			claim,
		).
		Updates(updates).
		Error
}

type (
	autoTagCompletionContextKey struct{}
	autoTagCompletionClaim      struct {
		Payload   types.KnowledgeAutoTagPayload
		ClaimedAt time.Time
	}
)

// WithKnowledgeAutoTagCompletion binds knowledge auto tag completion.
func WithKnowledgeAutoTagCompletion(
	ctx context.Context,
	p types.KnowledgeAutoTagPayload,
	claimedAt time.Time,
) context.Context {
	return context.WithValue(ctx, autoTagCompletionContextKey{}, autoTagCompletionClaim{p, claimedAt})
}

func finishAutoTagCompletionInTx(ctx context.Context, tx *gorm.DB) error {
	claimed, ok := ctx.Value(autoTagCompletionContextKey{}).(autoTagCompletionClaim)
	if !ok {
		return nil
	}
	p, claim := claimed.Payload, claimed.ClaimedAt
	if p.TaskInput == nil || p.CompletionInput == "" {
		return types.ErrSourceLineageUnknown
	}
	res := tx.Model(&types.TaskPendingOp{}).
		Where(
			("task_type = ? AND scope = ? AND scope_id = ? AND tenant_id = ? AND dedup_k" +
				"ey = ? AND op = ? AND claimed_at = ?"),
			types.TypeKnowledgeAutoTag,
			types.TaskScopeKnowledge,
			p.KnowledgeID,
			p.TenantID,
			p.TaskInput.ID,
			autoTagCompletionRunning,
			claim,
		).
		Updates(map[string]any{"op": autoTagCompletionDone, "claimed_at": nil})
	if res.Error != nil {
		return res.Error
	}
	if res.RowsAffected != 1 {
		return errors.New("automatic tag intent no longer claimed")
	}
	return nil
}
