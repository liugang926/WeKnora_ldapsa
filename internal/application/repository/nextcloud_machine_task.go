package repository

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"time"

	apperrors "github.com/Tencent/WeKnora/internal/errors"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// ErrNextcloudMachineTaskInvalid rejects unverified, stale or consumed machine work.
var ErrNextcloudMachineTaskInvalid = apperrors.NewProtocolError(
	errors.New("nextcloud machine task is unverified, stale or already consumed"),
	"Nextcloud machine task is unverified, stale or already consumed",
)

const nextcloudMachineReceiptField = "_nextcloud_machine_task"

type nextcloudMachineTask struct {
	ReceiptID            string `gorm:"primaryKey"`
	TaskID               string
	TaskType             string
	TenantID             uint64
	KnowledgeBaseID      string
	KnowledgeID          string
	Attempt              int
	PayloadDigest        string
	OriginalProof        string
	OriginalKind         string
	BuildGeneration      uint64
	DataSourceID         string `gorm:"column:datasource_id"`
	ExternalID           string
	PairOperationID      string
	PairPublicationEpoch int64
	ManagerPurposeID     string
	ManagerVersion       uint64
	ManagerDigest        string
	CreatedAtMS          int64
}

func (nextcloudMachineTask) TableName() string { return "nextcloud_machine_tasks" }

type nextcloudMachineTaskClaim struct {
	ReceiptID    string `gorm:"primaryKey"`
	State        string
	ClaimID      string
	BuildLeaseID string
	Version      uint64
}

func (nextcloudMachineTaskClaim) TableName() string { return "nextcloud_machine_task_claims" }

type nextcloudMachineTaskEnvelope struct {
	ID     string `json:"id"`
	TaskID string `json:"task_id"`
}

type nextcloudMachinePayload struct {
	TenantID        uint64                          `json:"tenant_id"`
	KnowledgeBaseID string                          `json:"knowledge_base_id"`
	KnowledgeID     string                          `json:"knowledge_id"`
	Attempt         int                             `json:"attempt"`
	Initiator       types.TaskInitiator             `json:"initiator"`
	Original        *types.KnowledgeBuildInputProof `json:"_original_input"`
	GraphOriginal   *types.KnowledgeBuildInputProof `json:"original_input_proof"`
	Receipt         nextcloudMachineTaskEnvelope    `json:"_nextcloud_machine_task"`
}

func nextcloudMachineTaskSupported(kind string) bool {
	switch kind {
	case types.TypeDataTableSummary,
		types.TypeDocumentProcess,
		types.TypeChunkExtract,
		types.TypeKnowledgePostProcess,
		types.TypeImageMultimodal,
		types.TypeSummaryGeneration,
		types.TypeQuestionGeneration,
		types.TypeKnowledgeAutoTag:
		return true
	}
	return false
}

func machineTaskPayload(kind string, raw []byte) (nextcloudMachinePayload, string, error) {
	var p nextcloudMachinePayload
	if !nextcloudMachineTaskSupported(kind) || json.Unmarshal(raw, &p) != nil ||
		p.TenantID == 0 || p.KnowledgeID == "" || p.Attempt < 0 || !reflect.DeepEqual(p.Initiator,
		types.TaskInitiator{}) {
		return p, "", ErrNextcloudMachineTaskInvalid
	}
	if kind == types.TypeChunkExtract {
		p.Original = p.GraphOriginal
	}
	if kind != types.TypeDocumentProcess && (p.Original == nil || p.Original.TenantID != p.TenantID ||
		p.Original.KnowledgeID != p.KnowledgeID ||
		p.Original.KnowledgeBaseID == "" || (p.KnowledgeBaseID != "" &&
		p.KnowledgeBaseID != p.Original.KnowledgeBaseID)) {
		return p, "", ErrNextcloudMachineTaskInvalid
	}
	if kind != types.TypeDocumentProcess {
		p.KnowledgeBaseID = p.Original.KnowledgeBaseID
	} else if p.KnowledgeBaseID == "" {
		return p, "", ErrNextcloudMachineTaskInvalid
	}
	var fields map[string]json.RawMessage
	if json.Unmarshal(raw, &fields) != nil {
		return p, "", ErrNextcloudMachineTaskInvalid
	}
	delete(fields, nextcloudMachineReceiptField)
	digest, err := KnowledgeInputDigest(fields)
	return p, digest, err
}

// managerTaskPurposeInTx is intentionally private. A UUID, empty initiator or
// ordinary build lease alone cannot authorize an automatic machine task.
func managerTaskPurposeInTx(ctx context.Context, tx *gorm.DB, tenant uint64, kb,
	source string,
) (*NextcloudOnboardingManagerPurpose, NextcloudSourcePairing, error) {
	var pair NextcloudSourcePairing
	if err := scopedOnboardingConfigurationPurposeInTx(ctx, tx, tenant, kb); err != nil {
		return nil, pair, err
	}
	purpose, present := ctx.Value(scopedOnboardingConfigurationPurposeKey{}).(scopedOnboardingConfigurationPurpose)
	if !present || purpose.datasource != source {
		return nil, pair, ErrNextcloudMachineTaskInvalid
	}
	var err error
	pair, _, err = scopedConfigurationPairInTx(tx, tenant, kb, source)
	if err != nil {
		return nil, pair, err
	}
	var op types.NextcloudOnboardingOperation
	if err := tx.Where("operation_id = ? AND tenant_id = ? AND knowledge_base_id = ? AND state = 'active'",
		pair.OperationID, tenant, kb).Take(&op).Error; err != nil {
		return nil, pair, err
	}
	var credential types.NextcloudOnboardingCredential
	if err := tx.Where("id = ? AND enabled = ?", op.CredentialID, true).Take(&credential).Error; err != nil {
		return nil, pair, err
	}
	if op.TenantID != credential.TenantID || op.InstanceID != credential.InstanceID ||
		op.ProcessingTemplateID != credential.TemplateKBID || op.ActorDirectoryID != credential.DirectoryID {
		return nil, pair, ErrNextcloudMachineTaskInvalid
	}
	credential, err = currentOnboardingCredential(tx, credential)
	if err != nil {
		return nil, pair, err
	}
	manager, err := currentOnboardingManagerPurpose(tx, credential, op)
	if err != nil {
		return nil, pair, err
	}
	if err := currentOnboardingActor(tx, credential, manager.ActorDirectoryID, manager.ActorObjectGUID); err != nil {
		return nil, pair, err
	}
	return manager, pair, nil
}

func requireMachineConfigurationRead(ctx context.Context, tenant uint64, kb string) error {
	if ctx == nil {
		return ErrNextcloudMachineTaskInvalid
	}
	slot, ok := ctx.Value(scopedOnboardingConfigurationReadKey{}).(*scopedOnboardingConfigurationReadSlot)
	if !ok || slot == nil {
		return ErrNextcloudMachineTaskInvalid
	}
	slot.mu.Lock()
	if slot.closed || slot.tenant != tenant || slot.kb != kb || slot.read == nil {
		slot.mu.Unlock()
		return ErrNextcloudMachineTaskInvalid
	}
	read := slot.read
	switch read.(type) {
	case *NextcloudOnboardingConfigurationRead, *NextcloudIndependentConfigurationRead:
	default:
		slot.mu.Unlock()
		return ErrNextcloudMachineTaskInvalid
	}
	slot.mu.Unlock()
	if err := CheckScopedOnboardingConfigurationReads(ctx); err != nil {
		return err
	}
	return read.Checkpoint(ctx)
}

// CheckNextcloudAutomaticSyncManagerPurpose adds a fresh manager decision to
// an already held exact original-fetch configuration purpose. It grants no
// Caller, ordinary KB role, source READ capability or queue authority.
func (s *KnowledgeInputProvenanceStore) CheckNextcloudAutomaticSyncManagerPurpose(ctx context.Context,
	tenant uint64, kb, source string,
) error {
	if s == nil || s.db == nil || ctx == nil {
		return ErrNextcloudMachineTaskInvalid
	}
	purpose, present := ctx.Value(scopedOnboardingConfigurationPurposeKey{}).(scopedOnboardingConfigurationPurpose)
	if !present || !purpose.originalFetch {
		return ErrNextcloudMachineTaskInvalid
	}
	if err := requireMachineConfigurationRead(ctx, tenant, kb); err != nil {
		return err
	}
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		_, _, err := managerTaskPurposeInTx(ctx, tx, tenant, kb, source)
		return err
	})
}

// PrepareNextcloudMachineTask binds the complete emitted queue bytes to a
// fixed execution identity. Only a producer already carrying the real SQL
// build lease, original proof reader and live configuration purpose can issue.
// Ordinary human/non-Nextcloud tasks retain their original queue behavior.
func PrepareNextcloudMachineTask(ctx context.Context, kind string, raw []byte, taskID string) ([]byte, string, error) {
	if _, present := ctx.Value(nextcloudBuildContextKey{}).(nextcloudBuildContext); !present {
		return raw, "", nil
	}
	var actor struct {
		Initiator types.TaskInitiator `json:"initiator"`
	}
	if json.Unmarshal(raw, &actor) != nil {
		return nil, "", ErrNextcloudMachineTaskInvalid
	}
	if !reflect.DeepEqual(actor.Initiator, types.TaskInitiator{}) {
		return raw, "", nil
	}
	build, ok := ctx.Value(knowledgeBuildInputKey{}).(knowledgeBuildInputContext)
	if !ok {
		return nil, "", ErrNextcloudMachineTaskInvalid
	}
	store, ok := build.reader.(*KnowledgeInputProvenanceStore)
	if !ok || store == nil {
		return nil, "", ErrNextcloudMachineTaskInvalid
	}
	return store.PrepareNextcloudMachineTask(ctx, kind, raw, taskID)
}

// PrepareNextcloudMachineTask issues an exact internal queue receipt from a live producer context.
func (s *KnowledgeInputProvenanceStore) PrepareNextcloudMachineTask(ctx context.Context, kind string,
	raw []byte, taskID string,
) ([]byte, string, error) {
	if ctx == nil {
		return raw, "", nil
	}
	_, build := ctx.Value(nextcloudBuildContextKey{}).(nextcloudBuildContext)
	purpose, fetch := ctx.Value(scopedOnboardingConfigurationPurposeKey{}).(scopedOnboardingConfigurationPurpose)
	if !build && (!fetch || !purpose.originalFetch || kind != types.TypeDocumentProcess) {
		return raw, "", nil
	}
	var actor struct {
		Initiator types.TaskInitiator `json:"initiator"`
	}
	if json.Unmarshal(raw, &actor) != nil {
		return nil, "", ErrNextcloudMachineTaskInvalid
	}
	if !reflect.DeepEqual(actor.Initiator, types.TaskInitiator{}) {
		return raw, "", nil
	}
	var identity nextcloudMachinePayload
	if json.Unmarshal(raw, &identity) != nil {
		return nil, "", ErrNextcloudMachineTaskInvalid
	}
	managed, err := NewNextcloudOnboardingStore(s.db).ManagedKB(ctx, identity.TenantID, identity.KnowledgeBaseID)
	if err != nil {
		return nil, "", err
	}
	if !managed && identity.KnowledgeBaseID == "" && identity.Original != nil {
		managed, err = NewNextcloudOnboardingStore(s.db).ManagedKB(ctx, identity.TenantID,
			identity.Original.KnowledgeBaseID)
	}
	if !managed && identity.GraphOriginal != nil {
		managed, err = NewNextcloudOnboardingStore(s.db).ManagedKB(ctx, identity.TenantID,
			identity.GraphOriginal.KnowledgeBaseID)
	}
	if err != nil {
		return nil, "", err
	}
	if !managed {
		return raw, "", nil
	}
	p, digest, err := machineTaskPayload(kind, raw)
	if err != nil || p.Receipt.ID != "" {
		return nil, "", ErrNextcloudMachineTaskInvalid
	}
	scope, original, generation, proofKind, err := s.machineTaskOriginal(ctx, kind, raw, p)
	if err != nil {
		return nil, "", err
	}
	if err := requireMachineConfigurationRead(ctx, p.TenantID, p.KnowledgeBaseID); err != nil {
		return nil, "", err
	}
	if taskID == "" {
		taskID = "nc-machine:" + uuid.NewString()
	}
	row := nextcloudMachineTask{
		ReceiptID: uuid.NewString(), TaskID: taskID, TaskType: kind,
		TenantID: p.TenantID, KnowledgeBaseID: p.KnowledgeBaseID, KnowledgeID: p.KnowledgeID,
		Attempt: p.Attempt, PayloadDigest: digest, OriginalProof: original, OriginalKind: proofKind,
		BuildGeneration: generation, DataSourceID: scope.DataSourceID, ExternalID: scope.ExternalID,
		CreatedAtMS: 0,
	}
	err = s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if proofKind == "build" {
			if err := ValidateNextcloudBuildWrite(ctx, tx, p.TenantID, p.KnowledgeBaseID, p.KnowledgeID); err != nil {
				return err
			}
		}
		now, err := nextcloudLeaseNowMS(tx)
		if err != nil {
			return err
		}
		row.CreatedAtMS = now
		manager, pair, err := managerTaskPurposeInTx(ctx, tx, p.TenantID, p.KnowledgeBaseID, scope.DataSourceID)
		if err != nil {
			return err
		}
		row.PairOperationID, row.PairPublicationEpoch = pair.OperationID, pair.PublicationEpoch
		row.ManagerPurposeID, row.ManagerVersion, row.ManagerDigest = manager.PurposeID, manager.Version,
			onboardingDigest(*manager)
		var existing nextcloudMachineTask
		existingErr := tx.Where("task_id = ?", row.TaskID).Take(&existing).Error
		if existingErr == nil {
			candidate := row
			candidate.ReceiptID, candidate.CreatedAtMS = existing.ReceiptID, existing.CreatedAtMS
			if !reflect.DeepEqual(candidate, existing) {
				return ErrNextcloudMachineTaskInvalid
			}
			row = existing
			return nil
		}
		if !errors.Is(existingErr, gorm.ErrRecordNotFound) {
			return existingErr
		}
		if err := tx.Create(&row).Error; err != nil {
			return err
		}
		return tx.Create(&nextcloudMachineTaskClaim{ReceiptID: row.ReceiptID, State: "ready", Version: 1}).Error
	})
	if err != nil {
		return nil, "", err
	}
	var fields map[string]json.RawMessage
	if json.Unmarshal(raw, &fields) != nil {
		return nil, "", ErrNextcloudMachineTaskInvalid
	}
	fields[nextcloudMachineReceiptField], _ = json.Marshal(nextcloudMachineTaskEnvelope{
		ID:     row.ReceiptID,
		TaskID: row.TaskID,
	})
	raw, err = json.Marshal(fields)
	return raw, taskID, err
}

// BeginNextcloudMachineTask never supplies a human identity. The worker must
// already own an exact current build lease and hold a fresh configuration read.
// Claims serialize retries across Redis and Lite. A crashed claim can move only
// after its old durable lease expired/released; its old writes remain fenced.
func (s *KnowledgeInputProvenanceStore) BeginNextcloudMachineTask(ctx context.Context, kind string,
	raw []byte, taskID string,
) (context.Context, func(bool, bool) error, error) {
	noop := func(bool, bool) error { return nil }
	p, digest, err := machineTaskPayload(kind, raw)
	if err != nil || p.Receipt.ID == "" || p.Receipt.TaskID != taskID || taskID == "" {
		return ctx, noop, ErrNextcloudMachineTaskInvalid
	}
	lease, scope, err := NextcloudBuildInputLease(ctx)
	if err != nil || scope.TenantID != p.TenantID || scope.KnowledgeBaseID != p.KnowledgeBaseID ||
		scope.KnowledgeID != p.KnowledgeID {
		return ctx, noop, ErrNextcloudMachineTaskInvalid
	}
	_, original, generation, proofKind, err := s.machineTaskOriginal(ctx, kind, raw, p)
	if err != nil {
		return ctx, noop, err
	}
	if err := requireMachineConfigurationRead(ctx, p.TenantID, p.KnowledgeBaseID); err != nil {
		return ctx, noop, err
	}
	claimID := uuid.NewString()
	err = s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := NewNextcloudContentLeaseStore(s.db).ValidateBuildLeaseInTx(ctx, tx, lease, scope); err != nil {
			return err
		}
		var row nextcloudMachineTask
		if err := tx.Where("receipt_id = ?", p.Receipt.ID).Take(&row).Error; err != nil {
			return err
		}
		if row.TaskType != kind || row.TaskID != taskID || row.TenantID != p.TenantID ||
			row.KnowledgeBaseID != p.KnowledgeBaseID || row.KnowledgeID != p.KnowledgeID ||
			row.Attempt != p.Attempt || row.PayloadDigest != digest || row.OriginalProof != original ||
			row.OriginalKind != proofKind || row.BuildGeneration != generation ||
			row.DataSourceID != scope.DataSourceID || row.ExternalID != scope.ExternalID {
			return ErrNextcloudMachineTaskInvalid
		}
		manager, pair, err := managerTaskPurposeInTx(ctx, tx, p.TenantID, p.KnowledgeBaseID, scope.DataSourceID)
		if err != nil {
			return err
		}
		if row.PairOperationID != pair.OperationID || row.PairPublicationEpoch != pair.PublicationEpoch ||
			row.ManagerPurposeID != manager.PurposeID || row.ManagerVersion != manager.Version ||
			row.ManagerDigest != onboardingDigest(*manager) {
			return ErrNextcloudMachineTaskInvalid
		}
		var claim nextcloudMachineTaskClaim
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("receipt_id = ?",
			row.ReceiptID).Take(&claim).Error; err != nil {
			return err
		}
		if claim.State == "running" {
			var old nextcloudContentLeaseRow
			if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("lease_id = ?",
				claim.BuildLeaseID).Take(&old).Error; err != nil {
				return err
			}
			now, err := nextcloudLeaseNowMS(tx)
			if err != nil {
				return err
			}
			if old.ReleasedAtMS == nil && old.ExpiresAtMS > now {
				return ErrNextcloudMachineTaskInvalid
			}
		} else if claim.State != "ready" {
			return ErrNextcloudMachineTaskInvalid
		}
		q := tx.Model(&nextcloudMachineTaskClaim{}).Where("receipt_id = ? AND version = ?", row.ReceiptID,
			claim.Version).Updates(map[string]any{
			"state": "running", "claim_id": claimID,
			"build_lease_id": lease.ID, "version": claim.Version + 1,
		})
		if q.Error != nil {
			return q.Error
		}
		if q.RowsAffected != 1 {
			return ErrNextcloudMachineTaskInvalid
		}
		return nil
	})
	if err != nil {
		return ctx, noop, err
	}
	finish := func(success, retryable bool) error {
		end, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer cancel()
		state := "failed"
		if success {
			state = "done"
		} else if retryable {
			state = "ready"
		}
		q := s.db.WithContext(end).Model(&nextcloudMachineTaskClaim{}).
			Where("receipt_id = ? AND state = 'running' AND claim_id = ? AND build_lease_id = ?",
				p.Receipt.ID, claimID, lease.ID).Update("state", state)
		if q.Error != nil {
			return q.Error
		}
		if q.RowsAffected != 1 {
			return ErrNextcloudMachineTaskInvalid
		}
		return nil
	}
	current := context.WithValue(ctx, nextcloudMachineExecutionKey{},
		nextcloudMachineExecution{
			receipt: p.Receipt.ID, claim: claimID, lease: lease.ID, kind: kind,
			scope: scope, parent: ctx, store: s,
		})
	return current, finish, nil
}
