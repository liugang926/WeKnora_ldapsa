package repository

import (
	"context"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"gorm.io/gorm"
)

type (
	nextcloudMachineExecutionKey struct{}
	nextcloudMachineExecution    struct {
		receipt, claim, lease, kind string
		scope                       NextcloudContentScope
		parent                      context.Context
		store                       *KnowledgeInputProvenanceStore
	}
)

// ValidateNextcloudMachineTaskWriteInTx is an additional SQL fence for admitted
// machine work. Finishing/taking over a claim cannot leave an older worker
// writing under a still-cached context. It never grants an ordinary edit role.
func ValidateNextcloudMachineTaskWriteInTx(ctx context.Context, tx *gorm.DB, tenant uint64, kb,
	knowledge string,
) error {
	execution, present := ctx.Value(nextcloudMachineExecutionKey{}).(nextcloudMachineExecution)
	if !present {
		return nil
	}
	if execution.parent == nil || execution.parent.Err() != nil || ctx.Err() != nil ||
		execution.scope.TenantID != tenant || execution.scope.KnowledgeBaseID != kb ||
		execution.scope.KnowledgeID != knowledge {
		return ErrNextcloudMachineTaskInvalid
	}
	q := tx.Model(&nextcloudMachineTaskClaim{}).
		Where("receipt_id = ? AND state = 'running' AND claim_id = ? AND build_lease_id = ?",
			execution.receipt, execution.claim, execution.lease).UpdateColumn("claim_id", gorm.Expr("claim_id"))
	if q.Error != nil {
		return q.Error
	}
	if q.RowsAffected != 1 {
		return ErrNextcloudMachineTaskInvalid
	}
	manager, pair, err := managerTaskPurposeInTx(
		ctx, tx, tenant, kb, execution.scope.DataSourceID, execution.scope.KnowledgeID,
	)
	if err != nil {
		return err
	}
	var receipt nextcloudMachineTask
	if err := tx.Where("receipt_id = ?", execution.receipt).Take(&receipt).Error; err != nil {
		return err
	}
	if receipt.ManagerPurposeID != manager.PurposeID || receipt.ManagerVersion != manager.Version ||
		receipt.ManagerDigest != onboardingDigest(*manager) || receipt.PairOperationID != pair.OperationID ||
		receipt.PairPublicationEpoch != pair.PublicationEpoch {
		return ErrNextcloudMachineTaskInvalid
	}
	return nil
}

// CheckNextcloudDocumentMachineTaskWrite is only consumed by ProcessDocument's
// existing edit checkpoint. Other task types, actions and ordinary web calls
// cannot consume this private execution capability.
func CheckNextcloudDocumentMachineTaskWrite(ctx context.Context, tenant uint64, kb, knowledge string) (bool, error) {
	execution, present := ctx.Value(nextcloudMachineExecutionKey{}).(nextcloudMachineExecution)
	if !present {
		return false, nil
	}
	if execution.kind != types.TypeDocumentProcess || execution.store == nil {
		return false, ErrNextcloudMachineTaskInvalid
	}
	if err := requireMachineConfigurationRead(ctx, tenant, kb); err != nil {
		return false, err
	}
	err := execution.store.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := ValidateNextcloudBuildWrite(ctx, tx, tenant, kb, knowledge); err != nil {
			return err
		}
		_, _, err := managerTaskPurposeInTx(
			ctx, tx, tenant, kb, execution.scope.DataSourceID, execution.scope.KnowledgeID,
		)
		return err
	})
	return err == nil, err
}

// RejectNextcloudMachineTask consumes only an exact unclaimed queue receipt.
// A tampered payload/type/ID cannot fail a different legitimate task. This
// closes permanent pre-claim authority failures without touching live claims.
func (s *KnowledgeInputProvenanceStore) RejectNextcloudMachineTask(ctx context.Context, kind string,
	raw []byte, taskID string,
) error {
	p, digest, err := machineTaskPayload(kind, raw)
	if err != nil || p.Receipt.ID == "" || p.Receipt.TaskID != taskID || taskID == "" {
		return ErrNextcloudMachineTaskInvalid
	}
	end, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	return s.db.WithContext(end).Transaction(func(tx *gorm.DB) error {
		var row nextcloudMachineTask
		predicate := "receipt_id = ? AND task_id = ? AND task_type = ? AND tenant_id = ? AND " +
			"knowledge_base_id = ? AND knowledge_id = ? AND attempt = ? AND payload_digest = ?"
		if err := tx.Where(predicate, p.Receipt.ID, taskID, kind, p.TenantID, p.KnowledgeBaseID,
			p.KnowledgeID, p.Attempt, digest).Take(&row).Error; err != nil {
			return err
		}
		return tx.Model(&nextcloudMachineTaskClaim{}).Where("receipt_id = ? AND state = 'ready'",
			row.ReceiptID).Update("state", "failed").Error
	})
}

// CheckNextcloudMachineTaskExecution is the non-SQL checkpoint before and
// after a machine model/provider call. Current manager authority is checked in
// addition to the already bound source input, model receipt and durable lease.
func CheckNextcloudMachineTaskExecution(ctx context.Context) error {
	execution, present := ctx.Value(nextcloudMachineExecutionKey{}).(nextcloudMachineExecution)
	if !present {
		return nil
	}
	if execution.store == nil {
		return ErrNextcloudMachineTaskInvalid
	}
	return execution.store.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		return ValidateNextcloudBuildWrite(ctx, tx, execution.scope.TenantID,
			execution.scope.KnowledgeBaseID, execution.scope.KnowledgeID)
	})
}
