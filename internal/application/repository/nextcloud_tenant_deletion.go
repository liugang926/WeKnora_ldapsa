package repository

import (
	"context"
	"errors"
	"strconv"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"gorm.io/gorm"
)

// The immutable owner tombstone survives a previous KB closure and closes only
// this Tenant's automation namespace. Source originals and repair keys remain.
type NextcloudTenantDeletion struct {
	TenantID           uint64 `gorm:"primaryKey"`
	DeletionID         string
	DeletionTimeMS     int64
	ClosedAtMS         int64
	RecoveryPlanSHA256 string
}

func (NextcloudTenantDeletion) TableName() string { return "nextcloud_onboarding_tenant_deletions" }
func tenantDeletionID(tenant uint64, deletion int64) string {
	return recoverySHA([]byte("weknora-tenant-deleted-v1\n" + strconv.FormatUint(tenant, 10) + "\n" + strconv.FormatInt(deletion, 10)))
}
func validTenantDeletion(row NextcloudTenantDeletion) bool {
	return row.TenantID > 0 && row.DeletionTimeMS > 0 && row.ClosedAtMS >= row.DeletionTimeMS && row.DeletionID == tenantDeletionID(row.TenantID, row.DeletionTimeMS)
}
func retainActualTenantDeletionInTx(tx *gorm.DB, tenant uint64) error {
	if !tx.Migrator().HasTable(&NextcloudTenantDeletion{}) {
		return ErrNextcloudTargetUnverified
	}
	var owner types.Tenant
	if tx.Unscoped().Select("id,deleted_at").Where("id=?", tenant).Take(&owner).Error != nil || !owner.DeletedAt.Valid {
		return ErrNextcloudTargetUnverified
	}
	deletion := owner.DeletedAt.Time.UnixMilli()
	if deletion <= 0 || deletion > time.Now().UnixMilli()+5000 {
		return ErrNextcloudTargetUnverified
	}
	var prior NextcloudTenantDeletion
	err := tx.Where("tenant_id=?", tenant).Take(&prior).Error
	if err == nil {
		if !validTenantDeletion(prior) || prior.DeletionTimeMS != deletion {
			return ErrNextcloudTargetUnverified
		}
		return nil
	}
	if !errors.Is(err, gorm.ErrRecordNotFound) {
		return err
	}
	closed := time.Now().UnixMilli()
	if closed < deletion {
		closed = deletion
	}
	return tx.Create(&NextcloudTenantDeletion{TenantID: tenant, DeletionID: tenantDeletionID(tenant, deletion), DeletionTimeMS: deletion, ClosedAtMS: closed}).Error
}

// Safe metadata is available through the existing operation GET, including a
// retired historical operation. Only current original-actor/delegation authority
// permits observation after Tenant membership has been removed.
func (s *NextcloudOnboardingStore) TenantDeletionObservation(ctx context.Context, c types.NextcloudOnboardingCredential, op types.NextcloudOnboardingOperation, dir, guid string) (*types.NextcloudTenantDeletionObservation, error) {
	if c.ID != op.CredentialID || c.TenantID != op.TenantID || c.InstanceID != op.InstanceID || !s.db.Migrator().HasTable(&NextcloudTenantDeletion{}) {
		return nil, ErrNextcloudTargetUnverified
	}
	if dir == "" && guid == "" {
		dir, guid = op.ActorDirectoryID, op.ActorObjectGUID
	}
	db := s.db.WithContext(ctx)
	var row NextcloudTenantDeletion
	err := db.Where("tenant_id=?", op.TenantID).Take(&row).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		var owner types.Tenant
		if db.Unscoped().Select("id,deleted_at").Where("id=?", op.TenantID).Take(&owner).Error != nil {
			return nil, ErrNextcloudTargetUnverified
		}
		if !owner.DeletedAt.Valid {
			return nil, nil
		}
		if err = authorizeTargetClosureObserver(db, c, op, dir, guid); err != nil {
			return nil, err
		}
		// Repair only a retained positive real tombstone; absent rows never suffice.
		if err = db.Transaction(func(tx *gorm.DB) error { return closeDeletedNextcloudTargetsInTx(tx, op.TenantID, "") }); err != nil {
			return nil, err
		}
		err = db.Where("tenant_id=?", op.TenantID).Take(&row).Error
	}
	if err != nil {
		return nil, err
	}
	if !validTenantDeletion(row) {
		return nil, ErrNextcloudTargetUnverified
	}
	if err = authorizeTargetClosureObserver(db, c, op, dir, guid); err != nil {
		return nil, err
	}
	return &types.NextcloudTenantDeletionObservation{Schema: "weknora-nextcloud-tenant-deletion-v1", State: "tenant_deleted", OperationID: op.OperationID, InstanceID: op.InstanceID, BindingID: op.BindingID, TenantID: strconv.FormatUint(op.TenantID, 10), DeletionID: row.DeletionID, DeletionTimeMS: strconv.FormatInt(row.DeletionTimeMS, 10), ClosedAt: strconv.FormatInt(row.ClosedAtMS/1000, 10)}, nil
}
