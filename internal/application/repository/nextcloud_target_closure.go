package repository

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var (
	// ErrNextcloudTargetDeleted reports a nextcloud target deleted condition.
	ErrNextcloudTargetDeleted = errors.New("nextcloud_target_deleted")
	// ErrNextcloudTargetUnverified reports that the current target closure cannot be verified.
	ErrNextcloudTargetUnverified = errors.New("nextcloud_target_unverified")
)

// NextcloudTargetClosure represents nextcloud target closure repository data.
// This immutable row closes an exact retained target. It is not pairing
// retirement, a deletion command, a physical-GC receipt or permission to purge
// source originals. Pair/credential/inventory rows remain for explicit repair.
type NextcloudTargetClosure struct {
	OperationID        string `gorm:"primaryKey"`
	ClosureID          string
	TenantID           uint64
	KnowledgeBaseID    string
	InstanceID         string
	BindingID          string
	OperationRevision  uint64
	Reason             string
	DeletionTimeMS     int64
	ClosedAtMS         int64
	RecoveryPlanSHA256 string
}

// TableName returns the persisted table name.
func (NextcloudTargetClosure) TableName() string { return "nextcloud_onboarding_target_closures" }

func targetClosureID(row NextcloudTargetClosure) string {
	canonical := strings.Join(
		[]string{
			"weknora-target-deleted-v1",
			row.OperationID,
			strconv.FormatUint(row.TenantID, 10),
			row.KnowledgeBaseID,
			row.InstanceID,
			row.BindingID,
			strconv.FormatUint(row.OperationRevision, 10),
			row.Reason,
			strconv.FormatInt(row.DeletionTimeMS, 10),
		},
		"\n",
	)
	hash := sha256.Sum256([]byte(canonical))
	return hex.EncodeToString(hash[:])
}

func validTargetClosure(row NextcloudTargetClosure, op types.NextcloudOnboardingOperation) bool {
	return row.OperationID == op.OperationID && row.TenantID == op.TenantID &&
		row.KnowledgeBaseID == op.KnowledgeBaseID &&
		row.InstanceID == op.InstanceID &&
		row.BindingID == op.BindingID &&
		row.OperationRevision > 0 &&
		row.OperationRevision <= op.Revision &&
		(row.Reason == "tenant_deleted" || row.Reason == "knowledge_base_deleted") &&
		row.DeletionTimeMS > 0 &&
		row.ClosedAtMS >= row.DeletionTimeMS &&
		row.ClosureID == targetClosureID(row)
}

// Exact current controls, not a source/KB UUID by itself, determine scope.
// No matching auto-onboarding operation means this guard grants nothing and
// leaves the pre-existing legacy source protocols responsible for access.
func requireNextcloudTargetOpenInTx(tx *gorm.DB, tenant uint64, kb string) error {
	if err := requireNextcloudTargetClosureSchema(tx); err != nil {
		return err
	}
	if err := requireNextcloudTargetTenantNotPermanentlyClosed(tx, tenant); err != nil {
		return err
	}
	if !tx.Migrator().HasTable("nextcloud_onboarding_operations") {
		if tx.Migrator().HasColumn("sessions", "nextcloud_scoped") ||
			tx.Migrator().HasTable("nextcloud_onboarding_credentials") ||
			tx.Migrator().HasTable(&NextcloudTargetClosure{}) {
			return ErrNextcloudTargetUnverified
		}
		return nil
	}
	var n int64
	if err := tx.Table("nextcloud_onboarding_operations").
		Where("tenant_id=? AND knowledge_base_id=?", tenant, kb).
		Count(&n).
		Error; err != nil {
		return err
	}
	if n == 0 {
		return nil
	}
	if !tx.Migrator().HasTable(&NextcloudTargetClosure{}) {
		return ErrNextcloudTargetUnverified
	}
	if err := tx.Model(&NextcloudTargetClosure{}).
		Where("tenant_id=? AND knowledge_base_id=?", tenant, kb).
		Count(&n).
		Error; err != nil {
		return err
	}
	if n != 0 {
		return ErrNextcloudTargetDeleted
	}
	var target types.KnowledgeBase
	if err := tx.Unscoped().
		Select("id,tenant_id,deleted_at").
		Where("tenant_id=? AND id=?", tenant, kb).
		Take(&target).
		Error; err != nil {
		return ErrNextcloudTargetUnverified
	}
	if target.DeletedAt.Valid {
		return ErrNextcloudTargetDeleted
	}
	return requireNextcloudTargetTenantLiveInTx(tx, tenant)
}

func requireNextcloudTargetTenantLiveInTx(tx *gorm.DB, tenant uint64) error {
	if err := requireNextcloudTargetClosureSchema(tx); err != nil {
		return err
	}
	if err := requireNextcloudTargetTenantNotPermanentlyClosed(tx, tenant); err != nil {
		return err
	}
	var target types.Tenant
	if err := tx.Unscoped().Select("id,deleted_at").Where("id=?", tenant).Take(&target).Error; err != nil {
		return ErrNextcloudTargetUnverified
	}
	if target.DeletedAt.Valid {
		return ErrNextcloudTargetDeleted
	}
	return nil
}

// Used before admission writer locks, never after a KB/content-fence lock.
func lockNextcloudTargetTenantLiveInTx(tx *gorm.DB, tenant uint64) error {
	if err := requireNextcloudTargetClosureSchema(tx); err != nil {
		return err
	}
	if err := requireNextcloudTargetTenantNotPermanentlyClosed(tx, tenant); err != nil {
		return err
	}
	var owner types.Tenant
	if err := tx.Unscoped().
		Select("id,deleted_at").
		Clauses(clause.Locking{Strength: "UPDATE"}).
		Where("id=?", tenant).
		Take(&owner).
		Error; err != nil {
		return ErrNextcloudTargetUnverified
	}
	if owner.DeletedAt.Valid {
		return ErrNextcloudTargetDeleted
	}
	return nil
}

// RequireTargetOpen returns the require target open result.
func (s *NextcloudOnboardingStore) RequireTargetOpen(ctx context.Context, op types.NextcloudOnboardingOperation) error {
	return requireNextcloudTargetOpenInTx(s.db.WithContext(ctx), op.TenantID, op.KnowledgeBaseID)
}

// Called by existing repository deletion transactions *after* the real soft
// delete, before commit. Unknown/legacy live sources cannot be closed from a
// guessed identity: the caller transaction rolls back instead.
func closeDeletedNextcloudTargetsInTx(tx *gorm.DB, tenant uint64, kb string) error {
	if _, ok := tx.Statement.ConnPool.(gorm.TxCommitter); !ok {
		return ErrNextcloudTargetUnverified
	}
	if err := requireNextcloudTargetClosureSchema(tx); err != nil {
		return err
	}
	var operations []types.NextcloudOnboardingOperation
	if tx.Migrator().HasTable("nextcloud_onboarding_operations") {
		q := tx.Where("tenant_id=?", tenant)
		if kb != "" {
			q = q.Where("knowledge_base_id=?", kb)
		}
		if err := q.Order("operation_id").
			Clauses(clause.Locking{Strength: "UPDATE"}).
			Find(&operations).
			Error; err != nil {
			return err
		}
	}
	// Do not let deleting the containing Tenant silently bypass a legacy source
	// retirement protocol not represented by these operation controls.
	if kb == "" && tx.Migrator().HasTable("nextcloud_source_pairings") {
		var unknown int64
		q := tx.Table("nextcloud_source_pairings p").
			Where("p.tenant_id=? AND p.state IN ?", tenant, []string{"active", "pending"})
		if tx.Migrator().HasTable("nextcloud_onboarding_operations") {
			q = q.Where(
				("NOT EXISTS(SELECT 1 FROM nextcloud_onboarding_operations o WHERE o.operati" +
					"on_id=p.operation_id AND o.tenant_id=p.tenant_id AND o.knowledge_base_id=p" +
					".knowledge_base_id AND o.instance_id=p.nextcloud_instance_id AND o.binding" +
					"_id=p.binding_id)"),
			)
		}
		if err := q.Count(&unknown).Error; err != nil {
			return err
		}
		if unknown > 0 {
			return ErrNextcloudTargetUnverified
		}
	}
	if kb == "" && tx.Migrator().HasTable("data_sources") {
		var unknown int64
		q := tx.Table("data_sources d").Where("d.tenant_id=? AND d.type='nextcloud' AND d.deleted_at IS NULL", tenant)
		if tx.Migrator().HasTable("nextcloud_onboarding_operations") &&
			tx.Migrator().HasTable("nextcloud_source_pairings") {
			q = q.Where(
				("NOT EXISTS(SELECT 1 FROM nextcloud_source_pairings p JOIN nextcloud_onboar" +
					"ding_operations o ON o.operation_id=p.operation_id AND o.tenant_id=p.tenan" +
					"t_id AND o.knowledge_base_id=p.knowledge_base_id WHERE p.datasource_id=d.i" +
					"d AND p.tenant_id=d.tenant_id AND p.knowledge_base_id=d.knowledge_base_id)"),
			)
		}
		if err := q.Count(&unknown).Error; err != nil {
			return err
		}
		if unknown > 0 {
			return ErrNextcloudTargetUnverified
		}
	}
	retainOwnerFact := func() error {
		if kb == "" && tx.Migrator().HasTable(&NextcloudTenantDeletion{}) {
			configured := len(operations) > 0
			if !configured && tx.Migrator().HasTable("nextcloud_onboarding_credentials") {
				var n int64
				if err := tx.Table("nextcloud_onboarding_credentials").
					Where("tenant_id=?", tenant).
					Count(&n).
					Error; err != nil {
					return err
				}
				configured = n > 0
			}
			if configured {
				if err := retainActualTenantDeletionInTx(tx, tenant); err != nil {
					return err
				}
			}
		}
		return nil
	}

	if len(operations) == 0 {
		return retainOwnerFact()
	}
	if !tx.Migrator().HasTable(&NextcloudTargetClosure{}) || !tx.Migrator().HasTable("nextcloud_content_fences") {
		return ErrNextcloudTargetUnverified
	}
	for _, op := range operations {
		// A completed explicit target migration owns the old target exit. A
		// later old-Tenant delete must not close its now-migrated NC root.
		if op.State == "retired" {
			continue
		}
		var prior NextcloudTargetClosure
		err := tx.Where("operation_id=?", op.OperationID).Take(&prior).Error
		if err == nil {
			if !validTargetClosure(prior, op) {
				return ErrNextcloudTargetUnverified
			}
			continue
		}
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}
		var owner types.Tenant
		if err := tx.Unscoped().Select("id,deleted_at").Where("id=?", op.TenantID).Take(&owner).Error; err != nil {
			return ErrNextcloudTargetUnverified
		}
		var target types.KnowledgeBase
		if err := tx.Unscoped().
			Select("id,tenant_id,deleted_at").
			Where("id=? AND tenant_id=?", op.KnowledgeBaseID, op.TenantID).
			Take(&target).
			Error; err != nil {
			return ErrNextcloudTargetUnverified
		}
		reason, deletion := "", int64(0)
		if owner.DeletedAt.Valid {
			reason, deletion = "tenant_deleted", owner.DeletedAt.Time.UnixMilli()
		} else if target.DeletedAt.Valid {
			reason, deletion = "knowledge_base_deleted", target.DeletedAt.Time.UnixMilli()
		}
		if reason == "" || deletion <= 0 || deletion > time.Now().UnixMilli()+5000 || op.Revision >= math.MaxInt64 ||
			op.PermissionVersion >= math.MaxInt64 {
			return ErrNextcloudTargetUnverified
		}
		// The broad fence invalidates every actual old read/build upgrade. This
		// does not delete any object, claim GC coverage or release bytes.
		nowMS, err := nextcloudLeaseNowMS(tx)
		if err != nil {
			return err
		}
		closedFence := map[string]any{
			"tenant_id":         op.TenantID,
			"knowledge_base_id": op.KnowledgeBaseID,
			"knowledge_id":      "",
			"datasource_id":     "",
			"external_id":       "",
			"state":             "retired",
			"epoch":             1,
			"retired_at_ms":     nowMS,
			"created_at_ms":     nowMS,
			"updated_at_ms":     nowMS,
		}
		if err := tx.Table("nextcloud_content_fences").
			Clauses(clause.OnConflict{DoNothing: true}).
			Create(closedFence).
			Error; err != nil {
			return err
		}
		// The owner is already deleted while its row lock is held. Do not use
		// the admission lock, which correctly refuses to keep a deleted target
		// open. This exact transition takes the fence row lock and preserves the
		// epoch of every already-closed fence, including deletion retries.
		if err := tx.Exec(`UPDATE nextcloud_content_fences
			SET state='retired', epoch=epoch+1, retired_at_ms=?, updated_at_ms=?
			WHERE tenant_id=? AND knowledge_base_id=? AND knowledge_id='' AND state='open'`,
			nowMS, nowMS, op.TenantID, op.KnowledgeBaseID).Error; err != nil {
			return err
		}
		// Hide originals without erasing their bytes or immutable receipts. Only
		// the old mutable successful-etag marker is invalidated for withdrawal.
		if tx.Migrator().HasTable("knowledges") {
			query := ("UPDATE knowledges SET enable_status='disabled',metadata=json_set(COALESCE(" +
				"metadata,'{}'),'$.nextcloud_etag','') WHERE tenant_id=? AND knowledge_base" +
				"_id=?")
			if tx.Name() == "postgres" {
				query = ("UPDATE knowledges SET enable_status='disabled',metadata=jsonb_set(COALESCE" +
					"(metadata::jsonb,'{}'::jsonb),'{nextcloud_etag}',to_jsonb(''::text),true) WHERE t" +
					"enant_id=? AND knowledge_base_id=?")
			}
			if err := tx.Exec(query, op.TenantID, op.KnowledgeBaseID).Error; err != nil {
				return err
			}
		}
		if tx.Migrator().HasTable("chunks") {
			if err := tx.Table("chunks").
				Where("tenant_id=? AND knowledge_base_id=?", op.TenantID, op.KnowledgeBaseID).
				Update("is_enabled", false).
				Error; err != nil {
				return err
			}
		}
		if tx.Migrator().HasTable("data_sources") && tx.Migrator().HasTable("nextcloud_source_pairings") {
			ids := tx.Table("nextcloud_source_pairings").
				Select("datasource_id").
				Where(
					("operation_id=? AND tenant_id=? AND knowledge_base_id=? AND nextcloud_insta" +
						"nce_id=? AND binding_id=?"),
					op.OperationID,
					op.TenantID,
					op.KnowledgeBaseID,
					op.InstanceID,
					op.BindingID,
				)
			if err := tx.Table("data_sources").
				Where("id IN (?) AND tenant_id=? AND knowledge_base_id=?", ids, op.TenantID, op.KnowledgeBaseID).
				Updates(map[string]any{"status": types.DataSourceStatusPaused, "sync_enabled": false}).
				Error; err != nil {
				return err
			}
		}
		if tx.Migrator().HasTable("nextcloud_event_connections") {
			if err := tx.Table("nextcloud_event_connections").
				Where(("tenant_id=? AND knowledge_base_id=? AND nextcloud_instance_id=? AND bindin"+
					"g_id=?"), op.TenantID, op.KnowledgeBaseID, op.InstanceID, op.BindingID).
				Update("status", "revoked").
				Error; err != nil {
				return err
			}
		}
		if err := tx.Where("operation_id=?", op.OperationID).Delete(&types.NextcloudSourceGrant{}).Error; err != nil {
			return err
		}
		op.Revision++
		op.PermissionVersion++
		q := tx.Model(&types.NextcloudOnboardingOperation{}).
			Where("operation_id=? AND revision=?", op.OperationID, op.Revision-1).
			Updates(map[string]any{
				"revision":             op.Revision,
				"permission_version":   op.PermissionVersion,
				"permission_synced_at": nil,
				"updated_at":           time.Now().UTC(),
			})
		if q.Error != nil {
			return q.Error
		}
		if q.RowsAffected != 1 {
			return ErrNextcloudTargetUnverified
		}
		if _, err := bumpPermissionVersion(tx, op.TenantID); err != nil {
			return err
		}
		row := NextcloudTargetClosure{
			OperationID:       op.OperationID,
			TenantID:          op.TenantID,
			KnowledgeBaseID:   op.KnowledgeBaseID,
			InstanceID:        op.InstanceID,
			BindingID:         op.BindingID,
			OperationRevision: op.Revision,
			Reason:            reason,
			DeletionTimeMS:    deletion,
			ClosedAtMS:        time.Now().UnixMilli(),
		}
		if row.ClosedAtMS < deletion {
			row.ClosedAtMS = deletion
		}
		row.ClosureID = targetClosureID(row)
		if err := tx.Create(&row).Error; err != nil {
			return err
		}
	}
	return retainOwnerFact()
}

// TargetObservation returns the target observation result.
func (s *NextcloudOnboardingStore) TargetObservation(
	ctx context.Context,
	c types.NextcloudOnboardingCredential,
	op types.NextcloudOnboardingOperation,
	dir, guid string,
) (types.NextcloudTargetObservation, error) {
	out := types.NextcloudTargetObservation{
		Schema:            "weknora-nextcloud-target-v1",
		State:             "unverified",
		Reason:            "target_unverified",
		OperationID:       op.OperationID,
		InstanceID:        op.InstanceID,
		BindingID:         op.BindingID,
		TenantID:          strconv.FormatUint(op.TenantID, 10),
		KnowledgeBaseID:   op.KnowledgeBaseID,
		OperationRevision: strconv.FormatUint(op.Revision, 10),
	}
	if c.ID != op.CredentialID || c.TenantID != op.TenantID || c.InstanceID != op.InstanceID ||
		!s.db.Migrator().HasTable(&NextcloudTargetClosure{}) {
		return out, ErrNextcloudTargetUnverified
	}
	if dir == "" && guid == "" {
		dir, guid = op.ActorDirectoryID, op.ActorObjectGUID
	}
	var row NextcloudTargetClosure
	err := s.db.WithContext(ctx).Where("operation_id=?", op.OperationID).Take(&row).Error
	if err == nil {
		if !validTargetClosure(row, op) {
			return out, ErrNextcloudTargetUnverified
		}
		// This exception permits only safe close metadata for the verified
		// original NC actor or an existing explicit delegation. It does not
		// restore deleted tenant membership, grants, Pair or other mutations.
		if err := authorizeTargetClosureObserver(s.db.WithContext(ctx), c, op, dir, guid); err != nil {
			return out, err
		}
		out.State, out.Reason = "target_deleted", row.Reason
		out.OperationRevision = strconv.FormatUint(row.OperationRevision, 10)
		out.ClosureID, out.DeletionTimeMS, out.ClosedAt = row.ClosureID, strconv.FormatInt(
			row.DeletionTimeMS,
			10,
		), strconv.FormatInt(
			row.ClosedAtMS/1000,
			10,
		)
		return out, nil
	}
	if !errors.Is(err, gorm.ErrRecordNotFound) {
		return out, err
	}
	// A completed migration never closes its old root merely because the old
	// owner was later deleted. Return only independently verified owner metadata.
	if op.State == "retired" {
		var ownerFact NextcloudTenantDeletion
		if err := s.db.WithContext(ctx).
			Where("tenant_id=?", op.TenantID).
			Take(&ownerFact).
			Error; err == nil &&
			validTenantDeletion(ownerFact) {
			if err := authorizeTargetClosureObserver(s.db.WithContext(ctx), c, op, dir, guid); err != nil {
				return out, err
			}
			return out, nil
		}
	}
	// A pre-hook or manually soft-deleted current control row is a positive
	// deletion fact. Repair its closure transaction only after safe observation
	// authorization; an absent KB/Tenant row can never produce a receipt.
	if repaired, err := s.repairPositiveTargetDeletion(ctx, c, op, dir, guid); err != nil {
		return out, err
	} else if repaired {
		current, err := s.Operation(ctx, c, op.OperationID)
		if err != nil {
			return out, err
		}
		return s.TargetObservation(ctx, c, current, dir, guid)
	}
	if _, err := s.Actor(ctx, c, dir, guid); err != nil {
		return out, err
	}
	if err := requireNextcloudTargetOpenInTx(s.db.WithContext(ctx), op.TenantID, op.KnowledgeBaseID); err != nil {
		return out, fmt.Errorf("%w: %w", ErrNextcloudTargetUnverified, err)
	}
	out.State, out.Reason = "present", ""
	return out, nil
}

func authorizeTargetClosureObserver(
	tx *gorm.DB,
	c types.NextcloudOnboardingCredential,
	op types.NextcloudOnboardingOperation,
	dir, guid string,
) error {
	if dir != c.DirectoryID || directoryUsable(tx, dir, time.Now().UTC()) != nil {
		return ErrNextcloudOnboardingDenied
	}
	var identity types.DirectoryIdentity
	if err := tx.Where(("directory_id=? AND object_guid=? AND status=? AND snapshot_version=(SELECT" +
		" snapshot_version FROM directories WHERE id=?)"), dir, guid, types.DirectoryObjectActive, dir).
		Take(&identity).
		Error; err != nil {
		return ErrNextcloudOnboardingDenied
	}
	if dir != op.ActorDirectoryID || guid != op.ActorObjectGUID {
		var delegated int64
		if err := tx.Model(&types.NextcloudOnboardingDelegation{}).
			Where("tenant_id=? AND directory_id=? AND object_guid=?", op.TenantID, dir, guid).
			Count(&delegated).
			Error; err != nil ||
			delegated != 1 {
			return ErrNextcloudOnboardingDenied
		}
	}
	return nil
}

func (s *NextcloudOnboardingStore) repairPositiveTargetDeletion(
	ctx context.Context,
	c types.NextcloudOnboardingCredential,
	op types.NextcloudOnboardingOperation,
	dir, guid string,
) (bool, error) {
	var owner types.Tenant
	var target types.KnowledgeBase
	db := s.db.WithContext(ctx)
	if db.Unscoped().Select("id,deleted_at").Where("id=?", op.TenantID).Take(&owner).Error != nil ||
		db.Unscoped().
			Select("id,tenant_id,deleted_at").
			Where("id=? AND tenant_id=?", op.KnowledgeBaseID, op.TenantID).
			Take(&target).
			Error != nil {
		return false, ErrNextcloudTargetUnverified
	}
	if !owner.DeletedAt.Valid && !target.DeletedAt.Valid {
		return false, nil
	}
	if err := authorizeTargetClosureObserver(db, c, op, dir, guid); err != nil {
		return false, err
	}
	err := db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Unscoped().
			Select("id,deleted_at").
			Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("id=?", op.TenantID).
			Take(&owner).
			Error; err != nil {
			return ErrNextcloudTargetUnverified
		}
		if err := tx.Unscoped().
			Select("id,tenant_id,deleted_at").
			Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("id=? AND tenant_id=?", op.KnowledgeBaseID, op.TenantID).
			Take(&target).
			Error; err != nil {
			return ErrNextcloudTargetUnverified
		}
		if !owner.DeletedAt.Valid && !target.DeletedAt.Valid {
			return ErrNextcloudTargetUnverified
		}
		if err := authorizeTargetClosureObserver(tx, c, op, dir, guid); err != nil {
			return err
		}
		kb := op.KnowledgeBaseID
		if owner.DeletedAt.Valid {
			kb = ""
		}
		if err := closeDeletedNextcloudTargetsInTx(tx, op.TenantID, kb); err != nil {
			return err
		}
		var current types.NextcloudOnboardingOperation
		var closure NextcloudTargetClosure
		if tx.Where("operation_id=?", op.OperationID).Take(&current).Error != nil ||
			tx.Where("operation_id=?", op.OperationID).Take(&closure).Error != nil ||
			!validTargetClosure(closure, current) {
			return ErrNextcloudTargetUnverified
		}
		return nil
	})
	return err == nil, err
}

// A rollback cannot resurrect automation on a positively deleted Tenant by
// choosing a fresh KB/operation UUID. Other Tenant/KB facts do not close it.
func requireNextcloudTargetTenantNotPermanentlyClosed(tx *gorm.DB, tenant uint64) error {
	if !tx.Migrator().HasTable(&NextcloudTenantDeletion{}) {
		return nil
	}
	var closed int64
	if err := tx.Model(&NextcloudTenantDeletion{}).Where("tenant_id=?", tenant).Count(&closed).Error; err != nil {
		return err
	}
	if closed > 0 {
		return ErrNextcloudTargetDeleted
	}
	return nil
}

func requireNextcloudTargetClosureSchema(tx *gorm.DB) error {
	current := tx.Migrator().HasColumn("sessions", "nextcloud_scoped") ||
		tx.Migrator().HasTable("nextcloud_onboarding_credentials") ||
		tx.Migrator().HasTable("nextcloud_onboarding_operations") ||
		tx.Migrator().HasTable(&NextcloudTargetClosure{}) ||
		tx.Migrator().HasTable(&NextcloudTenantDeletion{})
	if current &&
		(!tx.Migrator().HasTable("nextcloud_onboarding_operations") || !tx.Migrator().HasTable(
			&NextcloudTargetClosure{},
		) || !tx.Migrator().HasTable(&NextcloudTenantDeletion{})) {
		return ErrNextcloudTargetUnverified
	}
	return nil
}
