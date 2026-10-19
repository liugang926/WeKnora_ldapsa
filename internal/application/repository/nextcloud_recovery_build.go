package repository

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"gorm.io/gorm"
)

type nextcloudRecoveryBuildLease struct {
	LeaseID         string `gorm:"primaryKey"`
	PlanSHA256      string
	OperationID     string
	TenantID        uint64
	KnowledgeBaseID string
	DataSourceID    string `gorm:"column:datasource_id"`
	KnowledgeID     string
	BuildEpoch      int64
	KBEpoch         int64
	ManifestSHA256  string
	VerifiedAtMS    int64
}

func (nextcloudRecoveryBuildLease) TableName() string { return "nextcloud_recovery_build_leases" }
func nextcloudCurrentControlModule(tx *gorm.DB) bool {
	return tx.Migrator().HasTable(&nextcloudControlRecoveryGate{}) ||
		tx.Migrator().HasColumn("sessions", "nextcloud_scoped") ||
		tx.Migrator().HasTable("nextcloud_onboarding_operations") ||
		tx.Migrator().HasTable("nextcloud_onboarding_credentials")
}

func nextcloudUnpairedCurrentKB(tx *gorm.DB, tenant uint64, kb string) (bool, error) {
	if !nextcloudCurrentControlModule(tx) {
		return false, nil
	}
	var n int64
	if tx.Migrator().HasTable(&types.DataSource{}) {
		if e := tx.Model(&types.DataSource{}).
			Where("tenant_id=? AND knowledge_base_id=? AND type=?", tenant, kb, types.ConnectorTypeNextcloud).
			Count(&n).
			Error; e != nil {
			return true, e
		}
		if n > 0 {
			return true, nil
		}
	}
	if tx.Migrator().HasColumn("knowledge_bases", "ever_had_nextcloud_source") {
		if e := tx.Table("knowledge_bases").
			Where("tenant_id=? AND id=? AND ever_had_nextcloud_source=?", tenant, kb, true).
			Count(&n).
			Error; e != nil {
			return true, e
		}
		if n > 0 {
			return true, nil
		}
	}
	return false, nil
}

func nextcloudKBControlsClosed(tx *gorm.DB, tenant uint64, kb string) (bool, error) {
	if err := requireNextcloudTargetOpenInTx(tx, tenant, kb); err != nil {
		return true, err
	}
	if !tx.Migrator().HasTable(&NextcloudSourcePairing{}) {
		return nextcloudCurrentControlModule(tx), nil
	}
	var pairs []NextcloudSourcePairing
	if e := tx.Where("tenant_id=? AND knowledge_base_id=?", tenant, kb).Find(&pairs).Error; e != nil {
		return true, e
	}
	if len(pairs) == 0 {
		return nextcloudUnpairedCurrentKB(tx, tenant, kb)
	}
	for _, p := range pairs {
		closed, e := nextcloudControlsClosed(tx, p.InstanceID)
		if e != nil || closed {
			return true, e
		}
	}
	return false, nil
}

// NextcloudBuildControlsAllowed returns the nextcloud build controls allowed result.
// A reconciled scope is a private maintenance capability written only by the
// DB-owner Reconcile command after signed current source/complete-manifest proof.
// Only new non-retired candidates from that exact proof can rebuild while the
// personal read gate remains closed.
func NextcloudBuildControlsAllowed(
	ctx context.Context,
	tx *gorm.DB,
	tenant uint64,
	kb, ds string,
	knowledgeID ...string,
) (bool, error) {
	allowed, _, _, e := nextcloudRecoveryBuildProof(ctx, tx, tenant, kb, ds, knowledgeID...)
	return allowed, e
}

func nextcloudRecoveryBuildProof(
	ctx context.Context,
	tx *gorm.DB,
	tenant uint64,
	kb, ds string,
	knowledgeID ...string,
) (bool, *nextcloudRecoveryScope, int64, error) {
	if err := requireNextcloudTargetOpenInTx(tx, tenant, kb); err != nil {
		return false, nil, 0, err
	}
	if !tx.Migrator().HasTable(&NextcloudSourcePairing{}) {
		return !nextcloudCurrentControlModule(tx), nil, 0, nil
	}
	q := tx.WithContext(ctx).Where("tenant_id=? AND knowledge_base_id=?", tenant, kb)
	if ds != "" {
		q = q.Where("datasource_id=?", ds)
	}
	var pairs []NextcloudSourcePairing
	if e := q.Find(&pairs).Error; e != nil {
		return false, nil, 0, e
	}
	if len(pairs) == 0 {
		unknown, e := nextcloudUnpairedCurrentKB(tx, tenant, kb)
		return !unknown, nil, 0, e
	}
	if len(pairs) != 1 {
		return false, nil, 0, fmt.Errorf("%w: maintenance proof check 1", ErrNextcloudContentLeaseDenied)
	}
	pair := pairs[0]
	closed, e := nextcloudControlsClosed(tx, pair.InstanceID)
	if e != nil || !closed {
		return !closed, nil, 0, e
	}
	if !tx.Migrator().HasTable(&nextcloudControlRecoveryGate{}) || !tx.Migrator().HasTable(&nextcloudRecoveryScope{}) ||
		!tx.Migrator().HasTable(&nextcloudRecoveryBuildLease{}) {
		return false, nil, 0, fmt.Errorf("%w: maintenance proof check 2", ErrNextcloudContentLeaseDenied)
	}
	var gate nextcloudControlRecoveryGate
	if e := tx.Where("instance_id=? AND closed=?", pair.InstanceID, true).Take(&gate).Error; e != nil {
		return false, nil, 0, fmt.Errorf("%w: maintenance proof check 3", ErrNextcloudContentLeaseDenied)
	}
	var scope nextcloudRecoveryScope
	if e := tx.Where(("plan_sha256=? AND operation_id=? AND tenant_id=? AND knowledge_base_id=? A" +
		"ND datasource_id=? AND state='reconciled'"), gate.PlanSHA256, pair.OperationID, tenant, kb, pair.DataSourceID).
		Take(&scope).
		Error; e != nil {
		return false, nil, 0, fmt.Errorf("%w: maintenance proof check 4", ErrNextcloudContentLeaseDenied)
	}
	pairSHA, e := recoveryJSONSHA(recoveryPair(pair))
	if e != nil || pairSHA != scope.PairSHA256 {
		return false, nil, 0, fmt.Errorf("%w: maintenance proof check 5", ErrNextcloudContentLeaseDenied)
	}
	var sentinel nextcloudContentFence
	if e := tx.Where("tenant_id=? AND knowledge_base_id=? AND knowledge_id='' AND state='open'", tenant, kb).
		Take(&sentinel).
		Error; e != nil ||
		sentinel.Epoch <= scope.ClosedKBEpoch {
		return false, nil, 0, fmt.Errorf("%w: maintenance proof check 6", ErrNextcloudContentLeaseDenied)
	}
	var receipt NextcloudRecoveryReconciliationReceipt
	if recoveryStrictJSON([]byte(scope.ReconciliationJSON), &receipt) != nil || receipt.PlanSHA256 != gate.PlanSHA256 ||
		receipt.OperationID != pair.OperationID ||
		receipt.PairSHA256 != scope.PairSHA256 ||
		receipt.ReconciliationSHA256 != scope.ReconciliationSHA256 {
		return false, nil, 0, fmt.Errorf("%w: maintenance proof check 7", ErrNextcloudContentLeaseDenied)
	}
	if receipt.Manifest.Identity.InstanceID != pair.InstanceID ||
		receipt.Manifest.Identity.BindingID != pair.BindingID ||
		receipt.Manifest.Identity.PublicationState != "active" ||
		receipt.Manifest.Identity.PublicationEpoch <= pair.PublicationEpoch {
		return false, nil, 0, fmt.Errorf("%w: maintenance proof check 8", ErrNextcloudContentLeaseDenied)
	}
	if len(knowledgeID) > 0 && knowledgeID[0] != "" {
		var retired []nextcloudRecoveryRetiredGeneration
		if recoveryStrictJSON([]byte(scope.RetiredGenerationsJSON), &retired) != nil {
			return false, nil, 0, fmt.Errorf("%w: maintenance proof check 9", ErrNextcloudContentLeaseDenied)
		}
		for _, r := range retired {
			if r.KnowledgeID == knowledgeID[0] {
				return false, nil, 0, fmt.Errorf("%w: maintenance proof check 10", ErrNextcloudContentLeaseDenied)
			}
		}
		var k types.Knowledge
		if e := tx.Where(("tenant_id=? AND knowledge_base_id=? AND id=? AND channel=? AND deleted_at " +
			"IS NULL"), tenant, kb, knowledgeID[0], types.ConnectorTypeNextcloud).
			Take(&k).
			Error; e != nil {
			return false, nil, 0, fmt.Errorf("%w: maintenance proof check 11", ErrNextcloudContentLeaseDenied)
		}
		metadata, e := strictLineageKnowledgeMetadata(k.Metadata)
		if e != nil {
			return false, nil, 0, e
		}
		if metadata["datasource_id"] != pair.DataSourceID || metadata["nextcloud_instance_id"] != pair.InstanceID ||
			metadata["nextcloud_binding_id"] != pair.BindingID ||
			metadata["nextcloud_publication_epoch"] != strconv.FormatInt(
				receipt.Manifest.Identity.PublicationEpoch,
				10,
			) {
			return false, nil, 0, fmt.Errorf("%w: maintenance proof check 12", ErrNextcloudContentLeaseDenied)
		}
		found := false
		for _, f := range receipt.Manifest.Files {
			if metadata["nextcloud_file_id"] != strconv.FormatInt(f.FileID, 10) {
				continue
			}
			want, w := types.NormalizeNextcloudContentRevision(f.SourceContentRevision)
			actual, a := types.NormalizeNextcloudContentRevision(metadata["nextcloud_content_revision"])
			if w != nil || a != nil || want != actual || metadata["nextcloud_path"] != f.Path {
				return false, nil, 0, fmt.Errorf("%w: maintenance proof check 13", ErrNextcloudContentLeaseDenied)
			}
			var version nextcloudSourceVersion
			res := nextcloudVersionQuery(
				tx,
				tenant,
				kb,
				pair.DataSourceID,
				"nextcloud:"+pair.InstanceID+":"+metadata["nextcloud_file_id"],
			).Find(&version)
			if res.Error != nil {
				return false, nil, 0, res.Error
			}
			if res.RowsAffected == 0 && (k.ParseStatus != types.ParseStatusPending || k.EnableStatus == "enabled") {
				return false, nil, 0, fmt.Errorf("%w: maintenance proof check 14", ErrNextcloudContentLeaseDenied)
			}
			if res.RowsAffected > 0 && version.State != "tombstone" {
				desired, d := types.NormalizeNextcloudContentRevision(version.DesiredContentRevision)
				if d != nil || desired != want || version.CandidateKnowledgeID != k.ID ||
					version.DesiredETag != f.ETag ||
					(version.State != "staging" && version.State != "published") {
					return false, nil, 0, fmt.Errorf("%w: maintenance proof check 15", ErrNextcloudContentLeaseDenied)
				}
			}
			found = true
			break
		}
		if !found {
			return false, nil, 0, fmt.Errorf("%w: maintenance proof check 16", ErrNextcloudContentLeaseDenied)
		}
	}
	return true, &scope, sentinel.Epoch, nil
}

func nextcloudRecordRecoveryBuildLease(
	ctx context.Context,
	tx *gorm.DB,
	lease NextcloudContentLease,
	scope NextcloudContentScope,
) error {
	allowed, proof, kbEpoch, e := nextcloudRecoveryBuildProof(
		ctx,
		tx,
		scope.TenantID,
		scope.KnowledgeBaseID,
		scope.DataSourceID,
		scope.KnowledgeID,
	)
	if e != nil {
		return e
	}
	if !allowed {
		return ErrNextcloudContentLeaseDenied
	}
	if proof == nil {
		return nil
	}
	return tx.Create(
		&nextcloudRecoveryBuildLease{
			LeaseID:         lease.ID,
			PlanSHA256:      proof.PlanSHA256,
			OperationID:     proof.OperationID,
			TenantID:        scope.TenantID,
			KnowledgeBaseID: scope.KnowledgeBaseID,
			DataSourceID:    scope.DataSourceID,
			KnowledgeID:     scope.KnowledgeID,
			BuildEpoch:      lease.Epoch,
			KBEpoch:         kbEpoch,
			ManifestSHA256:  proof.ReconciliationSHA256,
			VerifiedAtMS:    time.Now().UnixMilli(),
		},
	).Error
}

func nextcloudValidateRecoveryBuildLease(
	ctx context.Context,
	tx *gorm.DB,
	lease NextcloudContentLease,
	scope NextcloudContentScope,
) error {
	allowed, proof, kbEpoch, e := nextcloudRecoveryBuildProof(
		ctx,
		tx,
		scope.TenantID,
		scope.KnowledgeBaseID,
		scope.DataSourceID,
		scope.KnowledgeID,
	)
	if e != nil {
		return e
	}
	if !allowed {
		return ErrNextcloudContentLeaseDenied
	}
	if proof == nil {
		return nil
	}
	var permit nextcloudRecoveryBuildLease
	if e := tx.Where(("lease_id=? AND plan_sha256=? AND operation_id=? AND tenant_id=? AND knowle" +
		"dge_base_id=? AND datasource_id=? AND knowledge_id=? AND build_epoch=? AND" +
		" kb_epoch=? AND manifest_sha256=?"), lease.ID, proof.PlanSHA256, proof.OperationID, scope.TenantID, scope.KnowledgeBaseID, scope.DataSourceID, scope.KnowledgeID, lease.Epoch, kbEpoch, proof.ReconciliationSHA256).
		Take(&permit).
		Error; e != nil {
		return ErrNextcloudContentLeaseDenied
	}
	return nil
}

func nextcloudAllowRecoveryBuildRead(ctx context.Context, tx *gorm.DB, tenant uint64, kb, kid string) (bool, error) {
	guard, ok := ctx.Value(nextcloudBuildContextKey{}).(nextcloudBuildContext)
	if !ok || guard.admissionCtx == nil || guard.admissionCtx.Err() != nil ||
		guard.lease.Kind != NextcloudContentBuildLease ||
		guard.scope.TenantID != tenant ||
		guard.scope.KnowledgeBaseID != kb ||
		guard.scope.KnowledgeID != kid {
		return false, nil
	}
	var live int64
	if e := tx.Table("nextcloud_content_leases").
		Where(("lease_id=? AND tenant_id=? AND knowledge_base_id=? AND knowledge_id=? AND " +
			"kind='build' AND epoch=? AND owner_id=? AND released_at_ms IS NULL AND exp" +
			"ires_at_ms>?"), guard.lease.ID, tenant, kb, kid, guard.lease.Epoch, guard.lease.OwnerID, time.Now().UnixMilli()).
		Count(&live).
		Error; e != nil {
		return false, e
	}
	if live != 1 {
		return false, nil
	}
	_, proof, _, e := nextcloudRecoveryBuildProof(ctx, tx, tenant, kb, guard.scope.DataSourceID, kid)
	if e != nil || proof == nil {
		return false, e
	}
	if e := nextcloudValidateRecoveryBuildLease(ctx, tx, guard.lease, guard.scope); e != nil {
		return false, e
	}
	return true, nil
}

// AllowRecoveryBuildRead allows recovery build read.
func (r *DataSourceRepository) AllowRecoveryBuildRead(
	ctx context.Context,
	tenant uint64,
	kb, kid string,
) (bool, error) {
	return nextcloudAllowRecoveryBuildRead(ctx, r.db.WithContext(ctx), tenant, kb, kid)
}
