package repository

import (
	"encoding/json"
	"errors"
	"math"
	"strconv"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// ErrNextcloudRecoveryUnknownOperation reports a nextcloud recovery unknown operation condition.
// Missing restored provenance never means the independently retained deletion
// may be ignored. Apply fails closed and persists its existing global gate.
var ErrNextcloudRecoveryUnknownOperation = errors.New("nextcloud recovery permanent fact has no local operation")

// NextcloudRecoveryTenantDeletion represents nextcloud recovery tenant deletion repository data.
type NextcloudRecoveryTenantDeletion struct {
	Tenant       types.NextcloudTenantDeletionObservation `json:"tenant"`
	OriginSHA256 string                                   `json:"origin_sha256"`
}

func tenantDeletionJournalLines(proof NextcloudRecoveryTenantDeletion) []string {
	t := proof.Tenant
	return []string{
		proof.OriginSHA256,
		t.Schema,
		t.State,
		t.OperationID,
		t.InstanceID,
		t.BindingID,
		t.TenantID,
		t.DeletionID,
		t.DeletionTimeMS,
		t.ClosedAt,
	}
}

func validateRecoveryTenantDeletion(raw json.RawMessage, instance string) (NextcloudRecoveryTenantDeletion, error) {
	var proof NextcloudRecoveryTenantDeletion
	var fields map[string]json.RawMessage
	if recoveryStrictJSON(raw, &proof) != nil || json.Unmarshal(raw, &fields) != nil || len(fields) != 2 {
		return proof, ErrNextcloudRecoveryDenied
	}
	var tenant map[string]json.RawMessage
	if json.Unmarshal(fields["tenant"], &tenant) != nil || len(tenant) != 9 {
		return proof, ErrNextcloudRecoveryDenied
	}
	for _, name := range []string{"schema", "state", "operation_id", "instance_id", "binding_id", "tenant_id", "deletion_id", "deletion_time_ms", "closed_at"} {
		var value string
		if json.Unmarshal(tenant[name], &value) != nil {
			return proof, ErrNextcloudRecoveryDenied
		}
	}
	t := proof.Tenant
	owner, te := canonicalPositiveTargetNumber(t.TenantID)
	deletion, de := canonicalPositiveTargetNumber(t.DeletionTimeMS)
	closed, ce := canonicalPositiveTargetNumber(t.ClosedAt)
	if te != nil || de != nil || ce != nil || closed > math.MaxInt64/1000 || closed < deletion/1000 ||
		closed > time.Now().Unix()+300 ||
		t.Schema != "weknora-nextcloud-tenant-deletion-v1" ||
		t.State != "tenant_deleted" ||
		t.InstanceID != instance ||
		!recoveryStream.MatchString(t.OperationID) ||
		!recoveryID.MatchString(t.BindingID) ||
		!recoveryHash.MatchString(proof.OriginSHA256) ||
		t.DeletionID != tenantDeletionID(uint64(owner), deletion) {
		return proof, ErrNextcloudRecoveryDenied
	}
	return proof, nil
}

func validateRecoveryTenantDeletions(fields map[string]json.RawMessage, p *NextcloudRecoveryPlan) error {
	var raws []json.RawMessage
	if raw, ok := fields["tenant_deletions"]; ok {
		if json.Unmarshal(raw, &raws) != nil || len(raws) > 10000 || len(raws) != len(p.TenantDeletions) {
			return ErrNextcloudRecoveryDenied
		}
	}
	seen := map[string]NextcloudRecoveryTenantDeletion{}
	lastOrigin := ""
	lastOwner := int64(0)
	for i, raw := range raws {
		proof, err := validateRecoveryTenantDeletion(raw, p.InstanceID)
		if err != nil {
			return err
		}
		owner, _ := canonicalPositiveTargetNumber(proof.Tenant.TenantID)
		if proof.OriginSHA256 < lastOrigin || (proof.OriginSHA256 == lastOrigin && owner <= lastOwner) {
			return ErrNextcloudRecoveryDenied
		}
		key := proof.OriginSHA256 + "\x00" + proof.Tenant.TenantID
		if _, exists := seen[key]; exists {
			return ErrNextcloudRecoveryDenied
		}
		lastOrigin, lastOwner = proof.OriginSHA256, owner
		seen[key] = proof
		p.TenantDeletions[i] = proof
	}
	for _, event := range p.JournalEvents {
		if event.Kind != "tenant_deleted" {
			if event.TenantDeletion != nil {
				return ErrNextcloudRecoveryDenied
			}
			continue
		}
		if event.TenantDeletion == nil || event.TargetClosure != nil || event.FileID != nil ||
			event.SourceRevision != "0" ||
			event.BindingID != event.TenantDeletion.Tenant.BindingID {
			return ErrNextcloudRecoveryDenied
		}
		proof, exists := seen[event.TenantDeletion.OriginSHA256+"\x00"+event.TenantDeletion.Tenant.TenantID]
		if !exists || proof != *event.TenantDeletion {
			return ErrNextcloudRecoveryDenied
		}
	}
	// A target receipt that names Tenant deletion must carry the independent
	// owner fact as well. Otherwise a restore could block only its old KB and
	// admit a fresh KB UUID in the same positively deleted namespace.
	for _, target := range p.TargetClosures {
		if target.Target.Reason != "tenant_deleted" {
			continue
		}
		covered := false
		for _, proof := range p.TenantDeletions {
			if proof.Tenant.TenantID == target.Target.TenantID && proof.Tenant.InstanceID == target.Target.InstanceID &&
				proof.Tenant.DeletionTimeMS == target.Target.DeletionTimeMS {
				covered = true
				break
			}
		}
		if !covered {
			return ErrNextcloudRecoveryDenied
		}
	}
	return nil
}

// Verify every immutable fact's local authority before any recovery mutation.
// This also covers idempotent receipt replay; a receipt cannot mask lost facts.
func requireRecoveryPermanentFactOperationsInTx(tx *gorm.DB, p *NextcloudRecoveryPlan) error {
	if (len(p.TargetClosures) > 0 || len(p.TenantDeletions) > 0) &&
		!tx.Migrator().HasTable("nextcloud_onboarding_operations") {
		return ErrNextcloudRecoveryUnknownOperation
	}
	for _, proof := range p.TargetClosures {
		var op types.NextcloudOnboardingOperation
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("operation_id=?", proof.Target.OperationID).
			Take(&op).
			Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return ErrNextcloudRecoveryUnknownOperation
			}
			return err
		}
		if strconv.FormatUint(op.TenantID, 10) != proof.Target.TenantID || op.InstanceID != proof.Target.InstanceID ||
			op.BindingID != proof.Target.BindingID ||
			op.KnowledgeBaseID != proof.Target.KnowledgeBaseID {
			return ErrNextcloudRecoveryDenied
		}
	}
	for _, proof := range p.TenantDeletions {
		var op types.NextcloudOnboardingOperation
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("operation_id=?", proof.Tenant.OperationID).
			Take(&op).
			Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return ErrNextcloudRecoveryUnknownOperation
			}
			return err
		}
		if strconv.FormatUint(op.TenantID, 10) != proof.Tenant.TenantID || op.InstanceID != proof.Tenant.InstanceID ||
			op.BindingID != proof.Tenant.BindingID {
			return ErrNextcloudRecoveryDenied
		}
	}
	return nil
}

func applyRecoveryTenantDeletionsInTx(tx *gorm.DB, p *NextcloudRecoveryPlan) error {
	if len(p.TenantDeletions) == 0 {
		return nil
	}
	if !tx.Migrator().HasTable(&NextcloudTenantDeletion{}) {
		return ErrNextcloudRecoveryDenied
	}
	for _, proof := range p.TenantDeletions {
		t := proof.Tenant
		owner, _ := canonicalPositiveTargetNumber(t.TenantID)
		deletion, _ := canonicalPositiveTargetNumber(t.DeletionTimeMS)
		closed, _ := canonicalPositiveTargetNumber(t.ClosedAt)
		var prior NextcloudTenantDeletion
		err := tx.Where("tenant_id=?", owner).Take(&prior).Error
		if err == nil {
			if !validTenantDeletion(prior) || prior.DeletionID != t.DeletionID || prior.DeletionTimeMS != deletion ||
				prior.ClosedAtMS/1000 != closed {
				return ErrNextcloudRecoveryDenied
			}
			continue
		}
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}
		closedMS := closed * 1000
		if closedMS < deletion {
			closedMS = deletion
		}
		// This restores the permanent automation fact, not a guessed Tenant delete,
		// manual ACL change or migrated root target closure. Native gates consume it.
		if err = tx.Create(
			&NextcloudTenantDeletion{
				TenantID:           uint64(owner),
				DeletionID:         t.DeletionID,
				DeletionTimeMS:     deletion,
				ClosedAtMS:         closedMS,
				RecoveryPlanSHA256: p.digest,
			},
		).Error; err != nil {
			return err
		}
	}
	return nil
}

func requireRecoveryPermanentFactsRetainedInTx(tx *gorm.DB, p *NextcloudRecoveryPlan) error {
	for _, proof := range p.TargetClosures {
		var row NextcloudTargetClosure
		if tx.Where("operation_id=?", proof.Target.OperationID).Take(&row).Error != nil ||
			row.ClosureID != proof.Target.ClosureID ||
			strconv.FormatInt(row.ClosedAtMS/1000, 10) != proof.Target.ClosedAt {
			return ErrNextcloudRecoveryDenied
		}
	}
	for _, proof := range p.TenantDeletions {
		var row NextcloudTenantDeletion
		if tx.Where("tenant_id=?", proof.Tenant.TenantID).Take(&row).Error != nil || !validTenantDeletion(row) ||
			row.DeletionID != proof.Tenant.DeletionID ||
			strconv.FormatInt(row.ClosedAtMS/1000, 10) != proof.Tenant.ClosedAt {
			return ErrNextcloudRecoveryDenied
		}
	}
	return nil
}
