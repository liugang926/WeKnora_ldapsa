package repository

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"regexp"
	"strconv"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// These facts are retained by the existing independent HMAC journal. This port
// grants no source/content access, and never purges or reconstructs any target.
type NextcloudRecoveryTargetClosure struct {
	Target     types.NextcloudTargetObservation `json:"target"`
	RootFileID string                           `json:"root_file_id"`
	RootKey    string                           `json:"root_key"`
}

var targetClosureKnowledgeBaseID = regexp.MustCompile(`^[A-Za-z0-9._-]{1,128}$`)

func targetClosureJournalLines(proof NextcloudRecoveryTargetClosure) []string {
	t := proof.Target
	return []string{
		proof.RootFileID,
		proof.RootKey,
		t.Schema,
		t.State,
		t.Reason,
		t.OperationID,
		t.InstanceID,
		t.BindingID,
		t.TenantID,
		t.KnowledgeBaseID,
		t.OperationRevision,
		t.ClosureID,
		t.DeletionTimeMS,
		t.ClosedAt,
	}
}

func canonicalPositiveTargetNumber(v string) (int64, error) {
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil || n <= 0 || strconv.FormatInt(n, 10) != v {
		return 0, ErrNextcloudRecoveryDenied
	}
	return n, nil
}

func validateRecoveryTargetClosure(raw json.RawMessage, instance string) (NextcloudRecoveryTargetClosure, error) {
	var proof NextcloudRecoveryTargetClosure
	var object map[string]json.RawMessage
	if recoveryStrictJSON(raw, &proof) != nil || json.Unmarshal(raw, &object) != nil || len(object) != 3 {
		return proof, ErrNextcloudRecoveryDenied
	}
	var target map[string]json.RawMessage
	if json.Unmarshal(object["target"], &target) != nil || len(target) != 12 {
		return proof, ErrNextcloudRecoveryDenied
	}
	for _, name := range []string{"schema", "state", "reason", "operation_id", "instance_id", "binding_id", "tenant_id", "knowledge_base_id", "operation_revision", "closure_id", "deletion_time_ms", "closed_at"} {
		var value string
		if json.Unmarshal(target[name], &value) != nil {
			return proof, ErrNextcloudRecoveryDenied
		}
	}
	t := proof.Target
	tenant, te := canonicalPositiveTargetNumber(t.TenantID)
	revision, re := canonicalPositiveTargetNumber(t.OperationRevision)
	deletion, de := canonicalPositiveTargetNumber(t.DeletionTimeMS)
	closed, ce := canonicalPositiveTargetNumber(t.ClosedAt)
	_, fe := canonicalPositiveTargetNumber(proof.RootFileID)
	if te != nil || re != nil || de != nil || ce != nil || fe != nil || closed > math.MaxInt64/1000 ||
		closed < deletion/1000 ||
		closed > time.Now().Unix()+300 ||
		t.Schema != "weknora-nextcloud-target-v1" ||
		t.State != "target_deleted" ||
		t.InstanceID != instance ||
		!recoveryStream.MatchString(t.OperationID) ||
		!recoveryID.MatchString(t.BindingID) ||
		!targetClosureKnowledgeBaseID.MatchString(t.KnowledgeBaseID) ||
		(t.Reason != "tenant_deleted" && t.Reason != "knowledge_base_deleted") ||
		!recoveryHash.MatchString(t.ClosureID) ||
		proof.RootKey != recoverySHA([]byte("nextcloud-filecache-root-v1\x00"+proof.RootFileID)) {
		return proof, ErrNextcloudRecoveryDenied
	}
	row := NextcloudTargetClosure{
		OperationID:       t.OperationID,
		TenantID:          uint64(tenant),
		KnowledgeBaseID:   t.KnowledgeBaseID,
		InstanceID:        t.InstanceID,
		BindingID:         t.BindingID,
		OperationRevision: uint64(revision),
		Reason:            t.Reason,
		DeletionTimeMS:    deletion,
	}
	if targetClosureID(row) != t.ClosureID {
		return proof, ErrNextcloudRecoveryDenied
	}
	return proof, nil
}

func validateRecoveryTargetClosures(fields map[string]json.RawMessage, p *NextcloudRecoveryPlan) error {
	var raws []json.RawMessage
	if raw, ok := fields["target_closures"]; ok {
		if json.Unmarshal(raw, &raws) != nil || len(raws) > 10000 || len(raws) != len(p.TargetClosures) {
			return ErrNextcloudRecoveryDenied
		}
	}
	seenOps, seenRoots, seenScopes := map[string]bool{}, map[string]bool{}, map[string]bool{}
	last := ""
	for i, raw := range raws {
		proof, err := validateRecoveryTargetClosure(raw, p.InstanceID)
		if err != nil {
			return err
		}
		t := proof.Target
		scope := t.TenantID + "\x00" + t.KnowledgeBaseID
		if t.BindingID <= last || seenOps[t.OperationID] || seenRoots[proof.RootFileID] || seenScopes[scope] {
			return ErrNextcloudRecoveryDenied
		}
		last = t.BindingID
		seenOps[t.OperationID] = true
		seenRoots[proof.RootFileID] = true
		seenScopes[scope] = true
		p.TargetClosures[i] = proof
	}
	for _, event := range p.JournalEvents {
		if event.Kind != "target_deleted" {
			if event.TargetClosure != nil {
				return ErrNextcloudRecoveryDenied
			}
			continue
		}
		if event.TargetClosure == nil || event.FileID != nil ||
			event.BindingID != event.TargetClosure.Target.BindingID ||
			event.SourceRevision != event.TargetClosure.Target.OperationRevision ||
			!seenOps[event.TargetClosure.Target.OperationID] {
			return ErrNextcloudRecoveryDenied
		}
		found := false
		for _, proof := range p.TargetClosures {
			if proof == *event.TargetClosure {
				found = true
				break
			}
		}
		if !found {
			return ErrNextcloudRecoveryDenied
		}
	}
	return nil
}

// The actual signed plan and pinned restored inventory have already matched
// before this is called. A current restored row is not proof of resurrection.
func applyRecoveryTargetClosuresInTx(ctx context.Context, tx *gorm.DB, p *NextcloudRecoveryPlan) error {
	if len(p.TargetClosures) == 0 {
		return nil
	}
	if !tx.Migrator().HasTable(&NextcloudTargetClosure{}) {
		return ErrNextcloudRecoveryDenied
	}
	for _, proof := range p.TargetClosures {
		t := proof.Target
		var op types.NextcloudOnboardingOperation
		err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("operation_id=?", t.OperationID).Take(&op).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return ErrNextcloudRecoveryUnknownOperation
		}
		if err != nil {
			return err
		}
		tenant, _ := canonicalPositiveTargetNumber(t.TenantID)
		if op.TenantID != uint64(tenant) || op.KnowledgeBaseID != t.KnowledgeBaseID || op.InstanceID != t.InstanceID ||
			op.BindingID != t.BindingID {
			return ErrNextcloudRecoveryDenied
		}
		var existing NextcloudTargetClosure
		err = tx.Where("operation_id=?", op.OperationID).Take(&existing).Error
		if err == nil {
			if !validTargetClosure(existing, op) || existing.ClosureID != t.ClosureID ||
				strconv.FormatInt(existing.ClosedAtMS/1000, 10) != t.ClosedAt {
				return ErrNextcloudRecoveryDenied
			}
			continue
		}
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}
		revision, _ := canonicalPositiveTargetNumber(t.OperationRevision)
		if op.PermissionVersion >= math.MaxInt64 {
			return ErrNextcloudRecoveryDenied
		}
		newRevision := op.Revision
		if uint64(revision) > newRevision {
			newRevision = uint64(revision)
		}
		q := tx.Model(&types.NextcloudOnboardingOperation{}).
			Where("operation_id=? AND revision=?", op.OperationID, op.Revision).
			Updates(map[string]any{"revision": newRevision, "permission_version": op.PermissionVersion + 1, "permission_synced_at": nil, "updated_at": time.Now().UTC()})
		if q.Error != nil {
			return q.Error
		}
		if q.RowsAffected != 1 {
			return ErrNextcloudRecoveryDenied
		}
		if _, err = bumpPermissionVersion(tx, op.TenantID); err != nil {
			return err
		}
		if err = tx.Where("operation_id=?", op.OperationID).Delete(&types.NextcloudSourceGrant{}).Error; err != nil {
			return err
		}
		if err = NewNextcloudContentLeaseStore(tx).RetireKBInTx(ctx, tx, op.TenantID, op.KnowledgeBaseID); err != nil {
			return err
		}
		if tx.Migrator().HasTable("nextcloud_event_connections") {
			if err = tx.Table("nextcloud_event_connections").
				Where("tenant_id=? AND knowledge_base_id=? AND nextcloud_instance_id=? AND binding_id=?", op.TenantID, op.KnowledgeBaseID, op.InstanceID, op.BindingID).
				Update("status", "revoked").
				Error; err != nil {
				return err
			}
		}
		if tx.Migrator().HasTable("data_sources") && tx.Migrator().HasTable("nextcloud_source_pairings") {
			ids := tx.Table("nextcloud_source_pairings").
				Select("datasource_id").
				Where("operation_id=? AND tenant_id=? AND knowledge_base_id=?", op.OperationID, op.TenantID, op.KnowledgeBaseID)
			if err = tx.Table("data_sources").
				Where("id IN (?) AND tenant_id=? AND knowledge_base_id=?", ids, op.TenantID, op.KnowledgeBaseID).
				Updates(map[string]any{"status": types.DataSourceStatusPaused, "sync_enabled": false}).
				Error; err != nil {
				return err
			}
		}
		deletion, _ := canonicalPositiveTargetNumber(t.DeletionTimeMS)
		closed, _ := canonicalPositiveTargetNumber(t.ClosedAt)
		closedMS := closed * 1000
		if closedMS < deletion {
			closedMS = deletion
		}
		row := NextcloudTargetClosure{
			OperationID:        op.OperationID,
			ClosureID:          t.ClosureID,
			TenantID:           op.TenantID,
			KnowledgeBaseID:    op.KnowledgeBaseID,
			InstanceID:         op.InstanceID,
			BindingID:          op.BindingID,
			OperationRevision:  uint64(revision),
			Reason:             t.Reason,
			DeletionTimeMS:     deletion,
			ClosedAtMS:         closedMS,
			RecoveryPlanSHA256: p.digest,
		}
		if err = tx.Create(&row).Error; err != nil {
			return err
		}
	}
	return nil
}
