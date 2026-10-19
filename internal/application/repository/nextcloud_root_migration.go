package repository

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/Tencent/WeKnora/internal/datasource/connector/nextcloud"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// NextcloudRootMigration represents nextcloud root migration repository data.
type NextcloudRootMigration struct {
	OperationID       string `gorm:"primaryKey"`
	PairOperationID   string
	TenantID          uint64
	KnowledgeBaseID   string
	DataSourceID      string `gorm:"column:datasource_id"`
	ProofJSON         string
	ProofSHA256       string
	State             string
	SyncWasEnabled    bool
	OperationRevision uint64
	CreatedAt         time.Time
	UpdatedAt         time.Time
}

// TableName returns the persisted table name.
func (NextcloudRootMigration) TableName() string { return "nextcloud_root_migrations" }

func rootProofDigest(proof nextcloud.RootMigration) string {
	proof.State = ""
	return onboardingDigest(proof)
}

// PrepareRootMigration prepares root migration.
func (s *NextcloudOnboardingStore) PrepareRootMigration(
	ctx context.Context,
	op types.NextcloudOnboardingOperation,
	proof nextcloud.RootMigration,
) (NextcloudRootMigration, error) {
	var out NextcloudRootMigration
	if _, err := uuid.Parse(proof.OperationID); err != nil {
		return out, ErrNextcloudOnboardingConflict
	}
	if proof.PairOperationID != op.OperationID || proof.BindingID != op.BindingID ||
		proof.InstanceID != op.InstanceID ||
		proof.TenantID != strconv.FormatUint(op.TenantID, 10) ||
		proof.KnowledgeBaseID != op.KnowledgeBaseID ||
		!recoveryHash.MatchString(proof.PhysicalRootKey) ||
		!recoveryHash.MatchString(proof.StorageIdentity) ||
		!recoveryHash.MatchString(proof.OldRootHash) ||
		!recoveryHash.MatchString(proof.NewRootHash) ||
		!recoveryHash.MatchString(proof.JournalChainSHA256) {
		return out, ErrNextcloudOnboardingConflict
	}
	root, err := strconv.ParseInt(proof.RootFileID, 10, 64)
	if err != nil || root < 1 {
		return out, ErrNextcloudOnboardingConflict
	}
	seq, err := strconv.ParseInt(proof.JournalSequence, 10, 64)
	if err != nil || seq < 1 {
		return out, ErrNextcloudOnboardingConflict
	}
	if _, err = uuid.Parse(proof.JournalStreamID); err != nil {
		return out, ErrNextcloudOnboardingConflict
	}
	old, err := strconv.ParseInt(proof.OldEpoch, 10, 64)
	if err != nil || old < 0 {
		return out, ErrNextcloudOnboardingConflict
	}
	prepared, err := strconv.ParseInt(proof.PreparedEpoch, 10, 64)
	if err != nil || prepared < old || prepared > old+1 {
		return out, ErrNextcloudOnboardingConflict
	}
	resume, err := strconv.ParseInt(proof.ResumeEpoch, 10, 64)
	if err != nil || resume < prepared || resume > prepared+1 {
		return out, ErrNextcloudOnboardingConflict
	}

	if !validRootMigrationPath(proof.OldRootPath) || !validRootMigrationPath(proof.NewRootPath) {
		return out, ErrNextcloudOnboardingConflict
	}
	err = s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := requireNextcloudTargetOpenInTx(tx, op.TenantID, op.KnowledgeBaseID); err != nil {
			return err
		}
		q := tx.Exec(
			("UPDATE knowledge_bases SET ever_had_nextcloud_source=ever_had_nextcloud_so" +
				"urce WHERE tenant_id=? AND id=? AND deleted_at IS NULL"),
			op.TenantID,
			op.KnowledgeBaseID,
		)
		if q.Error != nil || q.RowsAffected != 1 {
			return ErrNextcloudOnboardingConflict
		}
		var pair NextcloudSourcePairing
		if err := tx.Where("operation_id=? AND tenant_id=? AND state='active'", op.OperationID, op.TenantID).
			Take(&pair).
			Error; err != nil || pair.DataSourceID != proof.DataSourceID ||
			pair.KnowledgeBaseID != op.KnowledgeBaseID {
			return ErrNextcloudOnboardingConflict
		}
		e := tx.Where("operation_id=?", proof.OperationID).Take(&out).Error
		if e == nil {
			if out.ProofSHA256 != rootProofDigest(proof) || out.PairOperationID != op.OperationID {
				return ErrNextcloudOnboardingConflict
			}
			return nil
		}
		if !errors.Is(e, gorm.ErrRecordNotFound) {
			return e
		}
		var pending int64
		if err := tx.Model(&NextcloudRootMigration{}).
			Where("pair_operation_id=? AND state='prepared'", op.OperationID).
			Count(&pending).
			Error; err != nil ||
			pending != 0 {
			return ErrNextcloudOnboardingConflict
		}
		var history []NextcloudRootMigration
		if err := tx.Where("pair_operation_id=? AND state='finalized'", op.OperationID).
			Find(&history).
			Error; err != nil {
			return err
		}
		var latest *nextcloud.RootMigration
		var latestSeq int64
		for _, row := range history {
			var previous nextcloud.RootMigration
			if json.Unmarshal([]byte(row.ProofJSON), &previous) != nil || rootProofDigest(previous) != row.ProofSHA256 {
				return ErrNextcloudOnboardingConflict
			}
			n, err := strconv.ParseInt(previous.JournalSequence, 10, 64)
			if err != nil {
				return ErrNextcloudOnboardingConflict
			}
			if n > latestSeq {
				copied := previous
				latest = &copied
				latestSeq = n
			}
		}
		if latest != nil &&
			(latest.JournalStreamID != proof.JournalStreamID || latestSeq >= seq || latest.PhysicalRootKey != proof.PhysicalRootKey || latest.StorageIdentity != proof.StorageIdentity || latest.NewRootHash != proof.OldRootHash || latest.NewRootPath != proof.OldRootPath) {
			return ErrNextcloudOnboardingConflict
		}
		var ds types.DataSource
		if err := tx.Where("id=? AND tenant_id=? AND deleted_at IS NULL", pair.DataSourceID, pair.TenantID).
			Take(&ds).
			Error; err != nil {
			return err
		}
		// Keep immutable body/graph fence epochs and receipts. The unique old lease
		// IDs are permanently closed, so late workers cannot use them after ACK.
		if tx.Migrator().HasTable("nextcloud_content_fences") {
			var fences []nextcloudContentFence
			if err := tx.Where("tenant_id=? AND knowledge_base_id=?", op.TenantID, op.KnowledgeBaseID).
				Order("knowledge_id").
				Clauses(clause.Locking{Strength: "UPDATE"}).
				Find(&fences).
				Error; err != nil {
				return err
			}
			for _, f := range fences {
				if f.State == "deleting" {
					return ErrNextcloudOnboardingConflict
				}
			}
			now, err := nextcloudLeaseNowMS(tx)
			if err != nil {
				return err
			}
			if err = tx.Table("nextcloud_content_leases").
				Where("tenant_id=? AND knowledge_base_id=? AND released_at_ms IS NULL", op.TenantID, op.KnowledgeBaseID).
				Updates(map[string]any{"released_at_ms": now, "updated_at_ms": now}).
				Error; err != nil {
				return err
			}
		}
		if tx.Migrator().HasTable("nextcloud_graph_tasks") {
			if err := tx.Table("nextcloud_graph_tasks").
				Where("tenant_id=? AND knowledge_base_id=? AND state IN ?", op.TenantID, op.KnowledgeBaseID, []string{"pending", "running"}).
				Updates(map[string]any{"state": "cancelled", "error_code": "root_migration", "completed_at": time.Now().Unix()}).
				Error; err != nil {
				return err
			}
		}
		enabled := ds.SyncEnabled == nil || *ds.SyncEnabled
		if err := tx.Table("data_sources").
			Where("id=? AND tenant_id=?", ds.ID, ds.TenantID).
			Update("sync_enabled", false).
			Error; err != nil {
			return err
		}
		raw, _ := json.Marshal(proof)
		out = NextcloudRootMigration{
			OperationID:       proof.OperationID,
			PairOperationID:   op.OperationID,
			TenantID:          op.TenantID,
			KnowledgeBaseID:   op.KnowledgeBaseID,
			DataSourceID:      pair.DataSourceID,
			ProofJSON:         string(raw),
			ProofSHA256:       rootProofDigest(proof),
			State:             "prepared",
			SyncWasEnabled:    enabled,
			OperationRevision: op.Revision,
			CreatedAt:         time.Now().UTC(),
			UpdatedAt:         time.Now().UTC(),
		}
		return tx.Create(&out).Error
	})
	return out, err
}

// FinalizeRootMigration finalizes root migration.
func (s *NextcloudOnboardingStore) FinalizeRootMigration(
	ctx context.Context,
	op types.NextcloudOnboardingOperation,
	row NextcloudRootMigration,
	proof nextcloud.RootMigration,
) error {
	if proof.State != "finalized" || rootProofDigest(proof) != row.ProofSHA256 {
		return ErrNextcloudOnboardingConflict
	}
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := requireNextcloudTargetOpenInTx(tx, op.TenantID, op.KnowledgeBaseID); err != nil {
			return err
		}
		q := tx.Exec(
			"UPDATE knowledge_bases SET ever_had_nextcloud_source=ever_had_nextcloud_source WHERE tenant_id=? AND id=?",
			op.TenantID,
			op.KnowledgeBaseID,
		)
		if q.Error != nil || q.RowsAffected != 1 {
			return ErrNextcloudOnboardingConflict
		}
		var current NextcloudRootMigration
		if err := tx.Where("operation_id=?", row.OperationID).Take(&current).Error; err != nil {
			return err
		}
		if current.ProofSHA256 != row.ProofSHA256 {
			return ErrNextcloudOnboardingConflict
		}
		if current.State == "finalized" {
			return nil
		}
		var operation types.NextcloudOnboardingOperation
		if err := tx.Where("operation_id=?", op.OperationID).Take(&operation).Error; err != nil {
			return err
		}
		if current.SyncWasEnabled && operation.Revision == current.OperationRevision {
			if err := tx.Table("data_sources").
				Where("id=? AND tenant_id=?", current.DataSourceID, current.TenantID).
				Update("sync_enabled", true).
				Error; err != nil {
				return err
			}
		}
		if err := tx.Model(&current).
			Updates(map[string]any{"state": "finalized", "updated_at": time.Now().UTC()}).
			Error; err != nil {
			return err
		}
		// A signed exact final ACK records the new NC publication authority. It does
		// not rewrite any successful graph receipt or immutable content generation.
		if tx.Migrator().HasTable("nextcloud_source_status_epochs") {
			epoch, _ := strconv.ParseInt(proof.ResumeEpoch, 10, 64)
			var ids []string
			if err := tx.Table("nextcloud_event_connections").
				Where("tenant_id=? AND datasource_id=? AND status='active'", op.TenantID, row.DataSourceID).
				Pluck("connection_id", &ids).
				Error; err != nil {
				return err
			}
			for _, id := range ids {
				if err := tx.Table("nextcloud_source_status_epochs").
					Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "connection_id"}}, DoUpdates: clause.Assignments(map[string]any{"publication_epoch": epoch, "pair_operation_id": op.OperationID, "verified_at": time.Now().Unix()}), Where: clause.Where{Exprs: []clause.Expression{clause.Expr{SQL: ("nextcloud_source_status_epochs.publication_epoch <= excluded.publication_e" +
						"poch")}}}}).
					Create(map[string]any{"connection_id": id, "publication_epoch": epoch, "pair_operation_id": op.OperationID, "verified_at": time.Now().Unix()}).
					Error; err != nil {
					return err
				}
			}
		}
		return nil
	})
}

func validRootMigrationPath(path string) bool {
	if !strings.HasPrefix(path, "/") || len(path) > 4096 || strings.ContainsAny(path, "\x00\r\n") {
		return false
	}
	for _, part := range strings.Split(strings.Trim(path, "/"), "/") {
		if part == "" || part == "." || part == ".." {
			return false
		}
	}
	return true
}

func (s *NextcloudOnboardingStore) finalizedRootPathChain(
	ctx context.Context,
	tenant uint64,
	kb, ds string,
) ([]nextcloud.RootMigration, error) {
	if !s.db.Migrator().HasTable(&NextcloudRootMigration{}) {
		return nil, nil
	}
	var rows []NextcloudRootMigration
	if err := s.db.WithContext(ctx).
		Where("tenant_id=? AND knowledge_base_id=? AND datasource_id=? AND state='finalized'", tenant, kb, ds).
		Find(&rows).
		Error; err != nil {
		return nil, err
	}
	proofs := []nextcloud.RootMigration{}
	for _, r := range rows {
		var p nextcloud.RootMigration
		if json.Unmarshal([]byte(r.ProofJSON), &p) != nil || rootProofDigest(p) != r.ProofSHA256 ||
			p.OperationID != r.OperationID ||
			p.PairOperationID != r.PairOperationID ||
			p.KnowledgeBaseID != kb ||
			p.DataSourceID != ds ||
			p.TenantID != strconv.FormatUint(tenant, 10) ||
			!validRootMigrationPath(p.OldRootPath) ||
			!validRootMigrationPath(p.NewRootPath) {
			return nil, ErrNextcloudOnboardingConflict
		}
		proofs = append(proofs, p)
	}
	sort.Slice(proofs, func(i, j int) bool {
		a, _ := strconv.ParseInt(proofs[i].JournalSequence, 10, 64)
		b, _ := strconv.ParseInt(proofs[j].JournalSequence, 10, 64)
		return a < b
	})
	for i := 1; i < len(proofs); i++ {
		a, b := proofs[i-1], proofs[i]
		sa, _ := strconv.ParseInt(a.JournalSequence, 10, 64)
		sb, _ := strconv.ParseInt(b.JournalSequence, 10, 64)
		if a.PairOperationID != b.PairOperationID || a.JournalStreamID != b.JournalStreamID || sa >= sb ||
			a.PhysicalRootKey != b.PhysicalRootKey ||
			a.StorageIdentity != b.StorageIdentity ||
			a.NewRootHash != b.OldRootHash ||
			a.NewRootPath != b.OldRootPath {
			return nil, ErrNextcloudOnboardingConflict
		}
	}
	return proofs, nil
}

// ResolveCurrentSourcePath resolves current source path.
func (s *NextcloudOnboardingStore) ResolveCurrentSourcePath(
	ctx context.Context,
	tenant uint64,
	kb, ds, oldPath string,
) (string, error) {
	proofs, err := s.finalizedRootPathChain(ctx, tenant, kb, ds)
	if err != nil {
		return "", err
	}
	if oldPath != "" && !strings.HasPrefix(oldPath, "/") {
		if strings.ContainsAny(oldPath, "\x00\r\n\\") {
			return "", ErrNextcloudOnboardingConflict
		}
		for _, part := range strings.Split(oldPath, "/") {
			if part == "" || part == "." || part == ".." {
				return "", ErrNextcloudOnboardingConflict
			}
		}
		return oldPath, nil // Root moves do not alter canonical root-relative file paths.
	}
	path := oldPath
	for _, proof := range proofs {
		if path == proof.OldRootPath || strings.HasPrefix(path, proof.OldRootPath+"/") {
			path = proof.NewRootPath + strings.TrimPrefix(path, proof.OldRootPath)
		}
	}
	return path, nil
}

// RootMigrationAuthorizesEpoch returns the root migration authorizes epoch result.
func (s *NextcloudOnboardingStore) RootMigrationAuthorizesEpoch(
	ctx context.Context,
	tenant uint64,
	kb, ds, oldPath string,
	oldEpoch, currentEpoch int64,
) (bool, error) {
	proofs, err := s.finalizedRootPathChain(ctx, tenant, kb, ds)
	if err != nil {
		return false, err
	}
	if oldPath != "" && !strings.HasPrefix(oldPath, "/") {
		if _, err := s.ResolveCurrentSourcePath(ctx, tenant, kb, ds, oldPath); err != nil {
			return false, err
		}
		if len(proofs) == 0 {
			return false, nil
		}
		firstOld, _ := strconv.ParseInt(proofs[0].OldEpoch, 10, 64)
		lastResume, _ := strconv.ParseInt(proofs[len(proofs)-1].ResumeEpoch, 10, 64)
		return oldEpoch >= 0 && oldEpoch <= firstOld && currentEpoch == lastResume, nil
	}
	path := oldPath
	used := false
	firstOld := int64(-1)
	lastResume := int64(-1)
	for _, proof := range proofs {
		if path == proof.OldRootPath || strings.HasPrefix(path, proof.OldRootPath+"/") {
			if !used {
				firstOld, _ = strconv.ParseInt(proof.OldEpoch, 10, 64)
			}
			used = true
			path = proof.NewRootPath + strings.TrimPrefix(path, proof.OldRootPath)
			lastResume, _ = strconv.ParseInt(proof.ResumeEpoch, 10, 64)
		}
	}
	return used && oldEpoch >= 0 && oldEpoch <= firstOld && lastResume == currentEpoch, nil
}

// NextcloudTargetMigration represents nextcloud target migration repository data.
// TargetMigration is a new empty-KB reservation followed by a one-way source
// retirement. Its new source is built privately until the complete live
// manifest has matching completed candidates.
type NextcloudTargetMigration struct {
	OperationID        string `json:"operation_id" gorm:"primaryKey"`
	OldOperationID     string `json:"old_operation_id"`
	NewOperationID     string `json:"new_operation_id"`
	OldTenantID        uint64 `json:"old_tenant_id"`
	OldKnowledgeBaseID string `json:"old_knowledge_base_id"`
	OldDataSourceID    string `json:"old_data_source_id"`
	NewTenantID        uint64 `json:"new_tenant_id"`
	NewKnowledgeBaseID string `json:"new_knowledge_base_id"`
	NewBindingID       string `json:"new_binding_id"`
	State              string `json:"state"`
	ManifestSHA256     string
	CreatedAt          time.Time
	UpdatedAt          time.Time
}

// TableName returns the persisted table name.
func (NextcloudTargetMigration) TableName() string { return "nextcloud_target_migrations" }

// PrepareTargetMigration prepares target migration.
func (s *NextcloudOnboardingStore) PrepareTargetMigration(
	ctx context.Context,
	old types.NextcloudOnboardingOperation,
	migrationID, newOperationID, actorDir, actorGUID string,
) (NextcloudTargetMigration, error) {
	var out NextcloudTargetMigration
	if _, err := uuid.Parse(migrationID); err != nil {
		return out, ErrNextcloudOnboardingConflict
	}
	var target types.NextcloudOnboardingOperation
	if err := s.db.WithContext(ctx).Where("operation_id=?", newOperationID).Take(&target).Error; err != nil {
		return out, ErrNextcloudOnboardingConflict
	}
	if target.InstanceID != old.InstanceID || target.BindingID == old.BindingID ||
		target.KnowledgeBaseID == old.KnowledgeBaseID && target.TenantID == old.TenantID {
		return out, ErrNextcloudOnboardingConflict
	}
	var cred types.NextcloudOnboardingCredential
	if err := s.db.WithContext(ctx).
		Where("id=? AND enabled=?", target.CredentialID, true).
		Take(&cred).
		Error; err != nil {
		return out, ErrNextcloudOnboardingDenied
	}
	if _, err := s.Actor(ctx, cred, actorDir, actorGUID); err != nil {
		return out, err
	}
	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := requireNextcloudTargetOpenInTx(tx, old.TenantID, old.KnowledgeBaseID); err != nil {
			return err
		}
		// Lock KBs in a deterministic tenant/KB order for cross-tenant moves.
		ids := []types.NextcloudOnboardingOperation{old, target}
		sort.Slice(ids, func(i, j int) bool {
			if ids[i].TenantID == ids[j].TenantID {
				return ids[i].KnowledgeBaseID < ids[j].KnowledgeBaseID
			}
			return ids[i].TenantID < ids[j].TenantID
		})
		for _, o := range ids {
			if err := requireNextcloudTargetOpenInTx(tx, o.TenantID, o.KnowledgeBaseID); err != nil {
				return err
			}
			q := tx.Exec(
				("UPDATE knowledge_bases SET ever_had_nextcloud_source=ever_had_nextcloud_so" +
					"urce WHERE tenant_id=? AND id=? AND deleted_at IS NULL"),
				o.TenantID,
				o.KnowledgeBaseID,
			)
			if q.Error != nil || q.RowsAffected != 1 {
				return ErrNextcloudOnboardingConflict
			}
		}
		e := tx.Where("operation_id=?", migrationID).Take(&out).Error
		if e == nil {
			if out.OldOperationID != old.OperationID || out.NewOperationID != target.OperationID {
				return ErrNextcloudOnboardingConflict
			}
			return nil
		}
		if !errors.Is(e, gorm.ErrRecordNotFound) {
			return e
		}

		if target.State != "reserved" {
			return ErrNextcloudOnboardingConflict
		}
		var pending int64
		if err := tx.Model(&NextcloudTargetMigration{}).
			Where("old_operation_id=? OR new_operation_id=?", old.OperationID, target.OperationID).
			Count(&pending).
			Error; err != nil ||
			pending != 0 {
			return ErrNextcloudOnboardingConflict
		}
		var pair NextcloudSourcePairing
		if err := tx.Where("operation_id=? AND state='active'", old.OperationID).Take(&pair).Error; err != nil {
			return ErrNextcloudOnboardingConflict
		}

		if tx.Migrator().HasTable("nextcloud_content_fences") {
			if err := NewNextcloudContentLeaseStore(
				tx,
			).RetireKBInTx(ctx, tx, old.TenantID, old.KnowledgeBaseID); err != nil {
				return err
			}
			now, err := nextcloudLeaseNowMS(tx)
			if err != nil {
				return err
			}
			if err = tx.Table("nextcloud_content_leases").
				Where("tenant_id=? AND knowledge_base_id=? AND released_at_ms IS NULL", old.TenantID, old.KnowledgeBaseID).
				Updates(map[string]any{"released_at_ms": now, "updated_at_ms": now}).
				Error; err != nil {
				return err
			}
		}
		if tx.Migrator().HasTable("nextcloud_graph_tasks") {
			if err := tx.Table("nextcloud_graph_tasks").
				Where("tenant_id=? AND knowledge_base_id=? AND state IN ?", old.TenantID, old.KnowledgeBaseID, []string{"pending", "running"}).
				Updates(map[string]any{"state": "cancelled", "error_code": "target_migration", "completed_at": time.Now().Unix()}).
				Error; err != nil {
				return err
			}
		}
		// Reuse the durable version withdrawal/GC protocol for exactly the old KB.
		if tx.Migrator().HasTable("nextcloud_source_versions") {
			var externalIDs []string
			if err := tx.Table("nextcloud_source_versions").
				Where("tenant_id=? AND knowledge_base_id=? AND datasource_id=?", old.TenantID, old.KnowledgeBaseID, pair.DataSourceID).
				Order("external_id").
				Pluck("external_id", &externalIDs).
				Error; err != nil {
				return err
			}
			versionStore := NewKnowledgeRepository(tx).(interface {
				TombstoneNextcloudVersion(context.Context, uint64, string, string, string) error
			})
			for _, external := range externalIDs {
				if err := versionStore.TombstoneNextcloudVersion(
					ctx,
					old.TenantID,
					old.KnowledgeBaseID,
					pair.DataSourceID,
					external,
				); err != nil {
					return err
				}
			}
		}
		if err := tx.Table("data_sources").
			Where("id=? AND tenant_id=?", pair.DataSourceID, old.TenantID).
			Updates(map[string]any{"sync_enabled": false, "status": types.DataSourceStatusPaused}).
			Error; err != nil {
			return err
		}
		if err := tx.Where("operation_id=?", old.OperationID).Delete(&types.NextcloudSourceGrant{}).Error; err != nil {
			return err
		}
		if err := tx.Model(&types.NextcloudOnboardingOperation{}).
			Where("operation_id=?", old.OperationID).
			Updates(map[string]any{"state": "retired", "updated_at": time.Now().UTC()}).
			Error; err != nil {
			return err
		}
		if _, err := bumpPermissionVersion(tx, old.TenantID); err != nil {
			return err
		}
		out = NextcloudTargetMigration{
			OperationID:        migrationID,
			OldOperationID:     old.OperationID,
			NewOperationID:     target.OperationID,
			OldTenantID:        old.TenantID,
			OldKnowledgeBaseID: old.KnowledgeBaseID,
			OldDataSourceID:    pair.DataSourceID,
			NewTenantID:        target.TenantID,
			NewKnowledgeBaseID: target.KnowledgeBaseID,
			NewBindingID:       target.BindingID,
			State:              "building",
			CreatedAt:          time.Now().UTC(),
			UpdatedAt:          time.Now().UTC(),
		}
		return tx.Create(&out).Error
	})
	return out, err
}

// FinalizeTargetMigration finalizes target migration.
func (s *NextcloudOnboardingStore) FinalizeTargetMigration(
	ctx context.Context,
	old types.NextcloudOnboardingOperation,
	migrationID string,
	manifest nextcloud.RecoveryManifest,
) (NextcloudTargetMigration, error) {
	var out NextcloudTargetMigration
	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := requireNextcloudTargetOpenInTx(tx, old.TenantID, old.KnowledgeBaseID); err != nil {
			return err
		}
		if err := tx.Where("operation_id=? AND old_operation_id=?", migrationID, old.OperationID).
			Take(&out).
			Error; err != nil {
			return ErrNextcloudOnboardingConflict
		}
		if err := requireNextcloudTargetOpenInTx(tx, out.NewTenantID, out.NewKnowledgeBaseID); err != nil {
			return err
		}
		q := tx.Exec(
			"UPDATE knowledge_bases SET ever_had_nextcloud_source=ever_had_nextcloud_source WHERE tenant_id=? AND id=?",
			out.NewTenantID,
			out.NewKnowledgeBaseID,
		)
		if q.Error != nil || q.RowsAffected != 1 {
			return ErrNextcloudOnboardingConflict
		}
		var target types.NextcloudOnboardingOperation
		if err := tx.Where("operation_id=? AND state='active'", out.NewOperationID).Take(&target).Error; err != nil {
			return ErrNextcloudOnboardingConflict
		}

		if target.PermissionVersion == 0 || target.PermissionSyncedAt == nil ||
			time.Since(*target.PermissionSyncedAt) > 300*time.Second {
			return ErrNextcloudOnboardingConflict
		}
		if manifest.Identity.InstanceID != target.InstanceID || manifest.Identity.BindingID != target.BindingID ||
			manifest.Identity.PublicationState != "active" ||
			manifest.Generation == "" {
			return ErrNextcloudOnboardingConflict
		}
		var pair NextcloudSourcePairing
		if err := tx.Where("operation_id=? AND state='active'", target.OperationID).Take(&pair).Error; err != nil {
			return ErrNextcloudOnboardingConflict
		}
		if out.State == "active" {
			return nil
		}
		var versions []nextcloudSourceVersion
		if err := tx.Table("nextcloud_source_versions").
			Where("tenant_id=? AND knowledge_base_id=? AND datasource_id=? AND state IN ?", target.TenantID, target.KnowledgeBaseID, pair.DataSourceID, []string{"staging", "published"}).
			Find(&versions).
			Error; err != nil {
			return err
		}
		byID := map[string]nextcloudSourceVersion{}
		for _, v := range versions {
			byID[v.ExternalID] = v
		}
		if len(byID) != len(manifest.Files) {
			return ErrNextcloudOnboardingConflict
		}
		for _, f := range manifest.Files {
			v, ok := byID["nextcloud:"+target.InstanceID+":"+strconv.FormatInt(f.FileID, 10)]
			if !ok || v.DesiredETag != f.ETag || v.CandidateKnowledgeID == "" {
				return ErrNextcloudOnboardingConflict
			}
			var k types.Knowledge
			if err := tx.Where("tenant_id=? AND id=? AND deleted_at IS NULL", target.TenantID, v.CandidateKnowledgeID).
				Take(&k).
				Error; err != nil || k.ParseStatus != types.ParseStatusCompleted ||
				k.PendingSubtasksCount != 0 {
				return ErrNextcloudOnboardingConflict
			}
			metadata, err := strictLineageKnowledgeMetadata(k.Metadata)
			if err != nil {
				return ErrNextcloudOnboardingConflict
			}
			want, e := types.NormalizeNextcloudContentRevision(f.SourceContentRevision)
			actual, a := types.NormalizeNextcloudContentRevision(metadata["nextcloud_content_revision"])
			if e != nil || a != nil || want != actual {
				return ErrNextcloudOnboardingConflict
			}
			if ready, err := nextcloudRequiredGraphReady(tx, &k); err != nil || !ready {
				return ErrNextcloudOnboardingConflict
			}
		}
		if err := tx.Model(&out).
			Updates(map[string]any{"state": "active", "manifest_sha256": onboardingDigest(manifest), "updated_at": time.Now().UTC()}).
			Error; err != nil {
			return err
		}
		out.State = "active"
		return nil
	})
	return out, err
}

func nextcloudTargetMigrationPublicationBlocked(tx *gorm.DB, tenant uint64, kb, _ string) (bool, error) {
	if !tx.Migrator().HasTable(&NextcloudTargetMigration{}) {
		return false, nil
	}
	var n int64
	err := tx.Model(&NextcloudTargetMigration{}).
		Where("new_tenant_id=? AND new_knowledge_base_id=? AND state='building'", tenant, kb).
		Count(&n).
		Error
	return n > 0, err
}

// TargetMigrationSource returns the target migration source result.
func (s *NextcloudOnboardingStore) TargetMigrationSource(
	ctx context.Context,
	old types.NextcloudOnboardingOperation,
	id string,
) (NextcloudTargetMigration, *types.DataSource, error) {
	var row NextcloudTargetMigration
	if err := s.db.WithContext(ctx).
		Where("operation_id=? AND old_operation_id=?", id, old.OperationID).
		Take(&row).
		Error; err != nil {
		return row, nil, ErrNextcloudOnboardingConflict
	}
	var pair NextcloudSourcePairing
	if err := s.db.WithContext(ctx).
		Where("operation_id=? AND state='active'", row.NewOperationID).
		Take(&pair).
		Error; err != nil {
		return row, nil, ErrNextcloudOnboardingConflict
	}
	var ds types.DataSource
	err := s.db.WithContext(ctx).
		Where("id=? AND tenant_id=? AND deleted_at IS NULL", pair.DataSourceID, row.NewTenantID).
		Take(&ds).
		Error
	return row, &ds, err
}

// TargetMigrationCandidateIDs returns the target migration candidate ids result.
func (s *NextcloudOnboardingStore) TargetMigrationCandidateIDs(
	ctx context.Context,
	row NextcloudTargetMigration,
) ([]string, error) {
	var ids []string
	err := s.db.WithContext(ctx).
		Table("nextcloud_source_versions").
		Where(("tenant_id=? AND knowledge_base_id=? AND state='staging' AND candidate_know"+
			"ledge_id<>''"), row.NewTenantID, row.NewKnowledgeBaseID).
		Order("external_id").
		Pluck("candidate_knowledge_id", &ids).
		Error
	return ids, err
}

// TargetMigrationMultimodal returns the target migration multimodal result.
func (s *NextcloudOnboardingStore) TargetMigrationMultimodal(
	ctx context.Context,
	row NextcloudTargetMigration,
) (bool, error) {
	var kb types.KnowledgeBase
	err := s.db.WithContext(ctx).
		Where("tenant_id=? AND id=? AND deleted_at IS NULL", row.NewTenantID, row.NewKnowledgeBaseID).
		Take(&kb).
		Error
	return kb.IsMultimodalEnabled(), err
}
