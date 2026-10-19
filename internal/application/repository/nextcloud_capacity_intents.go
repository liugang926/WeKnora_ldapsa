package repository

import (
	"context"
	"errors"
	"strconv"
	"strings"

	"github.com/Tencent/WeKnora/internal/types"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type NextcloudCapacityWriteIntent struct {
	Token                                                             string `gorm:"primaryKey"`
	KnowledgeID, WriterID, PhysicalPath, RootIdentity, ScopeID, State string
	ObservedBytes                                                     int64
	SHA256                                                            string `gorm:"column:sha256"`
	ObjectIdentity                                                    string
	ResourceID                                                        string
	OrphanChargeBytes, UpdatedAtMS                                    int64
}

func (NextcloudCapacityWriteIntent) TableName() string { return "nextcloud_capacity_write_intents" }

func (s *NextcloudCapacityStore) lockDurable(
	tx *gorm.DB,
	r *NextcloudCapacityReservation,
) (NextcloudCapacityReservation, int64, error) {
	var row NextcloudCapacityReservation
	if r == nil {
		return row, 0, ErrNextcloudCapacityUnavailable
	}
	// A writer lock is obtained on SQLite before reading state as well.
	if e := tx.Model(&NextcloudCapacityReservation{}).
		Where("token=?", r.Token).
		UpdateColumn("token", gorm.Expr("token")).
		Error; e != nil {
		return row, 0, e
	}
	if e := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("token=?", r.Token).Take(&row).Error; e != nil {
		return row, 0, e
	}
	if row.ProtocolVersion != 1 || row.TenantID != r.TenantID || row.KnowledgeBaseID != r.KnowledgeBaseID ||
		row.BackendID != r.BackendID ||
		row.ScopeID != r.ScopeID {
		return row, 0, ErrNextcloudCapacityUnavailable
	}
	var kb types.KnowledgeBase
	if e := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
		Where("id=? AND tenant_id=?", row.KnowledgeBaseID, row.TenantID).
		Take(&kb).
		Error; e != nil {
		return row, 0, e
	}
	if kb.StorageBackendID == nil || *kb.StorageBackendID != row.BackendID {
		return row, 0, ErrNextcloudCapacityUnavailable
	}
	now, e := nextcloudLeaseNowMS(tx)
	return row, now, e
}

func (s *NextcloudCapacityStore) PrepareLocal(
	ctx context.Context,
	r *NextcloudCapacityReservation,
	writer, knowledge, path, root, scope string,
	size int64,
) error {
	backend, inner, ok := types.ParseStorageBackendPath(path)
	if r == nil || writer == "" || knowledge == "" || !ok || backend != r.BackendID ||
		!strings.HasPrefix(inner, "local://") ||
		len(root) != 64 ||
		scope != r.ScopeID ||
		size != r.Bytes {
		return ErrNextcloudCapacityUnavailable
	}
	parts := strings.Split(strings.TrimPrefix(inner, "local://"), "/")
	if len(parts) != 3 || parts[0] != strconv.FormatUint(r.TenantID, 10) || parts[1] != knowledge ||
		!strings.HasPrefix(parts[2], "capacity-"+r.Token) {
		return ErrNextcloudCapacityUnavailable
	}
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		row, now, e := s.lockDurable(tx, r)
		if e != nil {
			return e
		}
		if row.State != "pending" || row.LeaseUntilMS <= now {
			return ErrNextcloudCapacityUnavailable
		}
		var count int64
		if e = tx.Model(&NextcloudCapacityWriteIntent{}).Where("token=?", r.Token).Count(&count).Error; e != nil {
			return e
		}
		if count != 0 {
			return ErrNextcloudCapacityUnavailable
		}
		return tx.Create(
			&NextcloudCapacityWriteIntent{
				Token:        r.Token,
				KnowledgeID:  knowledge,
				WriterID:     writer,
				PhysicalPath: path,
				RootIdentity: root,
				ScopeID:      scope,
				State:        "prepared",
				UpdatedAtMS:  now,
			},
		).Error
	})
}

func (s *NextcloudCapacityStore) ValidateWriter(
	ctx context.Context,
	r *NextcloudCapacityReservation,
	writer, path, knowledge string,
) error {
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		row, now, e := s.lockDurable(tx, r)
		if e != nil {
			return e
		}
		if row.State != "pending" || row.LeaseUntilMS <= now {
			return ErrNextcloudCapacityUnavailable
		}
		var i NextcloudCapacityWriteIntent
		if e = tx.Where("token=?", r.Token).Take(&i).Error; e != nil {
			return e
		}
		if i.WriterID != writer || i.PhysicalPath != path || (knowledge != "" && i.KnowledgeID != knowledge) ||
			(i.State != "prepared" && i.State != "writing" && i.State != "written" && i.State != "cataloged") {
			return ErrNextcloudCapacityUnavailable
		}
		return nil
	})
}

func (s *NextcloudCapacityStore) LocalCreated(
	ctx context.Context,
	r *NextcloudCapacityReservation,
	writer, path, identity string,
) error {
	if len(identity) != 64 {
		return ErrNextcloudCapacityUnavailable
	}
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		row, now, e := s.lockDurable(tx, r)
		if e != nil {
			return e
		}
		if row.State != "pending" || row.LeaseUntilMS <= now {
			return ErrNextcloudCapacityUnavailable
		}
		u := tx.Model(&NextcloudCapacityWriteIntent{}).
			Where("token=? AND writer_id=? AND physical_path=? AND state='prepared' AND object_identity=''", row.Token, writer, path).
			Updates(map[string]any{"state": "writing", "object_identity": identity, "updated_at_ms": now})
		if u.Error != nil {
			return u.Error
		}
		if u.RowsAffected != 1 {
			return ErrNextcloudCapacityUnavailable
		}
		return nil
	})
}

func (s *NextcloudCapacityStore) PhysicalWritten(
	ctx context.Context,
	r *NextcloudCapacityReservation,
	writer, path string,
	size int64,
	hash string,
) error {
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		row, now, e := s.lockDurable(tx, r)
		if e != nil {
			return e
		}
		if row.State != "pending" || row.LeaseUntilMS <= now || size != row.Bytes || len(hash) != 64 {
			return ErrNextcloudCapacityUnavailable
		}
		update := tx.Model(&NextcloudCapacityWriteIntent{}).
			Where("token=? AND writer_id=? AND physical_path=? AND state='writing'", r.Token, writer, path).
			Updates(map[string]any{"state": "written", "observed_bytes": size, "sha256": hash, "updated_at_ms": now})
		if update.Error != nil {
			return update.Error
		}
		if update.RowsAffected != 1 {
			return ErrNextcloudCapacityUnavailable
		}
		return nil
	})
}

func (s *NextcloudCapacityStore) Cataloged(
	ctx context.Context,
	r *NextcloudCapacityReservation,
	writer, path, knowledge, ref string,
) error {
	handle, ok := types.ParseResourcePath(ref)
	if !ok {
		return ErrNextcloudCapacityUnavailable
	}
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		row, now, e := s.lockDurable(tx, r)
		if e != nil {
			return e
		}
		if row.State != "pending" || row.LeaseUntilMS <= now {
			return ErrNextcloudCapacityUnavailable
		}
		var resource types.StoredResource
		if e = tx.Where("handle=? AND tenant_id=? AND storage_backend_id=? AND physical_path=? AND provider='local' AND state='active'", handle, row.TenantID, row.BackendID, path).
			Take(&resource).
			Error; e != nil {
			return e
		}
		if resource.Size != row.Bytes {
			return ErrNextcloudCapacityUnavailable
		}
		var count int64
		if e = tx.Model(&types.ResourceBinding{}).
			Where("resource_id=? AND tenant_id=? AND owner_type='knowledge' AND owner_id=? AND relation='source_file'", resource.ID, row.TenantID, knowledge).
			Count(&count).
			Error; e != nil {
			return e
		}
		if count != 1 {
			return ErrNextcloudCapacityUnavailable
		}
		u := tx.Model(&NextcloudCapacityWriteIntent{}).
			Where("token=? AND writer_id=? AND physical_path=? AND knowledge_id=? AND state='written'", row.Token, writer, path, knowledge).
			Updates(map[string]any{"state": "cataloged", "resource_id": resource.ID, "updated_at_ms": now})
		if u.Error != nil {
			return u.Error
		}
		if u.RowsAffected != 1 {
			return ErrNextcloudCapacityUnavailable
		}
		return nil
	})
}

// Cancel is proof from the new protocol's absence of a pre-IO intent. Old
// reservations remain unknown and can never be retrospectively certified.
func (s *NextcloudCapacityStore) CancelUnstarted(ctx context.Context, r *NextcloudCapacityReservation) error {
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		row, _, e := s.lockDurable(tx, r)
		if e != nil {
			return e
		}
		if row.State == "cancelled" {
			return nil
		}
		if row.State != "pending" {
			return ErrNextcloudCapacityUnavailable
		}
		var n int64
		if e = tx.Model(&NextcloudCapacityWriteIntent{}).Where("token=?", row.Token).Count(&n).Error; e != nil {
			return e
		}
		if n != 0 {
			return ErrNextcloudCapacityUnavailable
		}
		return tx.Model(&row).Update("state", "cancelled").Error
	})
}

func (s *NextcloudCapacityStore) MarkWriteFailed(
	ctx context.Context,
	r *NextcloudCapacityReservation,
	writer string,
) error {
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		row, now, e := s.lockDurable(tx, r)
		if e != nil {
			return e
		}
		if row.State != "pending" {
			return nil
		}
		return tx.Model(&NextcloudCapacityWriteIntent{}).
			Where("token=? AND writer_id=? AND state IN ('prepared','writing','written','cataloged')", row.Token, writer).
			Updates(map[string]any{"state": "failed", "updated_at_ms": now}).
			Error
	})
}

func (s *NextcloudCapacityStore) Intent(
	ctx context.Context,
	r *NextcloudCapacityReservation,
) (NextcloudCapacityWriteIntent, error) {
	var i NextcloudCapacityWriteIntent
	if r == nil || r.ProtocolVersion != 1 {
		return i, ErrNextcloudCapacityUnavailable
	}
	e := s.db.WithContext(ctx).Where("token=?", r.Token).Take(&i).Error
	return i, e
}

// Called only while the exact local provider holds its exclusive OS fence.
// An object is never deleted here. Existing owner claims stay authoritative.
func (s *NextcloudCapacityStore) ReconcileLocal(
	ctx context.Context,
	r *NextcloudCapacityReservation,
	expected NextcloudCapacityWriteIntent,
	o types.CapacityLocalObject,
) error {
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		row, now, e := s.lockDurable(tx, r)
		if e != nil {
			return e
		}
		var i NextcloudCapacityWriteIntent
		if e = tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("token=?", row.Token).Take(&i).Error; e != nil {
			return e
		}
		if i.Token != expected.Token || i.WriterID != expected.WriterID || i.KnowledgeID != expected.KnowledgeID ||
			i.PhysicalPath != expected.PhysicalPath ||
			i.RootIdentity != o.RootIdentity ||
			i.ScopeID != o.ScopeID ||
			o.Token != row.Token ||
			o.BackendID != row.BackendID ||
			o.PhysicalPath != i.PhysicalPath ||
			o.Size < 0 ||
			o.Size > row.Bytes {
			return ErrNextcloudCapacityUnavailable
		}
		var resources []types.StoredResource
		if e = tx.Unscoped().
			Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("physical_path=?", i.PhysicalPath).
			Find(&resources).
			Error; e != nil {
			return e
		}
		if len(resources) > 1 {
			return ErrNextcloudCapacityUnavailable
		}
		var owners int64
		own := false
		resourceID := ""
		for _, resource := range resources {
			if resource.TenantID != row.TenantID || resource.StorageBackendID != row.BackendID ||
				resource.Provider != "local" {
				return ErrNextcloudCapacityUnavailable
			}
			if e = tx.Model(&types.ResourceBinding{}).
				Where("resource_id=?", resource.ID).
				Count(&owners).
				Error; e != nil {
				return e
			}
			var k types.Knowledge
			ke := tx.Where("id=? AND tenant_id=? AND knowledge_base_id=?", i.KnowledgeID, row.TenantID, row.KnowledgeBaseID).
				Take(&k).
				Error
			var count int64
			if e = tx.Model(&types.ResourceBinding{}).
				Where("resource_id=? AND tenant_id=? AND owner_type='knowledge' AND owner_id=? AND relation='source_file'", resource.ID, row.TenantID, i.KnowledgeID).
				Count(&count).
				Error; e != nil {
				return e
			}
			own = ke == nil && count == 1 && resource.State == types.ResourceStateActive && resource.Size == o.Size &&
				k.FilePath == types.BuildResourcePath(resource.Handle)
			if ke != nil && !errors.Is(ke, gorm.ErrRecordNotFound) {
				return ke
			}
			resourceID = resource.ID
		}
		if o.Exists &&
			(i.ObjectIdentity == "" || i.ObjectIdentity != o.ObjectIdentity || (i.SHA256 != "" && i.SHA256 != o.SHA256)) {
			return ErrNextcloudCapacityUnavailable
		}
		if i.State == "charged" {
			if row.State != "committed" {
				return ErrNextcloudCapacityUnavailable
			}
			if !o.Exists || o.Size != i.ObservedBytes || o.SHA256 != i.SHA256 {
				return ErrNextcloudCapacityUnavailable
			}
			return nil
		}
		if i.State == "released" {
			if row.State != "released" {
				return ErrNextcloudCapacityUnavailable
			}
			if o.Exists || owners != 0 {
				return ErrNextcloudCapacityUnavailable
			}
			return nil
		}
		if row.State != "pending" {
			return ErrNextcloudCapacityUnavailable
		}
		if !o.Exists {
			if owners != 0 {
				return ErrNextcloudCapacityUnavailable
			}
			if e = tx.Model(&i).Updates(map[string]any{"state": "released", "updated_at_ms": now}).Error; e != nil {
				return e
			}
			return tx.Model(&row).Update("state", "released").Error
		}
		orphan := o.Size
		if own {
			orphan = 0
		}
		if e = tx.Model(&i).
			Updates(map[string]any{"state": "charged", "observed_bytes": o.Size, "sha256": o.SHA256, "resource_id": resourceID, "orphan_charge_bytes": orphan, "updated_at_ms": now}).
			Error; e != nil {
			return e
		}
		// Keep the original reserved bytes as an immutable admission receipt. A
		// fresh provider measurement supersedes its temporary disk buffer; the
		// exact orphan object remains charged to the original KB indefinitely.
		return tx.Model(&row).Updates(map[string]any{"state": "committed", "committed_at_ms": now}).Error
	})
}

func (s *NextcloudCapacityStore) FinishDurable(
	ctx context.Context,
	r *NextcloudCapacityReservation,
	k string,
	success bool,
) error {
	if !success {
		return s.CancelUnstarted(ctx, r)
	}
	i, e := s.Intent(ctx, r)
	if errors.Is(e, gorm.ErrRecordNotFound) {
		return s.CancelUnstarted(ctx, r)
	}
	if e != nil {
		return e
	}
	if i.KnowledgeID != k || i.State != "cataloged" {
		return ErrNextcloudCapacityUnavailable
	}
	// The writer still holds the provider lock. The catalog must match the
	// intended knowledge and physical object; recovery uses fresh observation.
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		row, now, e := s.lockDurable(tx, r)
		if e != nil {
			return e
		}
		if row.State != "pending" || row.LeaseUntilMS <= now {
			return ErrNextcloudCapacityUnavailable
		}
		var count int64
		var resource types.StoredResource
		if e = tx.Where("id=?", i.ResourceID).Take(&resource).Error; e != nil {
			return e
		}
		if e = tx.Raw("SELECT COUNT(*) FROM resources s JOIN resource_bindings b ON b.resource_id=s.id JOIN knowledges k ON k.id=b.owner_id AND k.tenant_id=s.tenant_id WHERE s.id=? AND s.tenant_id=? AND s.storage_backend_id=? AND s.physical_path=? AND s.state='active' AND s.size=? AND b.owner_type='knowledge' AND b.relation='source_file' AND k.id=? AND k.knowledge_base_id=? AND k.file_path=?", i.ResourceID, row.TenantID, row.BackendID, i.PhysicalPath, i.ObservedBytes, k, row.KnowledgeBaseID, types.BuildResourcePath(resource.Handle)).
			Scan(&count).
			Error; e != nil {
			return e
		}
		if count != 1 {
			return ErrNextcloudCapacityUnavailable
		}
		if e = tx.Model(&i).
			Updates(map[string]any{"state": "charged", "orphan_charge_bytes": 0, "updated_at_ms": now}).
			Error; e != nil {
			return e
		}
		return tx.Model(&row).Updates(map[string]any{"state": "committed", "committed_at_ms": now}).Error
	})
}
