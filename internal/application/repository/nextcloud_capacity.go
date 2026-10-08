package repository

import (
	"context"
	"errors"
	"math"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var (
	ErrNextcloudCapacityUnavailable = errors.New("Nextcloud storage capacity is unavailable")
	ErrNextcloudCapacityPaused      = errors.New("Nextcloud AI ingestion is paused by capacity")
	ErrNextcloudRetentionQuota      = errors.New("Nextcloud knowledge base retention quota exceeded")
)

type NextcloudCapacityPolicy struct {
	TenantID            uint64 `gorm:"primaryKey"`
	KnowledgeBaseID     string `gorm:"primaryKey"`
	RetentionQuotaBytes int64
	WarningPercent      int
	PausePercent        int
}

func (NextcloudCapacityPolicy) TableName() string { return "nextcloud_capacity_policies" }

type capacityScope struct {
	ScopeID        string `gorm:"primaryKey"`
	TotalBytes     int64
	AvailableBytes int64
	MeasuredAtMS   int64
	State          string
	Revision       int64
}

func (capacityScope) TableName() string { return "nextcloud_capacity_scopes" }

type NextcloudCapacityReservation struct {
	Token           string `gorm:"primaryKey"`
	ScopeID         string
	TenantID        uint64
	KnowledgeBaseID string
	BackendID       string
	RequestKey      string
	Bytes           int64
	State           string
	LeaseUntilMS    int64
	CreatedAtMS     int64
}

func (NextcloudCapacityReservation) TableName() string { return "nextcloud_capacity_reservations" }

type NextcloudCapacityStore struct{ db *gorm.DB }

func NewNextcloudCapacityStore(db *gorm.DB) *NextcloudCapacityStore {
	return &NextcloudCapacityStore{db: db}
}

func (s *NextcloudCapacityStore) SetPolicy(ctx context.Context, p NextcloudCapacityPolicy) error {
	if p.TenantID == 0 || p.KnowledgeBaseID == "" || p.RetentionQuotaBytes < 0 || p.WarningPercent <= 0 || p.WarningPercent >= p.PausePercent || p.PausePercent > 100 {
		return ErrNextcloudCapacityUnavailable
	}
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var kb types.KnowledgeBase
		if err := tx.Where("tenant_id=? AND id=?", p.TenantID, p.KnowledgeBaseID).Take(&kb).Error; err != nil {
			return err
		}
		return tx.Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "tenant_id"}, {Name: "knowledge_base_id"}}, DoUpdates: clause.AssignmentColumns([]string{"retention_quota_bytes", "warning_percent", "pause_percent"})}).Create(&p).Error
	})
}
func validCapacity(m types.StorageCapacity, now time.Time) bool {
	return len(m.ScopeID) == 64 && m.TotalBytes > 0 && m.AvailableBytes >= 0 && m.AvailableBytes <= m.TotalBytes && !m.MeasuredAt.After(now.Add(time.Second)) && now.Sub(m.MeasuredAt) <= 30*time.Second
}
func addBytes(a, b int64) (int64, error) {
	if a < 0 || b < 0 || a > math.MaxInt64-b {
		return 0, ErrNextcloudCapacityUnavailable
	}
	return a + b, nil
}
func threshold(total int64, percent int) int64 {
	return total/100*int64(percent) + (total%100*int64(percent))/100
}

// Reserve serializes disk-wide and KB quota admissions. A measurement failure
// closes new AI work, without deleting any active or retained object.
func (s *NextcloudCapacityStore) Reserve(ctx context.Context, tenant uint64, kb, backend, key string, bytes int64, m types.StorageCapacity) (*NextcloudCapacityReservation, error) {
	if s == nil || s.db == nil || tenant == 0 || kb == "" || backend == "" || key == "" || len(key) > 512 || bytes <= 0 {
		return nil, ErrNextcloudCapacityUnavailable
	}
	var result *NextcloudCapacityReservation
	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		nowMS, err := nextcloudLeaseNowMS(tx)
		if err != nil {
			return err
		}
		now := time.UnixMilli(nowMS)
		if !validCapacity(m, now) {
			return ErrNextcloudCapacityUnavailable
		}
		scope := capacityScope{ScopeID: m.ScopeID, TotalBytes: m.TotalBytes, AvailableBytes: m.AvailableBytes, MeasuredAtMS: m.MeasuredAt.UnixMilli(), State: "normal", Revision: 1}
		if err := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&scope).Error; err != nil {
			return err
		}
		// UPDATE also acquires a writer lock on SQLite before any quota reads.
		if err := tx.Model(&capacityScope{}).Where("scope_id=?", m.ScopeID).UpdateColumn("revision", gorm.Expr("revision+1")).Error; err != nil {
			return err
		}
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("scope_id=?", m.ScopeID).Take(&scope).Error; err != nil {
			return err
		}
		var row types.KnowledgeBase
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("id=? AND tenant_id=?", kb, tenant).Take(&row).Error; err != nil {
			return err
		}
		if row.StorageBackendID == nil || *row.StorageBackendID != backend {
			return ErrNextcloudCapacityUnavailable
		}
		policy := NextcloudCapacityPolicy{TenantID: tenant, KnowledgeBaseID: kb, WarningPercent: 80, PausePercent: 90}
		if err := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&policy).Error; err != nil {
			return err
		}
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=? AND knowledge_base_id=?", tenant, kb).Take(&policy).Error; err != nil {
			return err
		}
		var existing NextcloudCapacityReservation
		err = tx.Where("tenant_id=? AND knowledge_base_id=? AND request_key=?", tenant, kb, key).Take(&existing).Error
		duplicate := err == nil
		if duplicate {
			if existing.ScopeID != m.ScopeID || existing.BackendID != backend || existing.Bytes != bytes || existing.State != "pending" || existing.LeaseUntilMS <= nowMS {
				return ErrNextcloudCapacityUnavailable
			}
		} else if !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}

		var diskPending, kbPending int64
		if err := tx.Model(&NextcloudCapacityReservation{}).Where("scope_id=? AND state='pending'", m.ScopeID).Select("COALESCE(SUM(bytes),0)").Scan(&diskPending).Error; err != nil {
			return err
		}
		if err := tx.Model(&NextcloudCapacityReservation{}).Where("tenant_id=? AND knowledge_base_id=? AND state='pending'", tenant, kb).Select("COALESCE(SUM(bytes),0)").Scan(&kbPending).Error; err != nil {
			return err
		}
		// Expired reservations remain charged: a crashed upload may own bytes not
		// yet catalogued. Reconciliation, rather than time alone, releases them.
		used := m.TotalBytes - m.AvailableBytes
		projected, err := addBytes(used, diskPending)
		if err != nil {
			return err
		}
		additional := bytes
		if duplicate {
			additional = 0
		}
		projected, err = addBytes(projected, additional)
		if err != nil {
			return err
		}
		state := "normal"
		if projected >= threshold(m.TotalBytes, policy.WarningPercent) {
			state = "warning"
		}
		if projected >= threshold(m.TotalBytes, policy.PausePercent) || diskPending > m.AvailableBytes || additional > m.AvailableBytes-diskPending {
			return ErrNextcloudCapacityPaused
		}
		var logical int64
		if err := tx.Raw(`SELECT COALESCE(SUM(r.size),0) FROM resources r WHERE r.tenant_id=? AND r.storage_backend_id=? AND r.state<>'deleted' AND EXISTS (SELECT 1 FROM resource_bindings b JOIN knowledges k ON k.id=b.owner_id AND k.tenant_id=r.tenant_id WHERE b.resource_id=r.id AND b.owner_type='knowledge' AND k.knowledge_base_id=?)`, tenant, backend, kb).Scan(&logical).Error; err != nil {
			return err
		}
		retained, err := addBytes(logical, kbPending)
		if err != nil {
			return err
		}
		retained, err = addBytes(retained, additional)
		if err != nil {
			return err
		}
		if policy.RetentionQuotaBytes > 0 && retained > policy.RetentionQuotaBytes {
			return ErrNextcloudRetentionQuota
		}
		if err := tx.Model(&scope).Updates(map[string]any{"total_bytes": m.TotalBytes, "available_bytes": m.AvailableBytes, "measured_at_ms": m.MeasuredAt.UnixMilli(), "state": state}).Error; err != nil {
			return err
		}
		if duplicate {
			result = &existing
			return nil
		}
		reservation := NextcloudCapacityReservation{Token: uuid.NewString(), ScopeID: m.ScopeID, TenantID: tenant, KnowledgeBaseID: kb, BackendID: backend, RequestKey: key, Bytes: bytes, State: "pending", LeaseUntilMS: now.Add(15 * time.Minute).UnixMilli(), CreatedAtMS: nowMS}
		if err := tx.Create(&reservation).Error; err != nil {
			return err
		}
		result = &reservation
		return nil
	})
	return result, err
}

func (s *NextcloudCapacityStore) Finish(ctx context.Context, r *NextcloudCapacityReservation, knowledge string, success bool) error {
	if r == nil || r.Token == "" {
		return ErrNextcloudCapacityUnavailable
	}
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var row NextcloudCapacityReservation
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("token=?", r.Token).Take(&row).Error; err != nil {
			return err
		}
		if row.TenantID != r.TenantID || row.KnowledgeBaseID != r.KnowledgeBaseID || row.BackendID != r.BackendID || row.Bytes != r.Bytes {
			return ErrNextcloudCapacityUnavailable
		}
		if row.State == "committed" && success {
			return nil
		}
		if row.State != "pending" {
			return ErrNextcloudCapacityUnavailable
		}
		if !success {
			return nil
		} // retained until storage/no-owner reconciliation proves release safe
		var count int64
		if err := tx.Raw(`SELECT COUNT(*) FROM resource_bindings b JOIN resources s ON s.id=b.resource_id JOIN knowledges k ON k.id=b.owner_id WHERE b.owner_type='knowledge' AND b.owner_id=? AND k.tenant_id=? AND k.knowledge_base_id=? AND s.tenant_id=k.tenant_id AND s.storage_backend_id=? AND s.state='active' AND s.size>=?`, knowledge, row.TenantID, row.KnowledgeBaseID, row.BackendID, row.Bytes).Scan(&count).Error; err != nil {
			return err
		}
		if count != 1 {
			return ErrNextcloudCapacityUnavailable
		}
		return tx.Model(&row).Update("state", "committed").Error
	})
}

// Observe persists actual measurement even when the next admission is denied.
func (s *NextcloudCapacityStore) Observe(ctx context.Context, m types.StorageCapacity) error {
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		ms, err := nextcloudLeaseNowMS(tx)
		if err != nil {
			return err
		}
		if !validCapacity(m, time.UnixMilli(ms)) {
			return ErrNextcloudCapacityUnavailable
		}
		state := "normal"
		used := m.TotalBytes - m.AvailableBytes
		if used >= threshold(m.TotalBytes, 90) {
			state = "paused"
		} else if used >= threshold(m.TotalBytes, 80) {
			state = "warning"
		}
		row := capacityScope{ScopeID: m.ScopeID, TotalBytes: m.TotalBytes, AvailableBytes: m.AvailableBytes, MeasuredAtMS: m.MeasuredAt.UnixMilli(), State: state, Revision: 1}
		return tx.Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "scope_id"}}, DoUpdates: clause.Assignments(map[string]any{"total_bytes": m.TotalBytes, "available_bytes": m.AvailableBytes, "measured_at_ms": m.MeasuredAt.UnixMilli(), "state": state, "revision": gorm.Expr("nextcloud_capacity_scopes.revision+1")})}).Create(&row).Error
	})
}

type NextcloudCapacityStatus struct {
	Policy               NextcloudCapacityPolicy `json:"policy"`
	ScopeID              string                  `json:"scope_id"`
	TotalBytes           int64                   `json:"physical_total_bytes"`
	AvailableBytes       int64                   `json:"physical_available_bytes"`
	MeasuredAtMS         int64                   `json:"measured_at_ms"`
	PendingBytes         int64                   `json:"reserved_bytes"`
	State                string                  `json:"state"`
	LogicalRetainedBytes int64                   `json:"logical_retained_bytes"`
}

func (s *NextcloudCapacityStore) Status(ctx context.Context, tenant uint64, kb, backend string, m types.StorageCapacity) (NextcloudCapacityStatus, error) {
	result := NextcloudCapacityStatus{ScopeID: m.ScopeID, TotalBytes: m.TotalBytes, AvailableBytes: m.AvailableBytes, MeasuredAtMS: m.MeasuredAt.UnixMilli()}
	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		now, err := nextcloudLeaseNowMS(tx)
		if err != nil {
			return err
		}
		if !validCapacity(m, time.UnixMilli(now)) {
			return ErrNextcloudCapacityUnavailable
		}
		var row types.KnowledgeBase
		if err := tx.Where("tenant_id=? AND id=?", tenant, kb).Take(&row).Error; err != nil {
			return err
		}
		if row.StorageBackendID == nil || *row.StorageBackendID != backend {
			return ErrNextcloudCapacityUnavailable
		}
		result.Policy = NextcloudCapacityPolicy{TenantID: tenant, KnowledgeBaseID: kb, WarningPercent: 80, PausePercent: 90}
		if err := tx.Where("tenant_id=? AND knowledge_base_id=?", tenant, kb).Take(&result.Policy).Error; err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}
		if err := tx.Model(&NextcloudCapacityReservation{}).Where("scope_id=? AND state='pending'", m.ScopeID).Select("COALESCE(SUM(bytes),0)").Scan(&result.PendingBytes).Error; err != nil {
			return err
		}
		if err := tx.Raw(`SELECT COALESCE(SUM(r.size),0) FROM resources r WHERE r.tenant_id=? AND r.storage_backend_id=? AND r.state<>'deleted' AND EXISTS (SELECT 1 FROM resource_bindings b JOIN knowledges k ON k.id=b.owner_id AND k.tenant_id=r.tenant_id WHERE b.resource_id=r.id AND b.owner_type='knowledge' AND k.knowledge_base_id=?)`, tenant, backend, kb).Scan(&result.LogicalRetainedBytes).Error; err != nil {
			return err
		}
		projected, err := addBytes(m.TotalBytes-m.AvailableBytes, result.PendingBytes)
		if err != nil {
			return err
		}
		result.State = "normal"
		if projected >= threshold(m.TotalBytes, result.Policy.PausePercent) {
			result.State = "paused"
		} else if projected >= threshold(m.TotalBytes, result.Policy.WarningPercent) {
			result.State = "warning"
		}
		return nil
	})
	return result, err
}
