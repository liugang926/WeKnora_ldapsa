package repository

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"time"

	apperrors "github.com/Tencent/WeKnora/internal/errors"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const (
	nextcloudGCIndexWindow    = time.Hour
	nextcloudGCFailedWindow   = 24 * time.Hour
	nextcloudGCOriginalWindow = 7 * 24 * time.Hour
	nextcloudGCRetryWindow    = time.Hour
	nextcloudGCDerivedBatch   = 1000
)

// nextcloudGCJob is an immutable inventory identity plus a retryable state.
// Object locators live only in nextcloud_gc_items; status responses never carry
// storage paths or URLs.
type nextcloudGCJob struct {
	ID                string `gorm:"primaryKey"`
	TenantID          uint64
	KnowledgeBaseID   string
	DataSourceID      string `gorm:"column:datasource_id"`
	ExternalID        string
	KnowledgeID       string `gorm:"uniqueIndex"`
	Reason            string
	State             string
	NotBefore         time.Time
	OriginalNotBefore time.Time
	NextAttemptAt     time.Time
	Attempts          int
	LastErrorCode     string
	CompletedAt       *time.Time
	CreatedAt         time.Time
	UpdatedAt         time.Time
}

func (nextcloudGCJob) TableName() string { return "nextcloud_gc_jobs" }

type nextcloudGCItem struct {
	JobID                  string `gorm:"primaryKey"`
	Kind                   string `gorm:"primaryKey"`
	ObjectRef              string `gorm:"primaryKey"`
	State                  string
	EstimatedBytes         int64
	ConfirmedReleasedBytes int64
	LeaseToken             string
	LeaseUntil             *time.Time
	CreatedAt              time.Time
	UpdatedAt              time.Time
}

func (nextcloudGCItem) TableName() string { return "nextcloud_gc_items" }

// NextcloudGCStatus is safe for administrative status views. It intentionally
// omits object references, document metadata and backend error messages.
type NextcloudGCStatus struct {
	ID                     string     `json:"id"`
	TenantID               uint64     `json:"tenant_id"`
	KnowledgeBaseID        string     `json:"knowledge_base_id"`
	KnowledgeID            string     `json:"knowledge_id"`
	Reason                 string     `json:"reason"`
	State                  string     `json:"state"`
	NotBefore              time.Time  `json:"not_before"`
	OriginalNotBefore      time.Time  `json:"original_not_before"`
	NextAttemptAt          time.Time  `json:"next_attempt_at"`
	Attempts               int        `json:"attempts"`
	LastErrorCode          string     `json:"last_error_code"`
	PendingItems           int64      `json:"pending_items"`
	EstimatedBytes         int64      `json:"estimated_bytes"`
	ConfirmedReleasedBytes int64      `json:"confirmed_released_bytes"`
	CompletedAt            *time.Time `json:"completed_at,omitempty"`
}

// NextcloudGCStore persists source-generation collection work and inventory.
type NextcloudGCStore struct {
	db           *gorm.DB
	deleteObject func(context.Context, *types.StoredResource) (bool, error)
}

// NewNextcloudGCStore returns the source-generation collection store.
func NewNextcloudGCStore(db *gorm.DB) *NextcloudGCStore { return &NextcloudGCStore{db: db} }

// ConfigureLocalDelete is set before the scheduler starts. The callback must
// delete only the persisted, scoped local path of the passed resource. It
// returns true only if this invocation confirmed an unlink. A missing path
// after a crash can complete the item idempotently, but credits zero bytes.
func (s *NextcloudGCStore) ConfigureLocalDelete(fn func(context.Context, *types.StoredResource) (bool, error)) {
	s.deleteObject = fn
}

type nextcloudGCSource struct {
	types.Knowledge
	DataSourceID string `gorm:"column:datasource_id"`
	ExternalID   string `gorm:"column:external_id"`
	SourceState  string `gorm:"column:source_state"`
	CandidateID  string `gorm:"column:candidate_id"`
}

// InventoryRetired writes the exact known resource references before any
// cleanup. The scan is bounded and idempotent across restarts. A job exists for
// every candidate that is no longer the source's desired candidate; the
// publication guard has already hidden these rows before GC considers them.
func (s *NextcloudGCStore) InventoryRetired(ctx context.Context, now time.Time, limit int) (int, error) {
	if limit <= 0 || limit > 200 {
		limit = 100
	}
	var sources []nextcloudGCSource
	err := s.db.WithContext(ctx).Table("knowledges AS k").
		Select(("k.*, v.datasource_id, v.external_id, v.state AS source_" +
			"state, v.candidate_knowledge_id AS candidate_id")).
		Joins(("JOIN nextcloud_source_versions AS v ON v.tenant_id = k." +
			"tenant_id AND v.knowledge_base_id = k.knowledge_base_id" +
			" AND v.datasource_id = k.metadata->>'datasource_id' AND" +
			" v.external_id = k.metadata->>'external_id'")).
		Where(("k.channel = ? AND (v.state = ? OR v.candidate_knowledge" +
			"_id <> k.id)"), types.ConnectorTypeNextcloud, "tombstone").
		Where("COALESCE(k.metadata->>'nextcloud_etag', '') = ''").
		Where("NOT EXISTS (SELECT 1 FROM nextcloud_gc_jobs AS j WHERE j.knowledge_id = k.id)").
		Order("k.updated_at ASC, k.id ASC").Limit(limit).Scan(&sources).Error
	if err != nil {
		return 0, err
	}
	created := 0
	for _, source := range sources {
		inserted, err := s.inventoryOne(ctx, source, now)
		if err != nil {
			return created, err
		}
		if inserted {
			created++
		}
	}
	return created, nil
}

func (s *NextcloudGCStore) inventoryOne(ctx context.Context, source nextcloudGCSource, now time.Time) (bool, error) {
	inserted := false
	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var version nextcloudSourceVersion
		if err := nextcloudVersionQuery(tx.Clauses(clause.Locking{Strength: "UPDATE"}),
			source.TenantID, source.KnowledgeBaseID, source.DataSourceID, source.ExternalID).
			Take(&version).Error; err != nil {
			return err
		}
		if version.State != "tombstone" && version.CandidateKnowledgeID == source.ID {
			return nil
		}
		var current types.Knowledge
		if err := tx.Unscoped().Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("tenant_id = ? AND id = ?", source.TenantID, source.ID).Take(&current).Error; err != nil {
			return err
		}
		metadata, err := nextcloudMetadata(&current)
		if err != nil {
			return err
		}
		if current.Channel != types.ConnectorTypeNextcloud ||
			nextcloudMetadataString(metadata, "datasource_id") != source.DataSourceID ||
			nextcloudMetadataString(metadata, "external_id") != source.ExternalID ||
			nextcloudMetadataString(metadata, "nextcloud_etag") != "" {
			return nil
		}
		window := nextcloudGCIndexWindow
		reason := "retired"
		if current.ParseStatus == types.ParseStatusFailed || current.ParseStatus == types.ParseStatusCancelled {
			window = nextcloudGCFailedWindow
		}
		if version.State == "tombstone" {
			reason = "tombstone"
			window = 0
		}
		retiredAt := current.UpdatedAt.UTC()
		if current.DeletedAt.Valid {
			retiredAt = current.DeletedAt.Time.UTC()
		}
		if retiredAt.IsZero() || retiredAt.After(now) {
			retiredAt = now
		}
		job := nextcloudGCJob{
			ID: uuid.NewString(), TenantID: current.TenantID, KnowledgeBaseID: current.KnowledgeBaseID,
			DataSourceID: source.DataSourceID, ExternalID: source.ExternalID, KnowledgeID: current.ID,
			Reason: reason, State: "pending", NotBefore: retiredAt.Add(window),
			OriginalNotBefore: retiredAt.Add(nextcloudGCOriginalWindow),
			NextAttemptAt:     retiredAt.Add(window), CreatedAt: now, UpdatedAt: now,
		}
		if reason == "tombstone" {
			job.OriginalNotBefore = now
		}
		result := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&job)
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected == 0 {
			return nil
		}
		inserted = true
		items := []nextcloudGCItem{{
			JobID:     job.ID,
			Kind:      "knowledge",
			ObjectRef: current.ID,
			State:     "pending",
			CreatedAt: now,
			UpdatedAt: now,
		}}
		if current.FilePath != "" {
			items = append(items, nextcloudGCItem{
				JobID: job.ID, Kind: "source_file", ObjectRef: current.FilePath,
				State: "pending", EstimatedBytes: current.StorageSize, CreatedAt: now, UpdatedAt: now,
			})
		}
		var chunkRows []struct {
			ID        string
			ImageInfo string
		}
		if err := tx.Table(
			"chunks",
		).Select("id, image_info").Where(("tenant_id = ? AND knowledge_base_id = ? AND knowledge_i" +
			"d = ?"),
			current.TenantID, current.KnowledgeBaseID, current.ID).Find(&chunkRows).Error; err != nil {
			return err
		}
		if err := appendNextcloudGCEmbeddingItems(tx, job, now, &items); err != nil {
			return err
		}
		// A row with no local chunks or embeddings can still have a remote
		// index, graph or Wiki reference. Historical backend and lease proof
		// was never recorded, so every legacy version remains fail-closed.
		items = append(items, nextcloudGCItem{
			JobID: job.ID, Kind: "derived_index", ObjectRef: current.ID,
			State: "pending", CreatedAt: now, UpdatedAt: now,
		})
		for _, row := range chunkRows {
			items = append(items, nextcloudGCItem{
				JobID: job.ID, Kind: "derived_chunk", ObjectRef: row.ID,
				State: "pending", CreatedAt: now, UpdatedAt: now,
			})
		}
		images := make(map[string]bool)
		invalidInventory := false
		for _, row := range chunkRows {
			if row.ImageInfo == "" {
				continue
			}
			var info []types.ImageInfo
			if err := json.Unmarshal([]byte(row.ImageInfo), &info); err != nil {
				invalidInventory = true
				continue
			}
			for _, image := range info {
				if image.URL != "" && !images[image.URL] {
					images[image.URL] = true
					items = append(items, nextcloudGCItem{
						JobID: job.ID, Kind: "extracted_image", ObjectRef: image.URL,
						State: "pending", CreatedAt: now, UpdatedAt: now,
					})
				}
			}
		}
		if err := tx.CreateInBatches(&items, 100).Error; err != nil {
			return err
		}
		if invalidInventory {
			return tx.Model(&nextcloudGCJob{}).Where("id = ?", job.ID).
				Updates(map[string]any{
					"state": "blocked", "last_error_code": "invalid_image_inventory",
					"next_attempt_at": now.Add(nextcloudGCRetryWindow), "updated_at": now,
				}).Error
		}
		return nil
	})
	return inserted && err == nil, err
}

// RunDue is intentionally fail-closed for all stored objects and indexes.
// The current generic delete path can lose cleanup failures and races a new
// resource binding. Even an empty local inventory cannot prove that historical
// graph, Wiki or external index output is absent, so the job stays blocked
// after any individually proven local resource deletion. Its metadata is
// retained for the separate 90-day history policy.
func (s *NextcloudGCStore) RunDue(ctx context.Context, now time.Time, limit int) (int, error) {
	if limit <= 0 || limit > 200 {
		limit = 100
	}
	var ids []string
	if err := s.db.WithContext(ctx).Model(&nextcloudGCJob{}).
		Where("state IN ? AND not_before <= ? AND next_attempt_at <= ?", []string{
			"pending",
			"retry",
			"blocked",
		}, now, now).
		Order("next_attempt_at ASC, id ASC").Limit(limit).Pluck("id", &ids).Error; err != nil {
		return 0, err
	}
	processed := 0
	for _, id := range ids {
		if err := s.runOne(ctx, id, now); err != nil {
			// The failed phase did not commit. Preserve a bounded, non-sensitive
			// machine code so operators can see and retry it after restart.
			recordErr := s.db.WithContext(ctx).Model(&nextcloudGCJob{}).
				Where("id = ? AND state <> ?", id, "collected").
				Updates(map[string]any{
					"state": "retry", "last_error_code": "gc_step_failed",
					"next_attempt_at": now.Add(nextcloudGCRetryWindow), "updated_at": now,
					"attempts": gorm.Expr("attempts + 1"),
				}).Error
			return processed, errors.Join(err, recordErr)
		}
		if s.deleteObject != nil {
			if err := s.runPhysical(ctx, id, now); err != nil {
				return processed, err
			}
		}
		processed++
	}
	return processed, nil
}

func (s *NextcloudGCStore) runOne(ctx context.Context, id string, now time.Time) error {
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var job nextcloudGCJob
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("id = ?", id).Take(&job).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return nil
			}
			return err
		}
		if job.State == "collected" || now.Before(job.NotBefore) || now.Before(job.NextAttemptAt) {
			return nil
		}
		// Existing inventories may predate exact derived IDs, and a late
		// parser write may have appeared since the previous scan. Persist
		// every observed ID before evaluating the terminal state.
		if err := refreshNextcloudGCDerivedInventory(tx, job, now); err != nil {
			return err
		}
		// A chunk may gain image references after the first inventory. Rescan
		// before any physical claim so a late image is durably inventoried and
		// a newly malformed image_info fails closed, even if the previous scan
		// was valid.
		valid, err := refreshNextcloudGCImageInventory(tx, job, now)
		if err != nil {
			return err
		}
		if !valid {
			return tx.Model(&nextcloudGCJob{}).Where("id = ?", job.ID).
				Updates(map[string]any{
					"state": "blocked", "attempts": job.Attempts + 1,
					"last_error_code": "invalid_image_inventory",
					"next_attempt_at": now.Add(nextcloudGCRetryWindow), "updated_at": now,
				}).Error
		}
		var version nextcloudSourceVersion
		if err := nextcloudVersionQuery(tx.Clauses(clause.Locking{Strength: "UPDATE"}),
			job.TenantID, job.KnowledgeBaseID, job.DataSourceID, job.ExternalID).Take(&version).Error; err != nil {
			return err
		}
		code := ""
		switch {
		case version.State != "tombstone" && version.CandidateKnowledgeID == job.KnowledgeID:
			code = "candidate_is_current"
		default:
			var row types.Knowledge
			if err := tx.Unscoped().Clauses(clause.Locking{Strength: "UPDATE"}).
				Where("tenant_id = ? AND knowledge_base_id = ? AND id = ? AND channel = ?",
					job.TenantID, job.KnowledgeBaseID, job.KnowledgeID, types.ConnectorTypeNextcloud).
				Take(&row).Error; err != nil {
				return err
			}
			metadata, err := nextcloudMetadata(&row)
			if err != nil {
				return err
			}
			switch {
			case nextcloudMetadataString(metadata, "datasource_id") != job.DataSourceID ||
				nextcloudMetadataString(metadata, "external_id") != job.ExternalID ||
				nextcloudMetadataString(metadata, "nextcloud_etag") != "":
				code = "source_identity_changed"
			case row.ParseStatus == types.ParseStatusPending || row.ParseStatus == types.ParseStatusProcessing ||
				row.ParseStatus == types.ParseStatusFinalizing || row.PendingSubtasksCount > 0:
				code = "build_still_active"
			default:
				var pendingObjects int64
				if err := tx.Model(&nextcloudGCItem{}).
					Where("job_id = ? AND kind NOT IN ? AND state <> ?", job.ID,
						[]string{"knowledge", "derived_index"}, "collected").
					Count(&pendingObjects).Error; err != nil {
					return err
				}
				if row.FilePath != "" {
					var inventoried int64
					if err := tx.Model(&nextcloudGCItem{}).
						Where("job_id = ? AND kind = ? AND object_ref = ? AND state = ?",
							job.ID, "source_file", row.FilePath, "collected").Count(&inventoried).Error; err != nil {
						return err
					}
					if inventoried != 1 {
						pendingObjects++
					}
				}
				var chunks, bindings int64
				if err := tx.Table("chunks").Where("tenant_id = ? AND knowledge_base_id = ? AND knowledge_id = ?",
					job.TenantID, job.KnowledgeBaseID, job.KnowledgeID).
					Count(&chunks).Error; err != nil {
					return err
				}
				if err := tx.Model(&types.ResourceBinding{}).Where("tenant_id = ? AND owner_type = ? AND owner_id = ?",
					job.TenantID, types.ResourceOwnerKnowledge, job.KnowledgeID).Count(&bindings).Error; err != nil {
					return err
				}
				if pendingObjects > 0 || chunks > 0 || bindings > 0 || row.EmbeddingModelID != "" {
					code = "safe_object_cleanup_unavailable"
				} else {
					code = "derived_provenance_unverified"
				}
			}
		}
		if code == "" {
			return apperrors.NewProtocolError(errors.New(
				"nextcloud GC missing terminal state",
			), "Nextcloud GC missing terminal state")
		}
		if err := tx.Model(&nextcloudGCItem{}).Where("job_id = ? AND kind <> ? AND state NOT IN ?",
			job.ID, "knowledge", []string{"deleting", "collected"}).
			Updates(map[string]any{"state": "blocked", "updated_at": now}).Error; err != nil {
			return err
		}
		return tx.Model(&nextcloudGCJob{}).Where("id = ?", job.ID).
			Updates(map[string]any{
				"state": "blocked", "attempts": job.Attempts + 1,
				"last_error_code": code, "next_attempt_at": now.Add(nextcloudGCRetryWindow), "updated_at": now,
			}).Error
	})
}

// appendNextcloudGCEmbeddingItems records only rows in the application's own
// database. The knowledge-level derived_index item always remains pending
// because historical external backends and build/read leases are not recorded.
// These exact IDs are an inventory for a later backend-specific deletion
// protocol, never authority to delete by a knowledge/path prefix.
func appendNextcloudGCEmbeddingItems(tx *gorm.DB, job nextcloudGCJob, now time.Time, items *[]nextcloudGCItem) error {
	var table, kind string
	switch tx.Name() {
	case "postgres":
		table, kind = "embeddings", "postgres_embedding"
	case "sqlite":
		table, kind = "lite_embeddings", "sqlite_embedding"
	default:
		return nil
	}
	if !tx.Migrator().HasTable(table) {
		return nil
	}
	var rows []struct{ ID int64 }
	if err := tx.Table(table+" AS e").Select("e.id").
		Where("e.knowledge_base_id = ? AND e.knowledge_id = ?", job.KnowledgeBaseID, job.KnowledgeID).
		Where(("NOT EXISTS (SELECT 1 FROM nextcloud_gc_items AS i WHERE" +
			" i.job_id = ? AND i.kind = ? AND i.object_ref = CAST(e." +
			"id AS TEXT))"),
			job.ID, kind).Order("e.id ASC").Limit(nextcloudGCDerivedBatch).Find(&rows).Error; err != nil {
		return err
	}
	for _, row := range rows {
		*items = append(*items, nextcloudGCItem{
			JobID: job.ID, Kind: kind,
			ObjectRef: strconv.FormatInt(row.ID, 10), State: "pending", CreatedAt: now, UpdatedAt: now,
		})
	}
	return nil
}

func refreshNextcloudGCDerivedInventory(tx *gorm.DB, job nextcloudGCJob, now time.Time) error {
	items := make([]nextcloudGCItem, 0)
	var rows []struct{ ID string }
	if err := tx.Table("chunks AS c").Select("c.id").
		Where("c.tenant_id = ? AND c.knowledge_base_id = ? AND c.knowledge_id = ?",
			job.TenantID, job.KnowledgeBaseID, job.KnowledgeID).
		Where(("NOT EXISTS (SELECT 1 FROM nextcloud_gc_items AS i WHERE" +
			" i.job_id = ? AND i.kind = ? AND i.object_ref = c.id)"),
			job.ID, "derived_chunk").Order("c.id ASC").Limit(nextcloudGCDerivedBatch).Find(&rows).Error; err != nil {
		return err
	}
	for _, row := range rows {
		items = append(items, nextcloudGCItem{
			JobID: job.ID, Kind: "derived_chunk",
			ObjectRef: row.ID, State: "pending", CreatedAt: now, UpdatedAt: now,
		})
	}
	if err := appendNextcloudGCEmbeddingItems(tx, job, now, &items); err != nil {
		return err
	}
	items = append(items, nextcloudGCItem{
		JobID: job.ID, Kind: "derived_index",
		ObjectRef: job.KnowledgeID, State: "pending", CreatedAt: now, UpdatedAt: now,
	})
	if len(items) == 0 {
		return nil
	}
	return tx.Clauses(clause.OnConflict{DoNothing: true}).CreateInBatches(&items, 100).Error
}

func refreshNextcloudGCImageInventory(tx *gorm.DB, job nextcloudGCJob, now time.Time) (bool, error) {
	var rows []struct{ ImageInfo string }
	if err := tx.Table("chunks").Select("image_info").Where(("tenant_id = ? AND knowledge_base_id = ? AND knowledge_i" +
		"d = ?"),
		job.TenantID, job.KnowledgeBaseID, job.KnowledgeID).Find(&rows).Error; err != nil {
		return false, err
	}
	for _, row := range rows {
		if row.ImageInfo == "" {
			continue
		}
		var images []types.ImageInfo
		if err := json.Unmarshal([]byte(row.ImageInfo), &images); err != nil {
			return false, nil
		}
		for _, image := range images {
			if image.URL == "" {
				continue
			}
			item := nextcloudGCItem{
				JobID: job.ID, Kind: "extracted_image", ObjectRef: image.URL,
				State: "pending", CreatedAt: now, UpdatedAt: now,
			}
			if err := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&item).Error; err != nil {
				return false, err
			}
		}
	}
	return true, nil
}

// ListStatus returns bounded, tenant-scoped progress with no object locator.
func (s *NextcloudGCStore) ListStatus(ctx context.Context, tenantID uint64, limit int) ([]NextcloudGCStatus, error) {
	if tenantID == 0 {
		return nil, apperrors.NewProtocolError(errors.New(
			"nextcloud GC status requires a tenant",
		), "Nextcloud GC status requires a tenant")
	}
	if limit <= 0 || limit > 200 {
		limit = 100
	}
	var jobs []nextcloudGCJob
	if err := s.db.WithContext(ctx).Where("tenant_id = ?", tenantID).
		Order("created_at DESC, id DESC").Limit(limit).Find(&jobs).Error; err != nil {
		return nil, err
	}
	status := make([]NextcloudGCStatus, 0, len(jobs))
	for _, job := range jobs {
		var aggregate struct {
			PendingItems           int64
			EstimatedBytes         int64
			ConfirmedReleasedBytes int64
		}
		if err := s.db.WithContext(ctx).Model(&nextcloudGCItem{}).
			Select(("SUM(CASE WHEN state <> 'collected' THEN 1 ELSE 0 END) A" +
				"S pending_items, COALESCE(SUM(estimated_bytes), 0) AS e" +
				"stimated_bytes, COALESCE(SUM(confirmed_released_bytes)," +
				" 0) AS confirmed_released_bytes")).
			Where("job_id = ?", job.ID).Scan(&aggregate).Error; err != nil {
			return nil, err
		}
		status = append(status, NextcloudGCStatus{
			ID: job.ID, TenantID: job.TenantID,
			KnowledgeBaseID: job.KnowledgeBaseID, KnowledgeID: job.KnowledgeID,
			Reason: job.Reason, State: job.State, NotBefore: job.NotBefore,
			OriginalNotBefore: job.OriginalNotBefore, NextAttemptAt: job.NextAttemptAt,
			Attempts: job.Attempts, LastErrorCode: job.LastErrorCode,
			PendingItems: aggregate.PendingItems, EstimatedBytes: aggregate.EstimatedBytes,
			ConfirmedReleasedBytes: aggregate.ConfirmedReleasedBytes, CompletedAt: job.CompletedAt,
		})
	}
	return status, nil
}

// RetryJob brings one tenant-owned failed inventory forward without bypassing
// its safety window. It does not turn a blocked physical deletion into a
// successful one; the next sweep will re-check and report the same blocker.
func (s *NextcloudGCStore) RetryJob(ctx context.Context, tenantID uint64, id string, now time.Time) (bool, error) {
	if tenantID == 0 || id == "" {
		return false, nil
	}
	result := s.db.WithContext(ctx).Model(&nextcloudGCJob{}).
		Where("id = ? AND tenant_id = ? AND state IN ?", id, tenantID, []string{"blocked", "retry"}).
		Updates(map[string]any{"state": "retry", "next_attempt_at": now, "updated_at": now})
	return result.RowsAffected > 0, result.Error
}
