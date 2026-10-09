package repository

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// DeleteNextcloudExactChunkForGC is a dormant, exact-row SQL adapter. The GC
// scheduler does not call it: complete reader/writer coverage, including user
// chunk edits, must be proven before physical derived cleanup is enabled.
// A live generation claim, persisted job and item, chunk revisions, and the
// receipt are checked and changed in one transaction. This never completes a
// job or its conservative derived_index blocker.
func (s *NextcloudGCStore) DeleteNextcloudExactChunkForGC(ctx context.Context,
	claim NextcloudContentGCClaim, jobID, chunkID string,
) error {
	if s == nil || s.db == nil || !claim.Scope.valid() || jobID == "" || chunkID == "" {
		return ErrNextcloudContentLeaseInvalid
	}
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := NewNextcloudContentLeaseStore(s.db).ValidateGCClaimInTx(ctx, tx, claim); err != nil {
			return err
		}
		var job nextcloudGCJob
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("id = ?", jobID).Take(&job).Error; err != nil {
			return err
		}
		scope := claim.Scope
		if job.TenantID != scope.TenantID || job.KnowledgeBaseID != scope.KnowledgeBaseID ||
			job.DataSourceID != scope.DataSourceID || job.ExternalID != scope.ExternalID ||
			job.KnowledgeID != scope.KnowledgeID || job.State == "collected" ||
			job.NotBefore.IsZero() {
			return ErrNextcloudContentLeaseDenied
		}
		nowMS, err := nextcloudLeaseNowMS(tx)
		if err != nil {
			return err
		}
		if nowMS < job.NotBefore.UnixMilli() {
			return ErrNextcloudContentGCBusy
		}
		var version nextcloudSourceVersion
		if err := nextcloudVersionQuery(tx, scope.TenantID, scope.KnowledgeBaseID,
			scope.DataSourceID, scope.ExternalID).Take(&version).Error; err != nil {
			return err
		}
		if version.State != "tombstone" && version.CandidateKnowledgeID == scope.KnowledgeID {
			return ErrNextcloudContentLeaseDenied
		}
		var knowledge types.Knowledge
		if err := tx.Unscoped().Where("tenant_id = ? AND knowledge_base_id = ? AND id = ? AND channel = ?",
			scope.TenantID, scope.KnowledgeBaseID, scope.KnowledgeID,
			types.ConnectorTypeNextcloud).Take(&knowledge).Error; err != nil {
			return err
		}
		metadata, err := nextcloudMetadata(&knowledge)
		if err != nil || nextcloudMetadataString(metadata, "datasource_id") != scope.DataSourceID ||
			nextcloudMetadataString(metadata, "external_id") != scope.ExternalID ||
			nextcloudMetadataString(metadata, "nextcloud_etag") != "" {
			return ErrNextcloudContentLeaseDenied
		}
		var item nextcloudGCItem
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where(
			"job_id = ? AND kind = ? AND object_ref = ?", jobID, "derived_chunk", chunkID,
		).Take(&item).Error; err != nil {
			return err
		}
		if item.State != "pending" && item.State != "blocked" && item.State != "collected" {
			return ErrNextcloudContentLeaseDenied
		}
		var chunk struct {
			TenantID        uint64
			KnowledgeBaseID string
			KnowledgeID     string
			ImageInfo       string
		}
		chunkErr := tx.Table("chunks").Clauses(clause.Locking{Strength: "UPDATE"}).
			Select("tenant_id", "knowledge_base_id", "knowledge_id", "image_info").
			Where("id = ?", chunkID).Take(&chunk).Error
		if chunkErr != nil && !errors.Is(chunkErr, gorm.ErrRecordNotFound) {
			return chunkErr
		}
		if chunkErr == nil && (chunk.TenantID != scope.TenantID ||
			chunk.KnowledgeBaseID != scope.KnowledgeBaseID || chunk.KnowledgeID != scope.KnowledgeID) {
			return ErrNextcloudContentLeaseDenied
		}
		var foreignRevisions int64
		if err := tx.Table("chunk_revisions").Where(
			"chunk_id = ? AND (tenant_id <> ? OR knowledge_base_id <> ? OR knowledge_id <> ?)",
			chunkID, scope.TenantID, scope.KnowledgeBaseID, scope.KnowledgeID,
		).Count(&foreignRevisions).Error; err != nil {
			return err
		}
		if foreignRevisions != 0 {
			return ErrNextcloudContentLeaseDenied
		}
		var revisions int64
		if err := tx.Table("chunk_revisions").Where(("chunk_id = ? AND tenant_id = ? AND knowledge_base_id = " +
			"? AND knowledge_id = ?"),
			chunkID, scope.TenantID, scope.KnowledgeBaseID, scope.KnowledgeID).Count(&revisions).Error; err != nil {
			return err
		}
		if item.State == "collected" {
			if chunkErr == nil || revisions != 0 {
				return ErrNextcloudContentLeaseDenied
			}
			return nil
		}
		if chunkErr == nil && chunk.ImageInfo != "" {
			var images []types.ImageInfo
			if err := json.Unmarshal([]byte(chunk.ImageInfo), &images); err != nil {
				return ErrNextcloudContentLeaseDenied
			}
			for _, image := range images {
				if image.URL == "" {
					continue
				}
				var inventoried int64
				if err := tx.Table("nextcloud_gc_items").Where(
					"job_id = ? AND kind = ? AND object_ref = ?", jobID, "extracted_image", image.URL,
				).Count(&inventoried).Error; err != nil {
					return err
				}
				if inventoried != 1 {
					return ErrNextcloudContentLeaseDenied
				}
			}
		}
		if err := tx.Where("chunk_id = ? AND tenant_id = ? AND knowledge_base_id = ? AND knowledge_id = ?",
			chunkID, scope.TenantID, scope.KnowledgeBaseID, scope.KnowledgeID).
			Delete(&types.ChunkRevision{}).Error; err != nil {
			return err
		}
		if chunkErr == nil {
			if err := tx.Unscoped().Where("id = ? AND tenant_id = ? AND knowledge_base_id = ? AND knowledge_id = ?",
				chunkID, scope.TenantID, scope.KnowledgeBaseID, scope.KnowledgeID).
				Delete(&types.Chunk{}).Error; err != nil {
				return err
			}
		}
		result := tx.Model(&nextcloudGCItem{}).Where(
			"job_id = ? AND kind = ? AND object_ref = ? AND state IN ?",
			jobID, "derived_chunk", chunkID, []string{"pending", "blocked"},
		).Updates(map[string]any{"state": "collected", "updated_at": time.UnixMilli(nowMS).UTC()})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return ErrNextcloudContentLeaseDenied
		}
		return nil
	})
}
