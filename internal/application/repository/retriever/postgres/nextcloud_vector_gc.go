package postgres

import (
	"context"
	"errors"
	"strconv"

	apprepo "github.com/Tencent/WeKnora/internal/application/repository"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// DeleteNextcloudExactVectorForGC is a backend-specific local-row operation.
// It is deliberately not called by the generic GC scheduler: global derived
// cleanup still requires complete reader/build coverage and backend proofs.
// The caller must hold a live exact GC claim. Deletion and the item receipt
// commit together; retries after a crash are safe and never complete the job.
func (g *pgRepository) DeleteNextcloudExactVectorForGC(ctx context.Context,
	claim apprepo.NextcloudContentGCClaim, jobID, vectorID string,
) error {
	id, err := strconv.ParseUint(vectorID, 10, 64)
	if err != nil || id == 0 || strconv.FormatUint(id, 10) != vectorID || jobID == "" {
		return apprepo.ErrNextcloudContentLeaseInvalid
	}
	return g.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := apprepo.NewNextcloudContentLeaseStore(g.db).
			ValidateGCClaimInTx(ctx, tx, claim); err != nil {
			return err
		}
		var job struct {
			TenantID                       uint64
			KnowledgeBaseID                string
			DataSourceID                   string `gorm:"column:datasource_id"`
			ExternalID, KnowledgeID, State string
		}
		if err := tx.Table("nextcloud_gc_jobs").Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("id = ?", jobID).Take(&job).Error; err != nil {
			return err
		}
		scope := claim.Scope
		if job.TenantID != scope.TenantID || job.KnowledgeBaseID != scope.KnowledgeBaseID ||
			job.DataSourceID != scope.DataSourceID || job.ExternalID != scope.ExternalID ||
			job.KnowledgeID != scope.KnowledgeID || job.State == "collected" {
			return apprepo.ErrNextcloudContentLeaseDenied
		}
		// The claim validates the caller's time argument, but the GC job owns
		// the policy delay. Keep its row locked until deletion and receipt commit
		// so a concurrent schedule extension cannot be crossed.
		var schedule struct{ Due bool }
		if err := tx.Raw(`SELECT not_before <= clock_timestamp() AS due
			FROM nextcloud_gc_jobs WHERE id = ?`, jobID).Scan(&schedule).Error; err != nil {
			return err
		}
		if !schedule.Due {
			return apprepo.ErrNextcloudContentGCBusy
		}
		var item struct{ State string }
		if err := tx.Table("nextcloud_gc_items").Clauses(clause.Locking{Strength: "UPDATE"}).
			Select("state").Where("job_id = ? AND kind = 'postgres_embedding' AND object_ref = ?",
			jobID, vectorID).Take(&item).Error; err != nil {
			return err
		}
		var row pgVector
		err := tx.Select("id", "knowledge_base_id", "knowledge_id").Where("id = ?", id).Take(&row).Error
		if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}
		if err == nil && (row.KnowledgeBaseID != scope.KnowledgeBaseID || row.KnowledgeID != scope.KnowledgeID) {
			return apprepo.ErrNextcloudContentLeaseDenied
		}
		if item.State == "collected" {
			if err == nil {
				return apprepo.ErrNextcloudContentLeaseDenied
			}
			return nil
		}
		if err == nil {
			if err := tx.Where("id = ? AND knowledge_base_id = ? AND knowledge_id = ?",
				id, scope.KnowledgeBaseID, scope.KnowledgeID).Delete(&pgVector{}).Error; err != nil {
				return err
			}
		}
		return tx.Table("nextcloud_gc_items").Where(
			"job_id = ? AND kind = 'postgres_embedding' AND object_ref = ?", jobID, vectorID).
			Update("state", "collected").Error
	})
}
