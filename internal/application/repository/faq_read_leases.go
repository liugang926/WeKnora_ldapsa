package repository

import (
	"context"
	"fmt"

	"github.com/Tencent/WeKnora/internal/types"
	"gorm.io/gorm"
)

type faqReadLeaseCoverageKey struct{}
type faqReadLeaseCoverage struct{ Leases []NextcloudContentLease }

// IDs here are private server state. Every capture verifies their actual SQL
// row, owner, scope, epoch, expiry and open fence in its existing transaction.
// Merely binding plan metadata or manufacturing a lease struct cannot cover a read.
func WithFAQReadLeaseCoverage(ctx context.Context, leases []NextcloudContentLease) (context.Context, error) {
	if len(leases) == 0 || len(leases) > 16384 {
		return ctx, ErrNextcloudContentLeaseInvalid
	}
	actual := append([]NextcloudContentLease{}, leases...)
	for _, lease := range actual {
		if lease.ID == "" || lease.OwnerID == "" || lease.Kind != NextcloudContentReadLease {
			return ctx, ErrNextcloudContentLeaseInvalid
		}
	}
	return context.WithValue(ctx, faqReadLeaseCoverageKey{}, faqReadLeaseCoverage{actual}), nil
}

func validateFAQReadLeaseCoverageInTx(tx *gorm.DB, scopes []types.KnowledgeSearchScope, lineage *types.SourceLineage) error {
	coverage, ok := tx.Statement.Context.Value(faqReadLeaseCoverageKey{}).(faqReadLeaseCoverage)
	if !ok || len(coverage.Leases) == 0 || lineage.RequireComplete() != nil {
		return ErrNextcloudContentGCUncovered
	}
	now, err := nextcloudLeaseNowMS(tx)
	if err != nil {
		return err
	}
	broad := map[string]bool{}
	exact := map[string]uint64{}
	for _, given := range coverage.Leases {
		var row nextcloudContentLeaseRow
		if err := tx.Where("lease_id=?", given.ID).Take(&row).Error; err != nil {
			return ErrNextcloudContentLeaseExpired
		}
		if row.Kind != string(NextcloudContentReadLease) || row.ReleasedAtMS != nil || row.ExpiresAtMS <= now || row.OwnerID != given.OwnerID || row.TenantID != given.TenantID || row.KnowledgeBaseID != given.KnowledgeBaseID || row.KnowledgeID != given.KnowledgeID || row.Epoch != given.Epoch {
			return ErrNextcloudContentLeaseExpired
		}
		var kb nextcloudContentFence
		if err := tx.Where("tenant_id=? AND knowledge_base_id=? AND knowledge_id=''", row.TenantID, row.KnowledgeBaseID).Take(&kb).Error; err != nil || kb.State != "open" {
			return ErrNextcloudContentLeaseDenied
		}
		key := fmt.Sprintf("%d:%s", row.TenantID, row.KnowledgeBaseID)
		if row.KnowledgeID == "" {
			if kb.Epoch != row.Epoch {
				return ErrNextcloudContentLeaseDenied
			}
			broad[key] = true
		} else {
			var fence nextcloudContentFence
			if err := tx.Where("tenant_id=? AND knowledge_base_id=? AND knowledge_id=?", row.TenantID, row.KnowledgeBaseID, row.KnowledgeID).Take(&fence).Error; err != nil || fence.State != "open" || fence.Epoch != row.Epoch {
				return ErrNextcloudContentLeaseDenied
			}
			exact[fmt.Sprintf("%s:%s:%s:%s", key, row.KnowledgeID, fence.DataSourceID, fence.ExternalID)] = uint64(row.Epoch)
		}
	}
	for _, scope := range scopes {
		if !broad[fmt.Sprintf("%d:%s", scope.TenantID, scope.KBID)] {
			return ErrNextcloudContentGCUncovered
		}
	}
	for _, source := range lineage.Sources {
		key := fmt.Sprintf("%d:%s:%s:%s:%s", source.TenantID, source.KnowledgeBaseID, source.KnowledgeID, source.DataSourceID, source.ExternalID)
		if exact[key] != source.Revision {
			return ErrNextcloudContentGCUncovered
		}
	}
	return nil
}
