package service

import (
	"fmt"
	"testing"

	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/stretchr/testify/require"
)

func TestRAGFixtureCapacityPolicyAndIsolation(t *testing.T) {
	for i := 0; i < 2; i++ {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			m := newMachinePipelineFixtureWithDocument(t, false, "Current employee policy", true, false)
			measured, err := m.files.(interfaces.StorageCapacityProvider).MeasureStorageCapacity(m.f.ctx)
			require.NoError(t, err)
			require.Positive(t, measured.AvailableBytes)
			var kb types.KnowledgeBase
			require.NoError(t, m.f.db.Where("id = ?", "kb").Take(&kb).Error)
			// The fixture still uses real storage measurements and the production
			// admission path: impossible allocations are rejected without writing bytes.
			_, err = m.source.capacityAdmission.Begin(m.f.ctx, &kb, m.ds.ID, "oversized", "etag", measured.TotalBytes+1)
			require.ErrorIs(t, err, repository.ErrNextcloudCapacityPaused)
			var pending int64
			require.NoError(t, m.f.db.Model(&repository.NextcloudCapacityReservation{}).
				Where("state = ?", "pending").Count(&pending).Error)
			require.Zero(t, pending)
			store := repository.NewNextcloudCapacityStore(m.f.db)
			policy := repository.NextcloudCapacityPolicy{
				TenantID: 7, KnowledgeBaseID: "kb", WarningPercent: 99, PausePercent: 100,
				RetentionQuotaBytes: 1,
			}
			require.NoError(t, store.SetPolicy(m.f.ctx, policy))
			_, err = m.source.capacityAdmission.Begin(m.f.ctx, &kb, m.ds.ID, "quota", "etag", 2)
			require.ErrorIs(t, err, repository.ErrNextcloudRetentionQuota)
			policy.RetentionQuotaBytes = 0
			require.NoError(t, store.SetPolicy(m.f.ctx, policy))
			k := m.ingest(t)
			require.NotEmpty(t, k.FilePath)
		})
	}
}
