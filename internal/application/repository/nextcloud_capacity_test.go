package repository

import (
	"context"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func capacityFixture(t *testing.T) (*NextcloudCapacityStore, *gorm.DB) {
	t.Helper()
	db, err := gorm.Open(sqlite.Open(filepath.Join(t.TempDir(), "capacity.db")), &gorm.Config{})
	require.NoError(t, err)
	require.NoError(t, db.AutoMigrate(&types.KnowledgeBase{}, &types.Knowledge{}, &types.StoredResource{}, &types.ResourceBinding{}))
	script, err := os.ReadFile("../../../migrations/sqlite/000065_nextcloud_capacity_admission.up.sql")
	require.NoError(t, err)
	require.NoError(t, db.Exec(string(script)).Error)
	intentSQL, intentErr := os.ReadFile("../../../migrations/sqlite/000067_nextcloud_capacity_write_intents.up.sql")
	require.NoError(t, intentErr)
	require.NoError(t, db.Exec(string(intentSQL)).Error)
	backend := "backend"
	require.NoError(t, db.Create(&types.KnowledgeBase{ID: "kb", TenantID: 7, StorageBackendID: &backend}).Error)
	return NewNextcloudCapacityStore(db), db
}
func capMeasure(available int64) types.StorageCapacity {
	return types.StorageCapacity{ScopeID: strings.Repeat("a", 64), TotalBytes: 1000, AvailableBytes: available, MeasuredAt: time.Now().UTC()}
}
func TestNextcloudCapacityWatermarkQuotaAndCurrentVersionsPreserved(t *testing.T) {
	s, db := capacityFixture(t)
	ctx := context.Background()
	r, err := s.Reserve(ctx, 7, "kb", "backend", "first", 100, capMeasure(300))
	require.NoError(t, err)
	require.NotNil(t, r)
	_, err = s.Reserve(ctx, 7, "kb", "backend", "second", 100, capMeasure(300))
	require.ErrorIs(t, err, ErrNextcloudCapacityPaused)
	var row types.KnowledgeBase
	require.NoError(t, db.Where("id='kb'").Take(&row).Error)
	require.Equal(t, "kb", row.ID)
	require.NoError(t, s.SetPolicy(ctx, NextcloudCapacityPolicy{TenantID: 7, KnowledgeBaseID: "kb", RetentionQuotaBytes: 110, WarningPercent: 80, PausePercent: 90}))
	_, err = s.Reserve(ctx, 7, "kb", "backend", "quota", 20, capMeasure(1000))
	require.ErrorIs(t, err, ErrNextcloudRetentionQuota)
	stale := capMeasure(1000)
	stale.MeasuredAt = time.Now().Add(-time.Minute)
	_, err = s.Reserve(ctx, 7, "kb", "backend", "stale", 1, stale)
	require.ErrorIs(t, err, ErrNextcloudCapacityUnavailable)
}
func TestNextcloudCapacityExpiredReservationsRemainCharged(t *testing.T) {
	s, db := capacityFixture(t)
	r, err := s.Reserve(context.Background(), 7, "kb", "backend", "first", 100, capMeasure(300))
	require.NoError(t, err)
	require.NoError(t, db.Model(&NextcloudCapacityReservation{}).Where("token=?", r.Token).Update("lease_until_ms", 1).Error)
	_, err = s.Reserve(context.Background(), 7, "kb", "backend", "second", 100, capMeasure(300))
	require.ErrorIs(t, err, ErrNextcloudCapacityPaused)
}

func TestNextcloudCapacityConcurrentReservationsAndObjectReceipt(t *testing.T) {
	s, db := capacityFixture(t)
	ctx := context.Background()
	require.NoError(t, db.Exec("PRAGMA busy_timeout=5000").Error)
	var outcomes = make(chan error, 2)
	for _, key := range []string{"race-a", "race-b"} {
		go func(k string) { _, err := s.Reserve(ctx, 7, "kb", "backend", k, 100, capMeasure(250)); outcomes <- err }(key)
	}
	first, second := <-outcomes, <-outcomes
	require.True(t, (first == nil && second != nil) || (second == nil && first != nil), "both concurrent admissions consumed final capacity")
	var rows []NextcloudCapacityReservation
	require.NoError(t, db.Find(&rows).Error)
	require.Len(t, rows, 1)
	require.ErrorIs(t, s.Finish(ctx, &rows[0], "doc", true), ErrNextcloudCapacityUnavailable)
	require.NoError(t, db.Create(&types.Knowledge{ID: "doc", TenantID: 7, KnowledgeBaseID: "kb"}).Error)
	resource := types.StoredResource{ID: "resource", Handle: "1234567890123456789012", TenantID: 7, StorageBackendID: "backend", Provider: "local", PhysicalPath: "backend://backend/local://7/doc/file", LocationHash: strings.Repeat("b", 64), Size: 100, State: "active"}
	require.NoError(t, db.Create(&resource).Error)
	require.NoError(t, db.Create(&types.ResourceBinding{ResourceID: resource.ID, TenantID: 7, OwnerType: "knowledge", OwnerID: "doc", Relation: "source_file"}).Error)
	require.NoError(t, s.Finish(ctx, &rows[0], "doc", true))
	staleMeasurement := capMeasure(250)
	staleMeasurement.MeasuredAt = time.Now().Add(-time.Millisecond)
	_, err := s.Reserve(ctx, 7, "kb", "backend", "stale-after-write", 100, staleMeasurement)
	require.ErrorIs(t, err, ErrNextcloudCapacityPaused, "recent committed upload must remain charged until a newer physical measurement")
	status, err := s.Status(ctx, 7, "kb", "backend", capMeasure(1000))
	require.NoError(t, err)
	require.Equal(t, int64(100), status.LogicalRetainedBytes)
	require.Equal(t, int64(100), status.PendingBytes)
	require.NoError(t, db.Model(&NextcloudCapacityReservation{}).Where("token=?", rows[0].Token).Update("committed_at_ms", time.Now().Add(-2*time.Second).UnixMilli()).Error)
	status, err = s.Status(ctx, 7, "kb", "backend", capMeasure(1000))
	require.NoError(t, err)
	require.Zero(t, status.PendingBytes)
}
