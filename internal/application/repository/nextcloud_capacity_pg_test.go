package repository

import (
	"context"
	"os"
	"testing"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
)

func TestPostgresNextcloudCapacityConcurrentPhysicalAndQuota(t *testing.T) {
	db := leaseTestPostgres(t)
	require.NoError(
		t,
		db.AutoMigrate(&types.KnowledgeBase{}, &types.Knowledge{}, &types.StoredResource{}, &types.ResourceBinding{}),
	)
	sql, err := os.ReadFile("../../../migrations/versioned/000146_nextcloud_capacity_admission.up.sql")
	require.NoError(t, err)
	require.NoError(t, db.Exec(string(sql)).Error)
	intentSQL, intentErr := os.ReadFile("../../../migrations/versioned/000148_nextcloud_capacity_write_intents.up.sql")
	require.NoError(t, intentErr)
	require.NoError(t, db.Exec(string(intentSQL)).Error)
	backend := "backend"
	require.NoError(t, db.Create(&types.KnowledgeBase{ID: "kb", TenantID: 7, StorageBackendID: &backend}).Error)
	s := NewNextcloudCapacityStore(db)
	out := make(chan error, 2)
	for _, key := range []string{"pg-a", "pg-b"} {
		go func(k string) {
			_, err := s.Reserve(context.Background(), 7, "kb", "backend", k, 100, capMeasure(250))
			out <- err
		}(key)
	}
	a, b := <-out, <-out
	require.True(t, (a == nil && b != nil) || (b == nil && a != nil))
	if a != nil {
		require.ErrorIs(t, a, ErrNextcloudCapacityPaused)
	} else {
		require.ErrorIs(t, b, ErrNextcloudCapacityPaused)
	}
	var count int64
	require.NoError(t, db.Model(&NextcloudCapacityReservation{}).Count(&count).Error)
	require.Equal(t, int64(1), count)
}
