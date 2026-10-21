package repository

import (
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func TestDirectoryUsableHonorsConfiguredStaleWindow(t *testing.T) {
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	require.NoError(t, err)
	require.NoError(t, db.AutoMigrate(&types.Directory{}))

	now := time.Date(2026, 10, 22, 1, 0, 0, 0, time.UTC)
	lastSuccessfulSync := now.Add(-301 * time.Second)
	directory := types.Directory{
		ID:                   "directory-stale-window",
		Enabled:              true,
		LastSuccessfulSyncAt: &lastSuccessfulSync,
		StaleAfterSeconds:    900,
	}
	require.NoError(t, db.Create(&directory).Error)

	require.NoError(t, directoryUsable(db, directory.ID, now),
		"a successful snapshot must remain usable until the configured stale window")

	require.NoError(t, db.Model(&directory).Update("stale_after_seconds", 300).Error)
	require.ErrorIs(t, directoryUsable(db, directory.ID, now), ErrNextcloudOnboardingDenied)

	require.NoError(t, db.Model(&directory).Updates(map[string]any{
		"stale_after_seconds": 900,
		"last_sync_error":     "last refresh failed",
	}).Error)
	require.ErrorIs(t, directoryUsable(db, directory.ID, now), ErrNextcloudOnboardingDenied)
}
