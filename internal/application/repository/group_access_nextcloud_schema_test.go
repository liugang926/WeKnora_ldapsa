package repository_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	repo "github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func TestGroupAccessMissingOnboardingLedgerPreservesSchemaBoundary(t *testing.T) {
	for _, marker := range []string{"legacy", "session", "credential", "control"} {
		t.Run(marker, func(t *testing.T) {
			db, err := gorm.Open(sqlite.Open(filepath.Join(t.TempDir(), "roles.db")), &gorm.Config{})
			require.NoError(t, err)
			sqlDB, err := db.DB()
			require.NoError(t, err)
			t.Cleanup(func() { _ = sqlDB.Close() })
			require.NoError(t, db.AutoMigrate(&types.NextcloudOnboardingOperation{}))
			switch marker {
			case "legacy":
				require.NoError(t, db.Exec("CREATE TABLE sessions(id TEXT PRIMARY KEY)").Error)
			case "session":
				require.NoError(
					t,
					db.Exec(
						"CREATE TABLE sessions(id TEXT PRIMARY KEY, nextcloud_scoped BOOLEAN NOT NULL DEFAULT FALSE)",
					).Error,
				)
			case "credential":
				require.NoError(t, db.Exec("CREATE TABLE nextcloud_onboarding_credentials(id TEXT PRIMARY KEY)").Error)
			case "control":
				require.NoError(
					t,
					db.Exec("CREATE TABLE nextcloud_control_recovery_gates(instance_id TEXT PRIMARY KEY)").Error,
				)
			}
			require.NoError(t, db.Migrator().DropTable(&types.NextcloudOnboardingOperation{}))
			access := repo.NewGroupAccessRepository(db).(interface {
				ScopedMatches(
					context.Context,
					string,
					uint64,
					string,
					time.Time,
				) ([]types.NextcloudScopedGrantMatch, error)
				TenantScoped(context.Context, uint64) (bool, error)
				ManagedKB(context.Context, uint64, string) (bool, error)
			})
			matches, matchesErr := access.ScopedMatches(context.Background(), "reader", 7, "kb", time.Now())
			scoped, scopedErr := access.TenantScoped(context.Background(), 7)
			managed, managedErr := access.ManagedKB(context.Background(), 7, "kb")
			require.Empty(t, matches)
			require.False(t, scoped)
			require.False(t, managed)
			for _, err := range []error{matchesErr, scopedErr, managedErr} {
				if marker == "legacy" {
					require.NoError(t, err)
				} else {
					require.ErrorIs(t, err, repo.ErrNextcloudOnboardingDenied)
				}
			}
		})
	}
}
