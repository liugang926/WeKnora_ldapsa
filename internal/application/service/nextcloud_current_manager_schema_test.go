package service

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// Current module fixtures install the actual append-only manager schema, even
// when a legitimate independently configured generic pair has no manager row.
// A missing pair of tables is never a legacy or synthetic authority shortcut.
func installMachineCurrentManagerSchema(t *testing.T, db, migration *gorm.DB) {
	t.Helper()
	if db.Migrator().HasTable("nextcloud_onboarding_manager_purposes") {
		require.True(t, db.Migrator().HasTable("nextcloud_onboarding_manager_heads"))
		return
	}
	path := "../../../migrations/sqlite/000083_nextcloud_current_manager_purpose.up.sql"
	if db.Name() == "postgres" {
		path = "../../../migrations/versioned/000164_nextcloud_current_manager_purpose.up.sql"
	}
	ddl, err := os.ReadFile(path)
	require.NoError(t, err)
	require.NoError(t, migration.Exec(string(ddl)).Error)
}
