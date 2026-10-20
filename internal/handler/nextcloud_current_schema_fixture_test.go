package handler

import (
	"os"
	"strings"
	"testing"

	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// HTTP fixtures use current Session models, including nextcloud_scoped. The
// corresponding production controls must exist before any real fence/read or
// source admission; an absent module is not a legacy authorization shortcut.
func installHTTPCurrentNextcloudControls(t *testing.T, db *gorm.DB) {
	t.Helper()
	if db.Migrator().HasTable("nextcloud_onboarding_operations") {
		for _, table := range []string{
			"nextcloud_control_recovery_gates", "nextcloud_onboarding_target_closures",
			"nextcloud_onboarding_tenant_deletions", "nextcloud_onboarding_manager_purposes",
			"nextcloud_onboarding_manager_heads", "nextcloud_machine_tasks",
		} {
			require.True(t, db.Migrator().HasTable(table), "current HTTP fixture controls incomplete: %s", table)
		}
		return
	}
	require.NoError(t, db.AutoMigrate(&types.Tenant{}, &types.DataSource{},
		&repository.NextcloudSourcePairing{}, &types.AuthToken{}, &types.WikiPage{}))
	for _, migration := range []struct{ table, name string }{
		{"nextcloud_source_versions", "000031_nextcloud_source_versions"},
		{"nextcloud_source_revisions", "000045_nextcloud_source_revisions"},
		{"nextcloud_content_fences", "000046_nextcloud_content_leases"},
		{"nextcloud_event_connections", "000032_nextcloud_event_inbox"},
		{"nextcloud_publication_recovery_receipts", "000064_nextcloud_publication_recovery"},
		{"nextcloud_root_migrations", "000076_nextcloud_directory_migrations"},
		{"nextcloud_control_recovery_gates", "000078_nextcloud_control_recovery"},
		{"nextcloud_recovery_build_leases", "000079_nextcloud_recovery_build_leases"},
		{"nextcloud_onboarding_operations", "000074_nextcloud_onboarding"},
		{"nextcloud_onboarding_target_closures", "000081_nextcloud_target_closure"},
		{"nextcloud_onboarding_manager_purposes", "000083_nextcloud_current_manager_purpose"},
		{"nextcloud_machine_tasks", "000084_nextcloud_machine_tasks"},
	} {
		if db.Migrator().HasTable(migration.table) {
			continue
		}
		ddl, err := os.ReadFile("../../migrations/sqlite/" + migration.name + ".up.sql")
		require.NoError(t, err)
		if strings.HasSuffix(migration.name, "_nextcloud_onboarding") {
			const alreadyMigrated = "ALTER TABLE sessions ADD COLUMN nextcloud_scoped BOOLEAN NOT NULL DEFAULT FALSE;"
			require.True(t, db.Migrator().HasColumn(&types.Session{}, "nextcloud_scoped"))
			require.Equal(t, 1, strings.Count(string(ddl), alreadyMigrated))
			// Current AutoMigrate already created this column. Keep every guard/trigger.
			ddl = []byte(strings.Replace(string(ddl), alreadyMigrated, "", 1))
		}
		require.NoError(t, db.Exec(string(ddl)).Error)
	}
}
