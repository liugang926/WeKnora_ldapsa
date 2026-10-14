package service

import (
	"os"
	"strings"
	"testing"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// All module controls are installed before any real source/fence admission.
// Keep the production SQL, including triggers; remove only the Session column
// already supplied by the fixture's current AutoMigrate type. This helper is
// available without the FTS tag so ordinary machine/PG regressions use it too.
func installMachineOnboardingSchema(t *testing.T, db, migration *gorm.DB) {
	t.Helper()
	if db.Migrator().HasTable("nextcloud_onboarding_operations") {
		// A later reader reuses this exact fixture installation. A partial schema
		// cannot be repaired with blank control rows or counted as successful setup.
		for _, table := range []string{"nextcloud_onboarding_credentials", "nextcloud_onboarding_delegations", "nextcloud_source_grants", "nextcloud_kb_blank_witnesses", "nextcloud_onboarding_target_closures", "nextcloud_onboarding_tenant_deletions"} {
			require.True(t, db.Migrator().HasTable(table), "actual module schema incomplete: %s", table)
		}
		return
	}
	require.NoError(t, db.AutoMigrate(&types.AuthToken{}, &types.WikiPage{}))
	directory, onboarding, closure := "sqlite", "000074_nextcloud_onboarding", "000081_nextcloud_target_closure"
	if db.Dialector.Name() == "postgres" {
		directory, onboarding, closure = "versioned", "000155_nextcloud_onboarding", "000162_nextcloud_target_closure"
	}
	ddl, err := os.ReadFile("../../../migrations/" + directory + "/" + onboarding + ".up.sql")
	require.NoError(t, err)
	statement := "ALTER TABLE sessions ADD COLUMN nextcloud_scoped BOOLEAN NOT NULL DEFAULT FALSE;"
	require.True(t, db.Migrator().HasColumn(&types.Session{}, "nextcloud_scoped"))
	require.Equal(t, 1, strings.Count(string(ddl), statement))
	require.NoError(t, migration.Exec(strings.Replace(string(ddl), statement, "", 1)).Error)
	ddl, err = os.ReadFile("../../../migrations/" + directory + "/" + closure + ".up.sql")
	require.NoError(t, err)
	require.NoError(t, migration.Exec(string(ddl)).Error)
}

func installDocumentOnboardingSchema(t *testing.T, db *gorm.DB) {
	t.Helper()
	installMachineOnboardingSchema(t, db, db)
}
