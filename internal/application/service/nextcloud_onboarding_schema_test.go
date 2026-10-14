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

// The source-derived fixtures exercise current controls too. Install the same
// production recovery/processing DDL as the machine fixture, before admission.
// GC inventory precedes processing migrations which extend its tables.
func installSourceCurrentControlSchema(t *testing.T, db, migration *gorm.DB) {
	t.Helper()
	require.NoError(t, db.AutoMigrate(&types.KnowledgeProcessingSpan{}))
	directory := "sqlite"
	names := []string{"000032_nextcloud_event_inbox", "000038_nextcloud_gc", "000040_nextcloud_gc_object_claim", "000041_nextcloud_gc_derived_inventory", "000064_nextcloud_publication_recovery", "000075_nextcloud_processing_state", "000076_nextcloud_directory_migrations", "000077_nextcloud_content_revision", "000078_nextcloud_control_recovery", "000079_nextcloud_recovery_build_leases", "000080_nextcloud_graph_recovery_fence"}
	if db.Dialector.Name() == "postgres" {
		directory = "versioned"
		names = []string{"000113_nextcloud_event_inbox", "000119_nextcloud_gc", "000121_nextcloud_gc_object_claim", "000122_nextcloud_gc_derived_inventory", "000145_nextcloud_publication_recovery", "000156_nextcloud_processing_state", "000157_nextcloud_directory_migrations", "000158_nextcloud_content_revision", "000159_nextcloud_control_recovery", "000160_nextcloud_recovery_build_leases", "000161_nextcloud_graph_recovery_fence"}
	}
	for _, name := range names {
		ddl, err := os.ReadFile("../../../migrations/" + directory + "/" + name + ".up.sql")
		require.NoError(t, err)
		if strings.HasSuffix(name, "_nextcloud_processing_state") && db.Migrator().HasColumn(&types.DataSource{}, "sync_enabled") {
			statement := "ALTER TABLE data_sources ADD COLUMN sync_enabled BOOLEAN NOT NULL DEFAULT TRUE;"
			require.Equal(t, 1, strings.Count(string(ddl), statement))
			ddl = []byte(strings.Replace(string(ddl), statement, "", 1))
		}
		require.NoError(t, migration.Exec(string(ddl)).Error)
	}
	installMachineOnboardingSchema(t, db, migration)
}
