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
	defer func() {
		if db.Migrator().HasTable("nextcloud_machine_tasks") {
			return
		}
		path := "../../../migrations/sqlite/000084_nextcloud_machine_tasks.up.sql"
		if db.Name() == "postgres" {
			path = "../../../migrations/versioned/000165_nextcloud_machine_tasks.up.sql"
		}
		ddl, err := os.ReadFile(path)
		require.NoError(t, err)
		require.NoError(t, migration.Exec(string(ddl)).Error)
	}()
	if db.Migrator().HasTable("nextcloud_onboarding_operations") {
		// A later reader reuses this exact fixture installation. A partial schema
		// cannot be repaired with blank control rows or counted as successful setup.
		for _, table := range []string{
			"nextcloud_onboarding_credentials",
			"nextcloud_onboarding_delegations",
			"nextcloud_source_grants",
			"nextcloud_kb_blank_witnesses",
			"nextcloud_onboarding_target_closures",
			"nextcloud_onboarding_tenant_deletions",
			"nextcloud_onboarding_manager_purposes",
			"nextcloud_onboarding_manager_heads",
		} {
			require.True(t, db.Migrator().HasTable(table), "actual module schema incomplete: %s", table)
		}
		return
	}
	require.NoError(t, db.AutoMigrate(&types.AuthToken{}, &types.WikiPage{}))
	directory, onboarding, closure, manager := "sqlite",
		"000074_nextcloud_onboarding",
		"000081_nextcloud_target_closure",
		"000083_nextcloud_current_manager_purpose"
	if db.Name() == "postgres" {
		directory, onboarding, closure, manager = "versioned",
			"000155_nextcloud_onboarding",
			"000162_nextcloud_target_closure",
			"000164_nextcloud_current_manager_purpose"
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
	ddl, err = os.ReadFile("../../../migrations/" + directory + "/" + manager + ".up.sql")
	require.NoError(t, err)
	require.NoError(t, migration.Exec(string(ddl)).Error)
}

func installDocumentOnboardingSchema(t *testing.T, db *gorm.DB) {
	t.Helper()
	installMachineOnboardingSchema(t, db, db)
}

// requireSourceAdmissionSchema proves that a reusable fixture installed the
// complete production event/pairing prerequisite instead of substituting
// AutoMigrate tables. The trigger check is schema-scoped on PostgreSQL so a
// leftover disposable test schema cannot satisfy the assertion.
func requireSourceAdmissionSchema(t *testing.T, db *gorm.DB) {
	t.Helper()
	for _, table := range []string{
		"nextcloud_event_connections",
		"nextcloud_event_nonces",
		"nextcloud_event_inbox",
		"nextcloud_event_checkpoint",
		"nextcloud_source_pairings",
		"nextcloud_source_pairing_aborts",
	} {
		require.True(t, db.Migrator().HasTable(table), "actual source admission schema incomplete: %s", table)
	}
	triggers := []string{
		"nextcloud_source_pairing_admit_insert",
		"nextcloud_source_pairing_admit_update",
		"nextcloud_source_pairing_sync_insert",
		"nextcloud_source_pairing_sync_update",
		"nextcloud_paired_kb_delete_guard",
	}
	var count int64
	if db.Name() == "postgres" {
		require.NoError(t, db.Raw(
			(`SELECT COUNT(*) FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid `+
				`JOIN pg_namespace n ON n.oid=c.relnamespace WHERE NOT t.tgisinternal `+
				`AND n.nspname=current_schema() AND t.tgname IN ?`),
			triggers,
		).Scan(&count).Error)
	} else {
		triggers = append(triggers, "nextcloud_source_pairing_immutable_update")
		require.NoError(t, db.Table("sqlite_master").
			Where("type = ? AND name IN ?", "trigger", triggers).
			Count(&count).Error)
	}
	require.EqualValues(t, len(triggers), count, "actual source pairing guards are incomplete")
}

// The source-derived fixtures exercise current controls too. Install the same
// production recovery/processing DDL as the machine fixture, before admission.
// GC inventory precedes processing migrations which extend its tables.
func installSourceCurrentControlSchema(t *testing.T, db, migration *gorm.DB) {
	t.Helper()
	installSourceCurrentControlSchemaFrom(t, db, migration, true)
}

// installSourceCurrentControlSchemaAfterAdmissionMigrations continues at the
// first post-event control after a fixture has executed the real event,
// pairing and pairing-abort migrations. It never replays event DDL.
func installSourceCurrentControlSchemaAfterAdmissionMigrations(t *testing.T, db, migration *gorm.DB) {
	t.Helper()
	requireSourceAdmissionSchema(t, db)
	installSourceCurrentControlSchemaFrom(t, db, migration, false)
}

func installSourceCurrentControlSchemaFrom(t *testing.T, db, migration *gorm.DB, includeEvent bool) {
	t.Helper()
	require.NoError(t, db.AutoMigrate(&types.KnowledgeProcessingSpan{}))
	directory := "sqlite"
	names := []string{
		"000032_nextcloud_event_inbox",
		"000038_nextcloud_gc",
		"000040_nextcloud_gc_object_claim",
		"000041_nextcloud_gc_derived_inventory",
		"000064_nextcloud_publication_recovery",
		"000075_nextcloud_processing_state",
		"000076_nextcloud_directory_migrations",
		"000077_nextcloud_content_revision",
		"000078_nextcloud_control_recovery",
		"000079_nextcloud_recovery_build_leases",
		"000080_nextcloud_graph_recovery_fence",
	}
	if db.Name() == "postgres" {
		directory = "versioned"
		names = []string{
			"000113_nextcloud_event_inbox",
			"000119_nextcloud_gc",
			"000121_nextcloud_gc_object_claim",
			"000122_nextcloud_gc_derived_inventory",
			"000145_nextcloud_publication_recovery",
			"000156_nextcloud_processing_state",
			"000157_nextcloud_directory_migrations",
			"000158_nextcloud_content_revision",
			"000159_nextcloud_control_recovery",
			"000160_nextcloud_recovery_build_leases",
			"000161_nextcloud_graph_recovery_fence",
		}
	}
	if !includeEvent {
		names = names[1:]
	}
	for _, name := range names {
		ddl, err := os.ReadFile("../../../migrations/" + directory + "/" + name + ".up.sql")
		require.NoError(t, err)
		if strings.HasSuffix(name, "_nextcloud_processing_state") &&
			db.Migrator().HasColumn(&types.DataSource{}, "sync_enabled") {
			statement := "ALTER TABLE data_sources ADD COLUMN sync_enabled BOOLEAN NOT NULL DEFAULT TRUE;"
			require.Equal(t, 1, strings.Count(string(ddl), statement))
			ddl = []byte(strings.Replace(string(ddl), statement, "", 1))
		}
		require.NoError(t, migration.Exec(string(ddl)).Error)
	}
	installMachineOnboardingSchema(t, db, migration)
}
