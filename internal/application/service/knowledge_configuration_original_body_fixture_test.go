package service

import (
	"context"
	"os"
	"testing"

	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

func installConfigurationOriginalBodyFixtureSchema(t *testing.T, db *gorm.DB, pg bool) {
	t.Helper()
	if db.Migrator().HasTable("knowledge_configuration_original_body_manifests") {
		return
	}
	path := "../../../migrations/sqlite/000082_knowledge_configuration_original_body.up.sql"
	if pg {
		path = "../../../migrations/versioned/000163_knowledge_configuration_original_body.up.sql"
	}
	ddl, err := os.ReadFile(path)
	require.NoError(t, err)
	require.NoError(t, db.Exec(string(ddl)).Error)
}

// Fixture maintenance preserves existing immutable producer origins. It is
// intentionally separate from request reading and cannot adopt unknown rows.
func inventoryKnownConfigurationOriginalBodyFixture(
	ctx context.Context, t *testing.T,

	db *gorm.DB,
	store *repository.KnowledgeInputProvenanceStore,
) {
	t.Helper()
	if !db.Migrator().HasTable("knowledge_config_input_origins") {
		return
	}
	var scopes []struct {
		TenantID        uint64
		KnowledgeBaseID string
	}
	require.NoError(
		t,
		db.Table("knowledge_config_input_origins o").
			Select("o.tenant_id,o.knowledge_base_id").
			Joins(("JOIN knowledge_configuration_control_versions v ON v.knowledge_base_id=o.k" +
				"nowledge_base_id AND v.revision=o.config_revision")).
			Order("o.tenant_id,o.knowledge_base_id").
			Find(&scopes).
			Error,
	)
	for _, scope := range scopes {
		require.NoError(
			t,
			store.InventoryKnownKnowledgeConfigurationOriginalBodies(ctx, scope.TenantID, scope.KnowledgeBaseID),
		)
	}
}
