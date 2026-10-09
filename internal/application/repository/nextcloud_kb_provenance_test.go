package repository

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func provenanceTestDB(t *testing.T) *gorm.DB {
	t.Helper()
	db, err := gorm.Open(sqlite.Open("file:"+t.Name()+"?mode=memory&cache=shared"), &gorm.Config{})
	require.NoError(t, err)
	require.NoError(t, db.Exec(`CREATE TABLE knowledge_bases (
		id TEXT PRIMARY KEY, tenant_id INTEGER NOT NULL,
		ever_had_nextcloud_source BOOLEAN NOT NULL DEFAULT 0,
		generated_profile TEXT,
		updated_at DATETIME,
		deleted_at DATETIME
	)`).Error)
	require.NoError(t, db.Exec(`CREATE TABLE knowledges (
		id TEXT PRIMARY KEY, tenant_id INTEGER NOT NULL,
		knowledge_base_id TEXT NOT NULL, deleted_at DATETIME
	)`).Error)
	require.NoError(t, db.AutoMigrate(&types.DataSource{}))
	require.NoError(t, db.AutoMigrate(&NextcloudSourcePairing{}))
	require.NoError(t, db.AutoMigrate(&NextcloudSourcePairingAbort{}))
	require.NoError(t, db.Exec(`CREATE TABLE nextcloud_source_tombstones (
		tenant_id INTEGER NOT NULL, scope_type TEXT NOT NULL, scope_id TEXT NOT NULL,
		PRIMARY KEY (tenant_id, scope_type, scope_id))`).Error)
	return db
}

func TestNextcloudSourceCreationMarksKBAtomically(t *testing.T) {
	db := provenanceTestDB(t)
	require.NoError(t, db.Exec(("INSERT INTO knowledge_bases (id, tenant_id) VALUES (?, "+
		"?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?)"),
		"kb", 7, "kb-ordinary", 7, "kb-update", 7, "kb-documents", 7,
		"kb-past-source", 7, "kb-past-documents", 7).Error)
	repo := NewDataSourceRepository(db)
	ctx := context.Background()
	ordinary := &types.DataSource{
		ID:              "ordinary",
		TenantID:        7,
		KnowledgeBaseID: "kb-ordinary",
		Type:            types.ConnectorTypeFeishu,
	}
	require.NoError(t, repo.Create(ctx, ordinary))
	var marked bool
	require.NoError(t, db.Raw(("SELECT ever_had_nextcloud_source FROM knowledge_bases W"+
		"HERE id = ?"), "kb-ordinary").Scan(&marked).Error)
	require.False(t, marked)

	nextcloud := &types.DataSource{
		ID:              "nextcloud",
		TenantID:        7,
		KnowledgeBaseID: "kb",
		Type:            types.ConnectorTypeNextcloud,
	}
	require.NoError(t, repo.Create(ctx, nextcloud))
	require.Error(t, repo.Create(ctx, &types.DataSource{
		ID: "second-nextcloud", TenantID: 7,
		KnowledgeBaseID: "kb", Type: types.ConnectorTypeNextcloud,
	}))
	require.Error(t, repo.Create(ctx, &types.DataSource{
		ID: "mixed-ordinary", TenantID: 7,
		KnowledgeBaseID: "kb", Type: types.ConnectorTypeFeishu,
	}))
	require.Error(t, repo.Create(ctx, &types.DataSource{
		ID: "mixed-nextcloud", TenantID: 7,
		KnowledgeBaseID: "kb-ordinary", Type: types.ConnectorTypeNextcloud,
	}))
	require.NoError(t, db.Unscoped().Delete(&types.DataSource{}, "id = ?", nextcloud.ID).Error)
	require.NoError(t, db.Raw(("SELECT ever_had_nextcloud_source FROM knowledge_bases W"+
		"HERE id = ?"), "kb").Scan(&marked).Error)
	require.True(t, marked, "hard-deleting the source must not erase provenance")
	require.Error(t, repo.Create(ctx, &types.DataSource{
		ID: "ordinary-after-delete", TenantID: 7,
		KnowledgeBaseID: "kb", Type: types.ConnectorTypeFeishu,
	}))

	missing := &types.DataSource{
		ID:              "invalid",
		TenantID:        7,
		KnowledgeBaseID: "missing",
		Type:            types.ConnectorTypeNextcloud,
	}
	require.Error(t, repo.Create(ctx, missing))
	var count int64
	require.NoError(t, db.Model(&types.DataSource{}).Where("id = ?", missing.ID).Count(&count).Error)
	require.Zero(t, count, "an unmarked source must not be committed")

	transition := &types.DataSource{
		ID:              "transition",
		TenantID:        7,
		KnowledgeBaseID: "kb-update",
		Type:            types.ConnectorTypeFeishu,
	}
	require.NoError(t, repo.Create(ctx, transition))
	transition.Type = types.ConnectorTypeNextcloud
	require.Error(t, repo.Update(ctx, transition))
	require.NoError(t, db.Raw(("SELECT ever_had_nextcloud_source FROM knowledge_bases W"+
		"HERE id = ?"), "kb-update").Scan(&marked).Error)
	require.False(t, marked, "rejected type change must not mark the KB")

	require.NoError(t, db.Exec(("INSERT INTO knowledges (id, tenant_id, knowledge_base_i" +
		"d) VALUES ('doc', 7, 'kb-documents')")).Error)
	require.Error(t, repo.Create(ctx, &types.DataSource{
		ID: "with-document", TenantID: 7,
		KnowledgeBaseID: "kb-documents", Type: types.ConnectorTypeNextcloud,
	}))

	pastSource := &types.DataSource{
		ID: "past-source", TenantID: 7,
		KnowledgeBaseID: "kb-past-source", Type: types.ConnectorTypeFeishu,
	}
	require.NoError(t, repo.Create(ctx, pastSource))
	require.NoError(t, repo.Delete(ctx, pastSource.ID))
	require.Error(t, repo.Create(ctx, &types.DataSource{
		ID: "after-past-source", TenantID: 7,
		KnowledgeBaseID: "kb-past-source", Type: types.ConnectorTypeNextcloud,
	}))
	require.NoError(t, db.Exec(`INSERT INTO knowledges (id, tenant_id, knowledge_base_id, deleted_at)
		VALUES ('past-document', 7, 'kb-past-documents', CURRENT_TIMESTAMP)`).Error)
	require.Error(t, repo.Create(ctx, &types.DataSource{
		ID: "after-past-document", TenantID: 7,
		KnowledgeBaseID: "kb-past-documents", Type: types.ConnectorTypeNextcloud,
	}))
}

func TestSQLiteNextcloudSourceRegistrationFencesGeneratedProfileWrite(t *testing.T) {
	db := provenanceTestDB(t)
	require.NoError(t, db.Exec(`INSERT INTO knowledge_bases (id, tenant_id) VALUES ('kb-profile', 7)`).Error)
	profiles := NewKnowledgeBaseRepository(db)
	ctx := context.Background()
	require.NoError(t, profiles.UpdateKnowledgeBaseGeneratedProfile(ctx, "kb-profile",
		&types.KnowledgeBaseProfile{Gist: "ordinary profile"}))

	sources := NewDataSourceRepository(db)
	require.NoError(t, sources.Create(ctx, &types.DataSource{
		ID: "source-profile", TenantID: 7,
		KnowledgeBaseID: "kb-profile", Type: types.ConnectorTypeNextcloud,
	}))
	require.ErrorIs(t, profiles.UpdateKnowledgeBaseGeneratedProfile(ctx, "kb-profile",
		&types.KnowledgeBaseProfile{Gist: "stale generated profile"}),
		types.ErrKnowledgeBaseProfileUnsupported)
	var stored string
	require.NoError(t, db.Raw(`SELECT generated_profile FROM knowledge_bases WHERE id = 'kb-profile'`).
		Scan(&stored).Error)
	require.Contains(t, stored, "ordinary profile")
	require.NotContains(t, stored, "stale generated profile")

	// A hard-deleted source does not reopen profile generation: the KB flag
	// records historical provenance, not only its current source list.
	require.NoError(t, db.Unscoped().Delete(&types.DataSource{}, "id = ?", "source-profile").Error)
	require.ErrorIs(t, profiles.UpdateKnowledgeBaseGeneratedProfile(ctx, "kb-profile", nil),
		types.ErrKnowledgeBaseProfileUnsupported)
}

func TestSQLiteNextcloudKBProvenanceMigrationBackfillsDeletedSources(t *testing.T) {
	db, err := gorm.Open(sqlite.Open("file:"+t.Name()+"?mode=memory&cache=shared"), &gorm.Config{})
	require.NoError(t, err)
	require.NoError(t, db.Exec("CREATE TABLE knowledge_bases (id TEXT PRIMARY KEY, tenant_id INTEGER NOT NULL)").Error)
	require.NoError(t, db.Exec(("CREATE TABLE data_sources (knowledge_base_id TEXT, tena" +
		"nt_id INTEGER, type TEXT, deleted_at DATETIME)")).Error)
	require.NoError(t, db.Exec(("CREATE TABLE knowledges (knowledge_base_id TEXT, tenant" +
		"_id INTEGER, channel TEXT, metadata TEXT, deleted_at DA" +
		"TETIME)")).Error)
	require.NoError(t, db.Exec("INSERT INTO knowledge_bases VALUES ('source',7),('document',7),('ordinary',7)").Error)
	require.NoError(t, db.Exec("INSERT INTO data_sources VALUES ('source',7,'nextcloud','2024-01-01')").Error)
	require.NoError(t, db.Exec("INSERT INTO knowledges VALUES ('document',7,'nextcloud','{}','2024-01-01')").Error)
	script, err := os.ReadFile("../../../migrations/sqlite/000030_kb_nextcloud_provenance.up.sql")
	require.NoError(t, err)
	for _, statement := range strings.Split(string(script), ";\n\n") {
		statement = strings.TrimSpace(statement)
		if statement != "" {
			require.NoError(t, db.Exec(statement).Error)
		}
	}
	for id, want := range map[string]bool{"source": true, "document": true, "ordinary": false} {
		var got bool
		require.NoError(t, db.Raw(("SELECT ever_had_nextcloud_source FROM knowledge_bases W"+
			"HERE id = ?"), id).Scan(&got).Error)
		require.Equal(t, want, got, id)
	}
	require.Error(t, db.Exec("UPDATE knowledge_bases SET ever_had_nextcloud_source = 0 WHERE id = 'source'").Error)
}
