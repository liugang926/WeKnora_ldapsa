package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"testing"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func sourceLineageRepoFixture(t *testing.T) (*knowledgeRepository, *gorm.DB, *types.Knowledge) {
	t.Helper()
	repo, db := newNextcloudVersionTestRepo(t)
	require.NoError(t, db.AutoMigrate(&types.KnowledgeBase{}))
	require.NoError(t, db.Create(&types.KnowledgeBase{
		ID:               "kb",
		TenantID:         7,
		Name:             "source",
		EmbeddingModelID: "embed",
		SummaryModelID:   "summary",
	}).Error)
	script, err := os.ReadFile("../../../migrations/sqlite/000045_nextcloud_source_revisions.up.sql")
	require.NoError(t, err)
	require.NoError(t, db.Exec(string(script)).Error)
	knowledge := insertNextcloudVersionTestKnowledge(t, db, "candidate", "", types.ParseStatusCompleted, "enabled")
	require.NoError(t, repo.StageNextcloudVersion(
		context.Background(),
		7,
		"kb",
		"ds",
		"nextcloud:instance:77",
		"original-etag",
		knowledge.ID,
	))
	published, err := repo.PublishNextcloudVersion(context.Background(), knowledge.ID)
	require.NoError(t, err)
	require.True(t, published)
	return repo, db, knowledge
}

func TestNextcloudSourceLineageResolverUsesPersistedTupleAndStableBuildGeneration(t *testing.T) {
	repo, db, k := sourceLineageRepoFixture(t)
	ctx := context.Background()
	knowledge, original, err := repo.ResolveNextcloudSourceLineage(ctx, 7, "kb", k.ID)
	require.NoError(t, err)
	require.Equal(t, k.ID, knowledge.ID)
	require.Equal(t, uint64(7), original.TenantID)
	require.Equal(t, "ds", original.DataSourceID)
	require.Equal(t, "instance", original.InstanceID)
	require.Equal(t, "binding", original.BindingID)
	require.Equal(t, "77", original.FileID)
	require.Equal(t, "original-etag", original.ETag)
	require.Equal(t, uint64(1), original.Revision)
	require.NotEmpty(t, original.PairOperationID)
	lineage, err := types.NewCompleteSourceLineage(original)
	require.NoError(t, err)
	matches, err := repo.RecheckNextcloudSourceLineage(ctx, lineage)
	require.NoError(t, err)
	require.True(t, matches)
	before := nextcloudRevisionRows(t, mustLineageSQLDB(t, db), "nextcloud:instance:77")
	metadata := knowledge.GetMetadata()
	metadata["nextcloud_target_etag"] = "original-etag"
	metadata["nextcloud_etag"] = ""
	metadata["nextcloud_path"] = "renamed.md"
	require.NoError(t, repo.StageNextcloudVersionWithSource(
		ctx,
		7,
		"kb",
		"ds",
		"nextcloud:instance:77",
		"original-etag",
		k.ID,
		"renamed.md",
		metadata,
	))
	published, err := repo.PublishNextcloudVersion(ctx, k.ID)
	require.NoError(t, err)
	require.True(t, published)
	after := nextcloudRevisionRows(t, mustLineageSQLDB(t, db), "nextcloud:instance:77")
	require.Greater(t, len(after), len(before))
	renamed, stable, err := repo.ResolveNextcloudSourceLineage(ctx, 7, "kb", k.ID)
	require.NoError(t, err)
	require.Equal(t, original, stable, "path/observation changes are not a build generation")
	require.Equal(t, "renamed.md", renamed.FileName)
	// A new admitted build epoch for the same immutable document is distinct.
	require.NoError(t, db.Exec("UPDATE nextcloud_content_fences SET epoch=epoch+1 WHERE knowledge_id=?", k.ID).Error)
	_, newBuild, err := repo.ResolveNextcloudSourceLineage(ctx, 7, "kb", k.ID)
	require.NoError(t, err)
	require.NotEqual(t, original.Revision, newBuild.Revision)
	// Reusing the same document ID for a changed source ETag cannot retain its
	// former snapshot, even when the parser reuses the content rows.
	require.NoError(t, repo.StageNextcloudVersion(ctx, 7, "kb", "ds", "nextcloud:instance:77", "new-etag", k.ID))
	published, err = repo.PublishNextcloudVersion(ctx, k.ID)
	require.NoError(t, err)
	require.True(t, published)
	_, updated, err := repo.ResolveNextcloudSourceLineage(ctx, 7, "kb", k.ID)
	require.NoError(t, err)
	require.Equal(t, "new-etag", updated.ETag)
	require.NotEqual(t, original, updated)
	matches, err = repo.RecheckNextcloudSourceLineage(ctx, lineage)
	require.NoError(t, err)
	require.False(t, matches)
	require.NoError(t, db.Exec("DROP TABLE nextcloud_source_revisions").Error)
	matches, err = repo.RecheckNextcloudSourceLineage(ctx, lineage)
	require.ErrorIs(t, err, ErrNextcloudLineageSnapshotStore)
	require.False(t, matches)
}

func mustLineageSQLDB(t *testing.T, db *gorm.DB) *sql.DB {
	t.Helper()
	sqlDB, err := db.DB()
	require.NoError(t, err)
	return sqlDB
}

func TestNextcloudSourceLineageResolverDeniesChangedMissingOrCorruptRows(t *testing.T) {
	for _, change := range []string{
		"wrong tenant",
		"wrong KB",
		"metadata forged",
		"paused source",
		"pair identity",
		"version hidden",
		"ledger missing",
		"fence missing",
		"KB fence closed",
		"ledger error",
	} {
		t.Run(change, func(t *testing.T) {
			repo, db, k := sourceLineageRepoFixture(t)
			tenant := uint64(7)
			kb := "kb"
			switch change {
			case "wrong tenant":
				tenant = 99
			case "wrong KB":
				kb = "other"
			case "metadata forged":
				require.NoError(t, db.Model(k).Update("metadata", types.JSON(`{"datasource_id":"other"}`)).Error)
			case "paused source":
				require.NoError(t, db.Exec("UPDATE data_sources SET status='paused'").Error)
			case "pair identity":
				require.NoError(t, db.Exec("UPDATE nextcloud_source_pairings SET binding_id='other'").Error)
			case "version hidden":
				require.NoError(t, db.Exec("UPDATE nextcloud_source_versions SET state='staging'").Error)
			case "ledger missing":
				require.NoError(t, db.Exec("DROP TABLE nextcloud_source_revisions").Error)
			case "fence missing":
				require.NoError(t, db.Exec("DELETE FROM nextcloud_content_fences WHERE knowledge_id=?", k.ID).Error)
			case "KB fence closed":
				require.NoError(t, db.Exec(("UPDATE nextcloud_content_fences SET state='retired' WHE" +
					"RE knowledge_id=''")).Error)
			case "ledger error":
				require.NoError(t, db.Exec("DROP TABLE nextcloud_source_versions").Error)
			}
			knowledge, identity, err := repo.ResolveNextcloudSourceLineage(context.Background(), tenant, kb, k.ID)
			require.Error(t, err)
			require.Nil(t, knowledge)
			require.Equal(t, types.SourceIdentity{}, identity)
		})
	}
}

func TestNextcloudSourceLineageResolverRejectsAmbiguousMetadata(t *testing.T) {
	for _, raw := range []string{
		`{"datasource_id":"one","datasource_id":"two"}`,
		`{"datasource_id":1}`,
		`{"nextcloud_etag":null}`,
	} {
		_, err := strictLineageKnowledgeMetadata([]byte(raw))
		require.Error(t, err)
	}
	_, err := strictLineageKnowledgeMetadata(append([]byte(`{"datasource_id":"`), append([]byte{
		0xff,
	}, []byte(`"}`)...)...))
	require.Error(t, err)
	good, _ := json.Marshal(map[string]string{"datasource_id": "ds", "nextcloud_etag": "opaque-etag"})
	metadata, err := strictLineageKnowledgeMetadata(good)
	require.NoError(t, err)
	require.Equal(t, "opaque-etag", metadata["nextcloud_etag"])
}

func TestPostgresNextcloudSourceLineageResolverReadSnapshotAndRename(t *testing.T) {
	t.Setenv("SYSTEM_AES_KEY", "0123456789abcdef0123456789abcdef")
	db := leaseTestPostgres(t)
	require.NoError(t, db.AutoMigrate(
		&types.Knowledge{},
		&types.KnowledgeBase{},
		&types.Chunk{},
		&nextcloudSourceVersion{},
		&types.DataSource{},
		&NextcloudSourcePairing{},
		&nextcloudCandidateRetryJob{},
	))
	require.NoError(t, db.Create(&types.KnowledgeBase{
		ID:               "kb",
		TenantID:         7,
		Name:             "source",
		EmbeddingModelID: "embed",
		SummaryModelID:   "summary",
	}).Error)
	sqlDB, err := db.DB()
	require.NoError(t, err)
	migrations, err := gorm.Open(postgres.New(postgres.Config{Conn: sqlDB, PreferSimpleProtocol: true}), &gorm.Config{})
	require.NoError(t, err)
	script, err := os.ReadFile("../../../migrations/versioned/000126_nextcloud_source_revisions.up.sql")
	require.NoError(t, err)
	require.NoError(t, migrations.Exec(string(script)).Error)
	config, err := (&types.DataSourceConfig{Type: types.ConnectorTypeNextcloud, ResourceIDs: []string{
		"binding",
	}, Settings: map[string]interface{}{
		"base_url": "https://nextcloud.example",
	}, Credentials: map[string]interface{}{"token": "test-token", "key_id": "test_key"}}).ToJSON()
	require.NoError(t, err)
	source := &types.DataSource{
		ID:              "ds",
		TenantID:        7,
		KnowledgeBaseID: "kb",
		Type:            types.ConnectorTypeNextcloud,
		Status:          types.DataSourceStatusActive,
		Config:          config,
	}
	require.NoError(t, db.Create(source).Error)
	// JSONB normalizes representation. Pairing binds the persisted source,
	// rather than the pre-insert compact JSON used by this test fixture.
	require.NoError(t, db.Where("id = 'ds'").Take(source).Error)
	base, sha, binding, err := NextcloudEventDataSourceIdentity(source.Config)
	require.NoError(t, err)
	require.NoError(t, db.Create(&NextcloudSourcePairing{
		OperationID:     "pair",
		TenantID:        7,
		KnowledgeBaseID: "kb",
		DataSourceID:    "ds",
		InstanceID:      "instance",
		BindingID:       binding,
		BaseURL:         base,
		ConfigSHA:       sha,
		State:           "active",
	}).Error)
	repo := &knowledgeRepository{db: db, nextcloudPublicationCheck: func(
		context.Context,
		*types.DataSourceConfig,
		string,
		string,
		int64,
		string,
		string,
	) error {
		return nil
	}}
	k := insertNextcloudVersionTestKnowledge(t, db, "candidate", "", types.ParseStatusCompleted, "enabled")
	ctx := context.Background()
	require.NoError(t, repo.StageNextcloudVersion(ctx, 7, "kb", "ds", "nextcloud:instance:77", "etag", k.ID))
	published, err := repo.PublishNextcloudVersion(ctx, k.ID)
	require.NoError(t, err)
	require.True(t, published)
	knowledge, saved, err := repo.ResolveNextcloudSourceLineage(ctx, 7, "kb", k.ID)
	require.NoError(t, err)
	metadata := knowledge.GetMetadata()
	metadata["nextcloud_target_etag"] = "etag"
	metadata["nextcloud_etag"] = ""
	metadata["nextcloud_path"] = "renamed.md"
	require.NoError(t, repo.StageNextcloudVersionWithSource(
		ctx,
		7,
		"kb",
		"ds",
		"nextcloud:instance:77",
		"etag",
		k.ID,
		"renamed.md",
		metadata,
	))
	published, err = repo.PublishNextcloudVersion(ctx, k.ID)
	require.NoError(t, err)
	require.True(t, published)
	_, renamed, err := repo.ResolveNextcloudSourceLineage(ctx, 7, "kb", k.ID)
	require.NoError(t, err)
	require.Equal(t, saved, renamed)
	lineage, err := types.NewCompleteSourceLineage(saved)
	require.NoError(t, err)
	matches, err := repo.RecheckNextcloudSourceLineage(ctx, lineage)
	require.NoError(t, err)
	require.True(t, matches)
	require.NoError(t, db.Exec("UPDATE nextcloud_content_fences SET epoch=epoch+1 WHERE knowledge_id=?", k.ID).Error)
	_, changed, err := repo.ResolveNextcloudSourceLineage(ctx, 7, "kb", k.ID)
	require.NoError(t, err)
	require.NotEqual(t, saved.Revision, changed.Revision)
	matches, err = repo.RecheckNextcloudSourceLineage(ctx, lineage)
	require.NoError(t, err)
	require.False(t, matches)
	require.NoError(t, db.Exec("DROP TABLE nextcloud_source_revisions").Error)
	matches, err = repo.RecheckNextcloudSourceLineage(ctx, lineage)
	require.ErrorIs(t, err, ErrNextcloudLineageSnapshotStore)
	require.False(t, matches)
}
