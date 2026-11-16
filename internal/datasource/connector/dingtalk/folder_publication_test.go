//go:build sqlite && sqlite_fts5

package dingtalk_test

import (
	"context"
	"crypto/md5"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"maps"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/database"
	"github.com/Tencent/WeKnora/internal/datasource/connector/dingtalk"
	"github.com/Tencent/WeKnora/internal/types"
	sqlitevec "github.com/asg017/sqlite-vec-go-bindings/cgo"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func TestSignedFolderPublication(t *testing.T) {
	t.Chdir("../../../..")
	path := filepath.Join(t.TempDir(), "folders.db")
	sqlitevec.Auto()
	require.NoError(t, database.RunMigrationsWithOptions("sqlite3://"+path, database.MigrationOptions{SQLiteDBPath: path}))
	db, err := gorm.Open(sqlite.Open(path), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	require.NoError(t, err)
	pool, err := db.DB()
	require.NoError(t, err)
	t.Cleanup(func() { _ = pool.Close() })
	journal, err := filepath.EvalSymlinks(t.TempDir())
	require.NoError(t, err)
	require.NoError(t, os.Chmod(journal, 0o700))
	key := make([]byte, 32)
	_, err = rand.Read(key)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(journal, "key"), key, 0o600))
	t.Setenv("WEKNORA_ORIGINAL_BODY_JOURNAL_DIRECTORY", journal)
	t.Setenv("WEKNORA_ORIGINAL_BODY_JOURNAL_KEY", filepath.Join(journal, "key"))
	t.Setenv("WEKNORA_ORIGINAL_BODY_JOURNAL_PIN", filepath.Join(journal, "pin"))
	require.NoError(t, repository.NewOriginalBodyRetentionStore(db).InitializeJournal())
	require.NoError(t, repository.InstallOriginalBodyRetentionTriggers(db))
	store := repository.NewKnowledgeInputProvenanceStore(db)
	ctx := context.Background()
	require.NoError(t, store.CreateKnowledgeBaseWithInput(repository.WithIndependentKnowledgeInput(ctx), &types.KnowledgeBase{
		ID: "kb", Name: "Folders", TenantID: 7, EmbeddingModelID: "embed", SummaryModelID: "summary",
	}))

	for _, tc := range []struct {
		name, first string
		preserve    bool
	}{
		{"flat", "Team", false}, {"reparse", "Team", false}, {"folders", "Team", true}, {"traversal", "..", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			connector, cfg := dingtalk.FolderPublicationFixture(tc.first, tc.preserve)
			config, err := cfg.ToJSON()
			require.NoError(t, err)
			ds := &types.DataSource{ID: tc.name, Name: tc.name, TenantID: 7, KnowledgeBaseID: "kb", Type: types.ConnectorTypeDingTalk, Config: config}
			require.NoError(t, db.Create(ds).Error)
			run := &types.SyncLog{ID: tc.name, TenantID: 7, DataSourceID: ds.ID, Status: types.SyncLogStatusRunning}
			require.NoError(t, db.Create(run).Error)
			claim, err := store.ClaimDingTalkRun(ctx, ds, run.ID, "sync", 0, time.Minute)
			require.NoError(t, err)
			fetch, err := store.BeginDingTalkPublicationFetch(claim, ds, run.ID)
			require.NoError(t, err)
			items, err := connector.FetchAll(fetch, cfg, cfg.ResourceIDs)
			require.NoError(t, err)
			require.Len(t, items, 2)
			for _, item := range items {
				altered := item
				altered.FileName = "Untrusted/" + item.FileName
				_, err := store.BindDingTalkOriginalArtifact(fetch, &altered)
				require.Error(t, err)
				bound, err := store.BindDingTalkOriginalArtifact(fetch, &item)
				if tc.first == ".." && item.ExternalID == "doc" {
					require.ErrorIs(t, err, repository.ErrKnowledgeInputChanged)
					continue
				}
				require.NoError(t, err)
				folder, leaf := types.SplitKnowledgeRelativePath(item.FileName)
				require.Equal(t, "Synthetic.md", leaf)
				if tc.preserve {
					expected := map[string]string{"doc": "Workspace/" + tc.first, "second": "Workspace/Other"}
					require.Equal(t, expected[item.ExternalID], folder)
				} else {
					require.Empty(t, folder)
				}
				metadata := maps.Clone(item.Metadata)
				metadata["datasource_id"], metadata["external_id"], metadata["source_resource_id"] = ds.ID, item.ExternalID, item.SourceResourceID
				encoded, err := json.Marshal(metadata)
				require.NoError(t, err)
				hash := md5.Sum(item.Content)
				k := &types.Knowledge{
					ID: ds.ID + "-" + item.ExternalID, TenantID: 7, KnowledgeBaseID: "kb", Channel: types.ConnectorTypeDingTalk,
					Type: "file", FileType: "md", Title: leaf, FileName: leaf, FolderPath: folder, FileSize: int64(len(item.Content)),
					FileHash: hex.EncodeToString(hash[:]), Metadata: encoded, CustomMetadata: types.JSON(`{}`),
				}
				if tc.name == "reparse" {
					k.FilePath = "local://fixture.md"
				}
				moved := *k
				moved.FolderPath = "Wrong"
				_, err = store.PublishDingTalkOriginal(bound, &moved, item.Content)
				require.ErrorIs(t, err, repository.ErrKnowledgeInputChanged)
				result, err := store.PublishDingTalkOriginal(bound, k, item.Content)
				require.NoError(t, err)
				require.Equal(t, "created", result.Outcome)
				snapshot, err := store.CaptureKnowledgeBuildInputs(ctx, types.KnowledgeBuildInputCaptureRequest{
					TenantID: 7, KnowledgeBaseID: "kb", KnowledgeID: k.ID, Expected: result.Proof,
				})
				require.NoError(t, err)
				require.Equal(t, result.Proof.AdmissionID, snapshot.Proof.AdmissionID)
				if tc.name == "reparse" && item.ExternalID == "doc" {
					t.Run("scoped group reparse", func(t *testing.T) { testScopedLocalReparse(t, db, store, k, item.Content) })
					continue
				}
				require.NoError(t, db.Exec("UPDATE knowledges SET folder_path = ? WHERE id = ?", "Forged", k.ID).Error)
				_, err = store.CaptureKnowledgeBuildInputs(ctx, types.KnowledgeBuildInputCaptureRequest{
					TenantID: 7, KnowledgeBaseID: "kb", KnowledgeID: k.ID, Expected: result.Proof,
				})
				require.ErrorIs(t, err, repository.ErrKnowledgeInputChanged)
			}
		})
	}
}
