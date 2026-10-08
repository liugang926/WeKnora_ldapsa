package tools

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

func actualAgentDocumentFixture(t *testing.T) (*ReadDocumentTool, *leasedAgentKnowledgeService, *gorm.DB) {
	t.Helper()
	t.Setenv("SYSTEM_AES_KEY", "0123456789abcdef0123456789abcdef")
	store, db := agentReadTestStore(t)
	require.NoError(t, db.AutoMigrate(&types.Knowledge{}, &types.Chunk{}, &types.KnowledgeBase{}, &types.DataSource{}, &repository.NextcloudSourcePairing{}))
	for _, name := range []string{"000031_nextcloud_source_versions.up.sql", "000045_nextcloud_source_revisions.up.sql"} {
		b, err := os.ReadFile("../../../migrations/sqlite/" + name)
		require.NoError(t, err)
		require.NoError(t, db.Exec(string(b)).Error)
	}
	config, err := (&types.DataSourceConfig{Type: types.ConnectorTypeNextcloud, ResourceIDs: []string{"binding"}, Settings: map[string]any{"base_url": "https://nextcloud.fixture.invalid"}, Credentials: map[string]any{"token": "local-stub-credential", "key_id": "fixture"}}).ToJSON()
	require.NoError(t, err)
	ds := &types.DataSource{ID: "ds", TenantID: 7, KnowledgeBaseID: "kb", Type: types.ConnectorTypeNextcloud, Status: types.DataSourceStatusActive, Config: config}
	require.NoError(t, db.Create(ds).Error)
	baseURL, digest, binding, err := repository.NextcloudEventDataSourceIdentity(config)
	require.NoError(t, err)
	require.NoError(t, db.Create(&repository.NextcloudSourcePairing{OperationID: "pair", TenantID: 7, KnowledgeBaseID: "kb", DataSourceID: "ds", InstanceID: "instance", BindingID: binding, BaseURL: baseURL, ConfigSHA: digest, State: "active"}).Error)
	require.NoError(t, db.Create(&types.KnowledgeBase{ID: "kb", TenantID: 7, Name: "fixture", EmbeddingModelID: "local", SummaryModelID: "local"}).Error)
	metadata, err := json.Marshal(map[string]string{"datasource_id": "ds", "external_id": "nextcloud:instance:77", "nextcloud_instance_id": "instance", "nextcloud_binding_id": "binding", "nextcloud_file_id": "77", "nextcloud_etag": "original", "nextcloud_target_etag": "original", "source_resource_id": "binding", "nextcloud_path": "file.txt"})
	require.NoError(t, err)
	k := &types.Knowledge{ID: "doc", TenantID: 7, KnowledgeBaseID: "kb", Channel: types.ConnectorTypeNextcloud, Type: "file", Title: "authoritative title", FileName: "file.txt", ParseStatus: types.ParseStatusCompleted, EnableStatus: "enabled", Metadata: types.JSON(metadata), Description: "unaccounted generated profile"}
	require.NoError(t, db.Create(k).Error)
	updated := time.Now().UTC()
	require.NoError(t, db.Table("nextcloud_source_versions").Create(map[string]any{"tenant_id": 7, "knowledge_base_id": "kb", "datasource_id": "ds", "external_id": "nextcloud:instance:77", "desired_etag": "original", "candidate_knowledge_id": "doc", "state": "published", "updated_at": updated}).Error)
	chunks := []*types.Chunk{{ID: "c1", TenantID: 7, KnowledgeBaseID: "kb", KnowledgeID: "doc", Content: "authoritative database body", ChunkType: types.ChunkTypeText, ChunkIndex: 0, IsEnabled: true, IndexStatus: "ready"}, {ID: "c2", TenantID: 7, KnowledgeBaseID: "kb", KnowledgeID: "doc", Content: "authoritative second chunk", ChunkType: types.ChunkTypeText, ChunkIndex: 1, IsEnabled: true, IndexStatus: "ready"}}
	for _, c := range chunks {
		require.NoError(t, db.Create(c).Error)
	}
	// The service/index objects are deliberately stale. Their body/title cannot
	// be stamped with the current source tuple and sent to a model.
	oldKnowledge := *k
	oldKnowledge.Title = "stale index title"
	for i, c := range chunks {
		copy := *c
		copy.Content = "stale index poison"
		chunks[i] = &copy
	}
	svc := &leasedAgentKnowledgeService{readDocKnowledgeService: &readDocKnowledgeService{docs: map[string]*types.Knowledge{"doc": &oldKnowledge}}, store: store}
	tool := NewReadDocumentTool(svc, &readDocChunkService{repo: &readDocChunkRepo{ordered: chunks}}, types.SearchTargets{{Type: types.SearchTargetTypeKnowledgeBase, KnowledgeBaseID: "kb", TenantID: 7}})
	tool.SetRAGChunkSnapshotReader(repository.NewRAGChunkSnapshotReader(db, nil))
	return tool, svc, db
}

func TestAgentLineageReadDocumentActualSnapshotReplacesStaleBodyTitleAndPreservesOriginalIdentity(t *testing.T) {
	for _, args := range []string{`{"id":"doc"}`, `{"id":"doc","query":"database"}`, `{"id":"c1","context":1}`} {
		t.Run(args, func(t *testing.T) {
			tool, _, db := actualAgentDocumentFixture(t)
			repo := repository.NewKnowledgeRepository(db).(interface {
				RecheckNextcloudSourceLineage(context.Context, *types.SourceLineage) (bool, error)
			})
			turn := types.NewRAGSourceLineage(func(ctx context.Context, l *types.SourceLineage) error {
				if len(l.Sources) == 0 {
					return l.RequireComplete()
				}
				ok, err := repo.RecheckNextcloudSourceLineage(ctx, l)
				if !ok && err == nil {
					return types.ErrSourceLineageUnknown
				}
				return err
			})
			ctx := types.WithRAGSourceLineage(context.Background(), turn)
			r := NewToolRegistry()
			r.RegisterTool(tool)
			result, err := r.ExecuteToolWithSourceLineage(ctx, ToolReadDocument, json.RawMessage(args))
			require.NoError(t, err)
			require.True(t, result.Success)
			require.Contains(t, result.Output, "authoritative title")
			require.Contains(t, result.Output, "authoritative database body")
			require.NotContains(t, result.Output, "stale index")
			require.NotContains(t, result.Output, "unaccounted generated profile")
			require.Len(t, result.SourceLineage.Sources, 1)
			source := result.SourceLineage.Sources[0]
			require.Equal(t, "original", source.ETag)
			require.Equal(t, "pair", source.PairOperationID)
			require.Equal(t, "77", source.FileID)
			require.Equal(t, uint64(1), source.Revision)
			var active int64
			require.NoError(t, db.Table("nextcloud_content_leases").Where("released_at_ms IS NULL").Count(&active).Error)
			require.Zero(t, active)
		})
	}
}

type changingAgentSnapshotReader struct {
	inner interface {
		HydrateRAGChunkSnapshots(context.Context, []types.RAGChunkReadRequest) ([]types.RAGChunkInputSnapshot, error)
	}
	db    *gorm.DB
	calls int
}

func (r *changingAgentSnapshotReader) HydrateRAGChunkSnapshots(ctx context.Context, requests []types.RAGChunkReadRequest) ([]types.RAGChunkInputSnapshot, error) {
	r.calls++
	if r.calls == 2 {
		if err := r.db.Exec("UPDATE nextcloud_content_fences SET epoch=epoch+1 WHERE knowledge_id='doc'").Error; err != nil {
			return nil, err
		}
	}
	return r.inner.HydrateRAGChunkSnapshots(ctx, requests)
}
func TestAgentLineageReadDocumentCannotReplaceOriginalPinOrEmitRevokedControlData(t *testing.T) {
	for _, change := range []string{"changed original epoch", "current grant withdrawn"} {
		t.Run(change, func(t *testing.T) {
			tool, svc, db := actualAgentDocumentFixture(t)
			reader := repository.NewRAGChunkSnapshotReader(db, nil)
			if change == "changed original epoch" {
				tool.SetRAGChunkSnapshotReader(&changingAgentSnapshotReader{inner: reader, db: db})
			} else {
				svc.revoked.Store(true)
			}
			turn, ctx := toolTurn()
			r := NewToolRegistry()
			r.RegisterTool(tool)
			result, err := r.ExecuteToolWithSourceLineage(ctx, ToolReadDocument, json.RawMessage(`{"id":"doc"}`))
			require.Error(t, err)
			require.Nil(t, result)
			require.Error(t, turn.Check(ctx))
		})
	}
}
func TestAgentLineageReadDocumentOrdinaryWithoutWriterReceiptStaysUnknown(t *testing.T) {
	tool, _, db := actualAgentDocumentFixture(t)
	require.NoError(t, db.Exec("DELETE FROM knowledges WHERE id='doc'").Error)
	manual := &types.Knowledge{ID: "manual", TenantID: 7, KnowledgeBaseID: "kb", Title: "apparently ordinary", Type: "manual", ParseStatus: types.ParseStatusCompleted, EnableStatus: "enabled"}
	require.NoError(t, db.Create(manual).Error)
	tool.knowledgeService = &readDocKnowledgeService{docs: map[string]*types.Knowledge{"manual": manual}}
	turn, ctx := toolTurn()
	r := NewToolRegistry()
	r.RegisterTool(tool)
	result, err := r.ExecuteToolWithSourceLineage(ctx, ToolReadDocument, json.RawMessage(`{"id":"manual"}`))
	require.Error(t, err)
	require.Nil(t, result)
	require.Error(t, turn.Check(ctx))
}
