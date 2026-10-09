package service

import (
	"context"
	"encoding/json"
	"github.com/Tencent/WeKnora/internal/application/repository"
	pgindex "github.com/Tencent/WeKnora/internal/application/repository/retriever/postgres"
	sqlindex "github.com/Tencent/WeKnora/internal/application/repository/retriever/sqlite"
	filesvc "github.com/Tencent/WeKnora/internal/application/service/file"
	"github.com/Tencent/WeKnora/internal/application/service/retriever"
	"github.com/Tencent/WeKnora/internal/models/embedding"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	sqlite_vec "github.com/asg017/sqlite-vec-go-bindings/cgo"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

var localDerivedVecAuto sync.Once

type localDerivedEmbedder struct{}

func (localDerivedEmbedder) Embed(context.Context, string) ([]float32, error) {
	return []float32{.2, .3, .4}, nil
}
func (localDerivedEmbedder) BatchEmbed(_ context.Context, texts []string) ([][]float32, error) {
	out := make([][]float32, len(texts))
	for i := range out {
		out[i] = []float32{.2, .3, .4}
	}
	return out, nil
}
func (m localDerivedEmbedder) BatchEmbedWithPool(ctx context.Context, _ embedding.Embedder, texts []string) ([][]float32, error) {
	return m.BatchEmbed(ctx, texts)
}
func (localDerivedEmbedder) GetModelID() string   { return "local-derived-controlled-model" }
func (localDerivedEmbedder) GetModelName() string { return "local-derived-controlled-model" }
func (localDerivedEmbedder) GetDimensions() int   { return 3 }
func actualLocalComposite(t *testing.T, db *gorm.DB, pg bool) *retriever.CompositeRetrieveEngine {
	t.Helper()
	engineType := types.SQLiteRetrieverEngineType
	var rows interfaces.RetrieveEngineRepository
	if pg {
		kind := "TEXT"
		if os.Getenv("WEKNORA_LOCAL_DERIVED_NATIVE_VECTOR") == "1" {
			require.NoError(t, db.Exec("CREATE EXTENSION IF NOT EXISTS vector WITH SCHEMA public").Error)
			kind = "public.halfvec"
		}
		require.NoError(t, db.Exec(`CREATE TABLE IF NOT EXISTS embeddings(id BIGSERIAL PRIMARY KEY,created_at TIMESTAMPTZ,updated_at TIMESTAMPTZ,source_id TEXT NOT NULL,source_type INTEGER NOT NULL,chunk_id TEXT,knowledge_id TEXT,knowledge_base_id TEXT,tag_id TEXT,content TEXT NOT NULL,dimension INTEGER NOT NULL,embedding `+kind+` NOT NULL,is_enabled BOOLEAN DEFAULT TRUE,UNIQUE(source_id,source_type))`).Error)
		rows = pgindex.NewPostgresRetrieveEngineRepository(db)
		engineType = types.PostgresRetrieverEngineType
	} else {
		localDerivedVecAuto.Do(sqlite_vec.Auto)
		rows = sqlindex.NewSQLiteRetrieveEngineRepository(db)
	}
	registry := retriever.NewRetrieveEngineRegistry(nil, nil)
	require.NoError(t, registry.Register(retriever.NewKVHybridRetrieveEngine(rows, engineType)))
	composite, e := retriever.NewCompositeRetrieveEngine(registry, []types.RetrieverEngineParams{{RetrieverType: types.VectorRetrieverType, RetrieverEngineType: engineType}, {RetrieverType: types.KeywordsRetrieverType, RetrieverEngineType: engineType}})
	require.NoError(t, e)
	return composite
}
func localDerivedActualPipeline(t *testing.T, pg bool, modes ...string) {
	mode := "normal"
	if len(modes) > 0 {
		mode = modes[0]
	}
	localDerivedVecAuto.Do(sqlite_vec.Auto)
	var composite *retriever.CompositeRetrieveEngine
	physicalRoot := t.TempDir()
	localProvider := filesvc.NewBackendScopedFileService("owned-derived-backend", filesvc.NewLocalFileService(physicalRoot, ""))
	var resourceRef, physicalPath, physicalResourceID string
	resourceBody := []byte("owned actual original local bytes")
	f, build, _, _, scope := sourceBuildFixture(t, pg, sourceDerivedFixtureHook{LocalDerived: true, BeforeKnowledge: func(f *enrichmentTestFixture, k *types.Knowledge) {
		if mode != "physical-original" {
			return
		}
		require.NoError(t, f.db.AutoMigrate(&types.StoredResource{}, &types.ResourceBinding{}))
		catalog := NewResourceCatalog(repository.NewResourceRepository(f.db))
		decorated := filesvc.NewResourceCatalogFileService(localProvider, catalog)
		var err error
		resourceRef, err = decorated.SaveBytes(types.WithResourceProvenance(context.Background(), types.ResourceProvenanceNextcloud), resourceBody, 7, "owned-original.txt", false)
		require.NoError(t, err)
		k.FilePath = resourceRef
		k.StorageSize = int64(len(resourceBody))
		handle, ok := types.ParseResourcePath(resourceRef)
		require.True(t, ok)
		var resource types.StoredResource
		require.NoError(t, f.db.Where("handle=?", handle).Take(&resource).Error)
		physicalPath = resource.PhysicalPath
		physicalResourceID = resource.ID
	}, BeforeChunk: func(f *enrichmentTestFixture, ctx context.Context) {
		if mode == "physical-original" {
			require.NoError(t, repository.NewResourceRepository(f.db).CreateBinding(ctx, &types.ResourceBinding{ResourceID: physicalResourceID, TenantID: 7, OwnerType: types.ResourceOwnerKnowledge, OwnerID: "source-candidate", Relation: types.ResourceRelationSourceFile}))
		}
		composite = actualLocalComposite(t, f.db, pg)
		require.NoError(t, composite.PrepareNextcloudStorageSelection(ctx))
	}})
	var gcSQL []string
	if pg {
		gcSQL = []string{"000119_nextcloud_gc", "000121_nextcloud_gc_object_claim", "000122_nextcloud_gc_derived_inventory"}
	} else {
		gcSQL = []string{"000038_nextcloud_gc", "000040_nextcloud_gc_object_claim", "000041_nextcloud_gc_derived_inventory"}
	}
	for _, name := range gcSQL {
		directory := "sqlite"
		if pg {
			directory = "versioned"
		}
		body, e := os.ReadFile("../../../migrations/" + directory + "/" + name + ".up.sql")
		require.NoError(t, e)
		require.NoError(t, f.db.Exec(string(body)).Error)
	}
	info := &types.IndexInfo{SourceID: f.chunks[0].ID, ChunkID: f.chunks[0].ID, SourceType: types.ChunkSourceType, KnowledgeID: f.k.ID, KnowledgeBaseID: "kb", Content: f.chunks[0].Content, IsEnabled: true}
	if mode == "sqlite-ftserror" && !pg {
		require.NoError(t, f.db.Exec("DROP TABLE lite_embeddings_fts").Error)
		require.Error(t, composite.BatchIndex(build, localDerivedEmbedder{}, []*types.IndexInfo{info}))
		var n int64
		require.NoError(t, f.db.Table("lite_embeddings").Count(&n).Error)
		require.Zero(t, n)
		return
	}
	require.NoError(t, composite.BatchIndex(build, localDerivedEmbedder{}, []*types.IndexInfo{info}))
	if pg && os.Getenv("WEKNORA_LOCAL_DERIVED_NATIVE_VECTOR") == "1" {
		results, err := pgindex.NewPostgresRetrieveEngineRepository(f.db).Retrieve(build, types.RetrieveParams{RetrieverType: types.VectorRetrieverType, Embedding: []float32{.2, .3, .4}, TopK: 3, KnowledgeBaseIDs: []string{"kb"}})
		require.NoError(t, err)
		require.NotEmpty(t, results)
		require.NotEmpty(t, results[0].Results)
	}
	table := "lite_embeddings"
	if pg {
		table = "embeddings"
	}
	var count int64
	require.NoError(t, f.db.Table(table).Where("knowledge_id=?", f.k.ID).Count(&count).Error)
	require.Equal(t, int64(1), count)
	if !pg {
		require.NoError(t, f.db.Table("vec_embeddings_3").Count(&count).Error)
		require.Equal(t, int64(1), count)
		require.NoError(t, f.db.Table("lite_embeddings_fts").Count(&count).Error)
		require.Equal(t, int64(1), count)
	}
	leases := repository.NewNextcloudContentLeaseStore(f.db)
	originalLease, _, e := repository.NextcloudBuildInputLease(build)
	require.NoError(t, e)
	require.NoError(t, f.db.Model(&types.Knowledge{}).Where("id=?", f.k.ID).Updates(map[string]any{"parse_status": types.ParseStatusCompleted, "pending_subtasks_count": 0}).Error)
	require.NoError(t, f.s.repo.(interface {
		TombstoneNextcloudVersion(context.Context, uint64, string, string, string) error
	}).TombstoneNextcloudVersion(context.Background(), 7, "kb", scope.DataSourceID, scope.ExternalID))
	if mode != "builder" {
		require.NoError(t, leases.ReleaseLease(context.Background(), originalLease.ID))
	}
	require.NoError(t, f.db.AutoMigrate(&types.StoredResource{}, &types.ResourceBinding{}))
	now := time.Now().UTC()
	gc := repository.NewNextcloudGCStore(f.db)
	if mode == "physical-original" {
		gc.ConfigureLocalDelete(func(ctx context.Context, resource *types.StoredResource) (bool, error) {
			return localProvider.(interfaces.NextcloudPhysicalGCDeleter).DeleteNextcloudObject(ctx, resource.PhysicalPath)
		})
	}
	_, e = gc.InventoryRetired(context.Background(), now, 10)
	require.NoError(t, e)
	// Explicit isolated complete/drained reader/build coverage, never production.
	millis := time.Now().Add(-time.Second).UnixMilli()
	require.NoError(t, f.db.Exec("INSERT INTO nextcloud_content_lease_coverage(tenant_id,knowledge_base_id,activated_at_ms,legacy_drained_at_ms,reader_revision,builder_revision) VALUES(7,'kb',?,?,'actual-local-pipeline-reader','actual-local-pipeline-builder')", millis, millis).Error)
	var reader repository.NextcloudContentLease
	if mode == "reader" {
		reader, e = leases.AcquireKBRead(context.Background(), 7, "kb", "actual-local-caller", time.Minute)
		require.NoError(t, e)
	}
	if mode == "receipt-crash" {
		if pg {
			require.NoError(t, f.db.Exec("CREATE FUNCTION derived_receipt_crash() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'actual receipt failure'; END $$;CREATE TRIGGER derived_receipt_crash BEFORE INSERT ON nextcloud_derived_gc_receipts FOR EACH ROW EXECUTE FUNCTION derived_receipt_crash()").Error)
		} else {
			require.NoError(t, f.db.Exec("CREATE TRIGGER derived_receipt_crash BEFORE INSERT ON nextcloud_derived_gc_receipts BEGIN SELECT RAISE(ABORT,'actual receipt failure'); END").Error)
		}
	}
	if mode == "late-writer" {
		require.Error(t, composite.BatchIndex(build, localDerivedEmbedder{}, []*types.IndexInfo{info}))
		require.NoError(t, f.db.Table(table).Where("knowledge_id=?", f.k.ID).Count(&count).Error)
		require.Equal(t, int64(1), count)
	}
	if mode == "foreign-parent" {
		require.NoError(t, f.db.Create(&types.Chunk{ID: "foreign-child", TenantID: 7, KnowledgeBaseID: "kb", KnowledgeID: "doc", ParentChunkID: f.chunks[0].ID, Content: "current other owner", ChunkType: types.ChunkTypeText}).Error)
	}
	if mode == "restored-current" {
		require.NoError(t, f.db.Exec("UPDATE nextcloud_source_versions SET state='published',candidate_knowledge_id=? WHERE tenant_id=7 AND knowledge_base_id='kb' AND datasource_id=?", f.k.ID, scope.DataSourceID).Error)
	}
	if mode == "unwitnessed-row" {
		sql := "INSERT INTO " + table + "(source_id,source_type,chunk_id,knowledge_id,knowledge_base_id,content,dimension) VALUES('unwitnessed',0,'source-body',?,'kb','unknown',0)"
		if pg {
			sql = "INSERT INTO " + table + "(source_id,source_type,chunk_id,knowledge_id,knowledge_base_id,content,dimension,embedding) VALUES('unwitnessed',0,'source-body',?,'kb','unknown',3,'[0.2,0.3,0.4]')"
		}
		require.NoError(t, f.db.Exec(sql, f.k.ID).Error)
	}
	if mode == "orphan-native" && !pg {
		require.NoError(t, f.db.Exec("DELETE FROM lite_embeddings WHERE knowledge_id=?", f.k.ID).Error)
	}
	_, e = gc.RunDue(context.Background(), time.Now().Add(time.Second), 10)
	if mode == "receipt-crash" {
		require.Error(t, e)
		require.NoError(t, f.db.Table(table).Where("knowledge_id=?", f.k.ID).Count(&count).Error)
		require.Equal(t, int64(1), count)
		if pg {
			require.NoError(t, f.db.Exec("DROP TRIGGER derived_receipt_crash ON nextcloud_derived_gc_receipts").Error)
		} else {
			require.NoError(t, f.db.Exec("DROP TRIGGER derived_receipt_crash").Error)
		}
		_, e = repository.NewNextcloudGCStore(f.db).RunDue(context.Background(), time.Now().Add(2*time.Hour), 10)
	}
	if mode == "restored-current" || mode == "foreign-parent" {
		require.Error(t, e)
	} else {
		require.NoError(t, e)
	}
	if mode == "reader" || mode == "builder" {
		require.NoError(t, f.db.Table(table).Where("knowledge_id=?", f.k.ID).Count(&count).Error)
		require.Equal(t, int64(1), count)
		leaseID := reader.ID
		if mode == "builder" {
			leaseID = originalLease.ID
		}
		require.NoError(t, leases.ReleaseLease(context.Background(), leaseID))
		_, e = gc.RunDue(context.Background(), time.Now().Add(2*time.Hour), 10)
		require.NoError(t, e)
	}
	if mode == "unwitnessed-row" {
		require.NoError(t, f.db.Table(table).Where("knowledge_id=?", f.k.ID).Count(&count).Error)
		require.Equal(t, int64(2), count)
		states, err := gc.ListStatus(context.Background(), 7, 10)
		require.NoError(t, err)
		require.Equal(t, "derived_backend_inventory_unverified", states[0].LastErrorCode)
		return
	}
	if mode == "restored-current" || mode == "foreign-parent" {
		require.NoError(t, f.db.Table(table).Where("knowledge_id=?", f.k.ID).Count(&count).Error)
		require.Equal(t, int64(1), count)
		return
	}
	require.NoError(t, f.db.Table(table).Where("knowledge_id=?", f.k.ID).Count(&count).Error)
	require.Zero(t, count)
	require.NoError(t, f.db.Table("chunks").Where("knowledge_id=?", f.k.ID).Count(&count).Error)
	require.Zero(t, count)
	if !pg {
		require.NoError(t, f.db.Table("vec_embeddings_3").Count(&count).Error)
		require.Zero(t, count)
		require.NoError(t, f.db.Table("lite_embeddings_fts").Count(&count).Error)
		require.Zero(t, count)
	}
	var manifest struct {
		State                            string
		PhysicalItems, PhysicalCollected int
		RetainedAuditItems               int
	}
	require.NoError(t, f.db.Table("nextcloud_derived_gc_manifests").Take(&manifest).Error)
	require.Equal(t, "collected", manifest.State)
	require.Equal(t, manifest.PhysicalItems, manifest.PhysicalCollected)
	require.Positive(t, manifest.RetainedAuditItems)
	var states []repository.NextcloudGCStatus
	states, e = gc.ListStatus(context.Background(), 7, 10)
	require.NoError(t, e)
	require.Equal(t, "collected", states[0].State)
	require.Zero(t, states[0].PendingItems)
	_, e = gc.RunDue(context.Background(), time.Now().Add(2*time.Hour), 10)
	require.NoError(t, e)
	var receipts []map[string]any
	require.NoError(t, f.db.Table("nextcloud_derived_gc_receipts").Find(&receipts).Error)
	require.Len(t, receipts, 2)
	if mode == "physical-original" {
		_, err := os.Stat(filepath.Join(physicalRoot, strings.TrimPrefix(func() string { _, inner, _ := types.ParseStorageBackendPath(physicalPath); return inner }(), "local://")))
		require.ErrorIs(t, err, os.ErrNotExist)
		require.Equal(t, int64(len(resourceBody)), states[0].ConfirmedReleasedBytes)
		require.Equal(t, 3, manifest.PhysicalItems)
	}
	if mode == "restored-after-collection" {
		require.NoError(t, f.db.Exec("UPDATE nextcloud_source_versions SET state='published',candidate_knowledge_id=? WHERE datasource_id=?", f.k.ID, scope.DataSourceID).Error)
		_, err := gc.RunDue(context.Background(), time.Now().Add(3*time.Hour), 10)
		require.Error(t, err)
		statuses, err := gc.ListStatus(context.Background(), 7, 10)
		require.NoError(t, err)
		require.Equal(t, "derived_exact_verification_failed", statuses[0].LastErrorCode)
	}
	encoded, e := json.Marshal(receipts)
	require.NoError(t, e)
	require.NotContains(t, string(encoded), strings.TrimSpace(f.chunks[0].Content))
}
func TestNextcloudLocalDerivedActualSQLitePipeline(t *testing.T) {
	for _, mode := range []string{"normal", "reader", "builder", "late-writer", "foreign-parent", "receipt-crash", "restored-current", "sqlite-ftserror", "physical-original", "unwitnessed-row", "restored-after-collection", "orphan-native"} {
		t.Run(mode, func(t *testing.T) { localDerivedActualPipeline(t, false, mode) })
	}
}
func TestPostgresNextcloudLocalDerivedActualPipeline(t *testing.T) {
	for _, mode := range []string{"normal", "reader", "builder", "late-writer", "foreign-parent", "receipt-crash", "restored-current", "physical-original", "unwitnessed-row", "restored-after-collection"} {
		t.Run(mode, func(t *testing.T) { localDerivedActualPipeline(t, true, mode) })
	}
}
