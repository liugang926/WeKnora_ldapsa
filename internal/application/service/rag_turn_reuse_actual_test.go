package service

import (
	"context"
	"testing"

	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/application/readlease"
	"github.com/Tencent/WeKnora/internal/application/repository"
	chatpipeline "github.com/Tencent/WeKnora/internal/application/service/chat_pipeline"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
)

// The input is produced by actual signed ProcessSync, file/catalog admission,
// parsing, Summary/Auto publication and the installed original-body journal.
// Only the vector backend is controlled: it returns actual persisted chunk IDs,
// whose text/provenance is captured by the real SQL snapshot reader.
func runRAGPhysicalActualReusedTurn(t *testing.T, pg bool) {
	for _, mode := range []string{"search-and-merge", "producer-finished", "producer-borrowed", "owner-cancelled", "source-revoked", "missing-proof", "caller-cancelled"} {
		t.Run(mode, func(t *testing.T) {
			m := newMachinePipelineFixture(t, pg)
			k := m.ingest(t)
			for i := 0; i < len(m.f.queue.tasks); i++ {
				require.NoError(t, m.run(t, m.f.queue.tasks[i]))
			}
			var chunks []*types.Chunk
			require.NoError(t, m.f.db.Where("knowledge_id=? AND chunk_type=?", k.ID, types.ChunkTypeText).Find(&chunks).Error)
			require.NotEmpty(t, chunks)
			engine := &fakeRetrieveEngineService{engineType: types.PostgresRetrieverEngineType, support: []types.RetrieverType{types.VectorRetrieverType},
				canned: []*types.IndexWithScore{{ChunkID: chunks[0].ID, KnowledgeID: k.ID, Score: 1, Content: "untrusted index text"}}}
			svc := &knowledgeBaseService{repo: repository.NewKnowledgeBaseRepository(m.f.db), kgRepo: m.f.s.repo, chunkRepo: m.f.s.chunkRepo,
				contentLeases: m.f.store.ContentLeaseStore(), ragChunkSnapshots: repository.NewRAGChunkSnapshotReader(m.f.db, m.f.store), ragChunkLineageGuard: m.f.guard,
				modelService: m.f.s.modelService, retrieveEngine: parentChildRetrieveRegistry{engine: engine},
				publicationGuard: access.NewNextcloudPublicationGuard(repository.NewDirectoryRepository(m.f.db), repository.NewDataSourceRepository(m.f.db))}
			var tenant types.Tenant
			require.NoError(t, m.f.db.Where("id=7").Take(&tenant).Error)
			ctx := context.WithValue(m.f.ctx, types.TenantInfoContextKey, &tenant)
			ctx, cancelOwner := context.WithCancel(ctx)
			defer cancelOwner()
			ctx, attach, checkpoint, releaseHTTP := BeginMessageMaterialHTTPBoundary(ctx)
			defer releaseHTTP()
			ctx, releaseModel, external := retainMessageMaterialProducer(ctx)
			require.True(t, external)
			defer releaseModel()
			markMessageMaterialReady(ctx)
			manager := chatpipeline.NewEventManager()
			chatpipeline.ConfigureSourceLineage(manager, m.f.guard)
			chatpipeline.NewPluginMerge(manager, m.f.s.chunkRepo, m.f.s.chunkService)
			chatpipeline.ConfigureRAGChunkMergeHydration(manager, svc)
			chatpipeline.NewPluginFilterTopK(manager)
			cm := &types.ChatManage{}
			cm.TenantID, cm.RerankTopK = 7, 1
			cm.SearchTargets = []*types.SearchTarget{{Type: types.SearchTargetTypeKnowledgeBase, TenantID: 7, KnowledgeBaseID: "kb"}}
			params := types.SearchParams{QueryText: "actual original topic", QueryEmbedding: []float32{0.5}, MatchCount: 1}
			// The holder may acquire its fence before the QA entry adds its
			// lineage accumulator. Reuse must preserve that later accumulator.
			readCtx := ctx
			var finishTool context.CancelFunc
			var borrowed *readlease.NextcloudReadGuard
			if mode == "producer-finished" || mode == "producer-borrowed" {
				readCtx, finishTool = context.WithCancel(ctx)
				defer finishTool()
				if mode == "producer-borrowed" {
					var err error
					borrowed, err = readlease.BeginNextcloudKBRead(readCtx, m.f.store.ContentLeaseStore(), []readlease.NextcloudKBReadScope{{TenantID: 7, KBID: "kb"}})
					require.NoError(t, err)
					defer borrowed.Close()
					readCtx = borrowed.Context()
				}
			}
			warm, err := svc.HybridSearch(readCtx, "kb", params)
			require.NoError(t, err)
			require.Len(t, warm, 1)
			if finishTool != nil {
				if borrowed != nil {
					require.NoError(t, borrowed.Close())
				}
				finishTool()
				releaseModel()
				require.NoError(t, CheckMessageMaterialOutputBoundary(attach(ctx)), "HTTP sender retains the original read fence after tool context completion")
				releaseHTTP()
				var active int64
				require.NoError(t, m.f.db.Table("nextcloud_content_leases").Where("kind='read' AND released_at_ms IS NULL").Count(&active).Error)
				require.Zero(t, active)
				return
			}
			if mode == "owner-cancelled" {
				cancelOwner()
				require.Error(t, checkpoint(), "actual request cancellation still denies output")
				return
			}
			ctx, err = manager.BeginSourceLineage(ctx, cm, access.LineageReadScope{Targets: cm.SearchTargets})
			require.NoError(t, err)
			turn := types.RAGSourceLineageFromContext(ctx)
			first, err := svc.HybridSearch(ctx, "kb", params)
			require.NoError(t, err)
			require.Len(t, first, 1)
			require.Equal(t, chunks[0].Content, first[0].Content)
			accounted, err := turn.CheckedSnapshot(ctx)
			require.NoError(t, err)
			require.Len(t, accounted.Sources, 1, "the late accumulator must include the actual source dependency")
			owned, ok := ragTurnPhysicalReadGuard(ctx, []readlease.NextcloudKBReadScope{{TenantID: 7, KBID: "kb"}})
			require.True(t, ok)
			require.NotNil(t, owned)
			// Reuse the same turn fence from a current caller context, rather than
			// the older context stored when the guard was created.
			ctx = context.WithValue(ctx, ragReuseCurrentSelectorKey{}, "current selection")
			if mode == "caller-cancelled" {
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			} else if mode == "source-revoked" {
				m.personal.Store(false)
			} else if mode == "missing-proof" {
				first[0].SourceLineage = types.UnknownSourceLineage()
			}
			if mode != "search-and-merge" {
				rows, err := svc.HydrateRAGChunkAugmentations(ctx, first[0], []string{chunks[0].ID})
				require.Error(t, err)
				require.Nil(t, rows)
				return
			}
			second, err := svc.HybridSearch(ctx, "kb", params)
			require.NoError(t, err, "a same-turn query expansion must attach its owned physical fence")
			require.Len(t, second, 1)
			require.Same(t, turn, types.RAGSourceLineageFromContext(ctx))
			for i := 0; i < 2; i++ {
				rows, err := svc.HydrateRAGChunkAugmentations(ctx, second[0], []string{chunks[0].ID})
				require.NoError(t, err, "repeated parent/neighbor capture keeps the existing turn fence")
				require.Len(t, rows, 1)
				require.Equal(t, chunks[0].Content, rows[0].Chunk.Content)
			}
			cm.SearchResult = second
			require.Nil(t, manager.Trigger(ctx, types.CHUNK_MERGE, cm))
			require.NoError(t, chatpipeline.CheckSourceLineageInputs(ctx, cm))
			require.Nil(t, manager.Trigger(ctx, types.FILTER_TOP_K, cm))
			require.Len(t, cm.MergeResult, 1)
			require.NoError(t, cm.MergeResult[0].SourceLineage.RequireComplete())
			require.NoError(t, checkpoint())
			releaseModel()
			require.NoError(t, CheckMessageMaterialOutputBoundary(attach(ctx)))
			m.personal.Store(false)
			require.Error(t, turn.Check(ctx), "an open read lease cannot prolong source access")
			releaseHTTP()
			var active int64
			require.NoError(t, m.f.db.Table("nextcloud_content_leases").Where("kind='read' AND released_at_ms IS NULL").Count(&active).Error)
			require.Zero(t, active)
		})
	}
}

type ragReuseCurrentSelectorKey struct{}

func TestRAGPhysicalActualReusedTurnSQLite(t *testing.T)   { runRAGPhysicalActualReusedTurn(t, false) }
func TestPostgresRAGPhysicalActualReusedTurn(t *testing.T) { runRAGPhysicalActualReusedTurn(t, true) }
