package service

import (
	"context"
	"testing"
	"unicode/utf8"

	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/application/repository"
	chatpipeline "github.com/Tencent/WeKnora/internal/application/service/chat_pipeline"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
)

// This selects both real text and the real generated Summary, including the
// producer's text-parent association. A short text also forces Merge through
// its actual base/neighbor snapshot path instead of silently skipping it.
func runRAGPhysicalActualShortSummary(t *testing.T, pg bool) {
	for _, mode := range []string{
		"short-summary",
		"text-neighbors",
		"source-revoked",
		"missing-summary-proof",
		"changed-stored-parent",
		"forged-parent-selector",
		"changed-cached-summary",
		"unsupported-image-parent",
	} {
		t.Run(mode, func(t *testing.T) {
			body := ("Current employee policy: read the approved record, then ask before changin" +
				"g another department's access.")
			if mode == "text-neighbors" {
				body = ("First policy paragraph: current authorization is required.\n\nSecond polic" +
					"y paragraph: keep the original record intact.\n\nThird policy paragraph: a" +
					"sk the owner before changing access.\n\nFourth policy paragraph: source wi" +
					"thdrawal ends reading rights.")
			}
			m := newMachinePipelineFixtureWithDocument(t, pg, body)
			if mode == "text-neighbors" {
				var kb types.KnowledgeBase
				require.NoError(t, m.f.db.Where("id='kb'").Take(&kb).Error)
				kb.ChunkingConfig.ChunkSize, kb.ChunkingConfig.ChunkOverlap = 64, 0
				require.NoError(
					t,
					m.f.store.UpdateKnowledgeBaseWithInput(repository.WithIndependentKnowledgeInput(m.f.ctx), &kb),
				)
			}
			k := m.ingest(t)
			for i := 0; i < len(m.f.queue.tasks); i++ {
				require.NoError(t, m.run(t, m.f.queue.tasks[i]))
			}
			var text []*types.Chunk
			var summary types.Chunk
			require.NoError(
				t,
				m.f.db.Where("knowledge_id=? AND chunk_type=?", k.ID, types.ChunkTypeText).
					Order("chunk_index").
					Find(&text).
					Error,
			)
			require.NotEmpty(t, text)
			require.NoError(
				t,
				m.f.db.Where("knowledge_id=? AND chunk_type=?", k.ID, types.ChunkTypeSummary).Take(&summary).Error,
			)
			require.Equal(
				t,
				text[0].ID,
				summary.ParentChunkID,
				"actual generated summary associates with its original source text",
			)
			require.Less(t, utf8.RuneCountInString(text[0].Content), 350)
			if mode == "text-neighbors" {
				require.Greater(t, len(text), 1)
				require.NotEmpty(t, text[0].NextChunkID)
			}
			engine := &fakeRetrieveEngineService{
				engineType: types.PostgresRetrieverEngineType,
				support:    []types.RetrieverType{types.VectorRetrieverType},
				canned: []*types.IndexWithScore{
					{ChunkID: text[0].ID, KnowledgeID: k.ID, Score: 1},
					{ChunkID: summary.ID, KnowledgeID: k.ID, Score: .9},
				},
			}
			svc := &knowledgeBaseService{
				repo:                 repository.NewKnowledgeBaseRepository(m.f.db),
				kgRepo:               m.f.s.repo,
				chunkRepo:            m.f.s.chunkRepo,
				contentLeases:        m.f.store.ContentLeaseStore(),
				ragChunkSnapshots:    repository.NewRAGChunkSnapshotReader(m.f.db, m.f.store),
				ragChunkLineageGuard: m.f.guard,
				modelService:         m.f.s.modelService,
				retrieveEngine:       parentChildRetrieveRegistry{engine: engine},
				publicationGuard: access.NewNextcloudPublicationGuard(
					repository.NewDirectoryRepository(m.f.db),
					repository.NewDataSourceRepository(m.f.db),
				),
			}
			var tenant types.Tenant
			require.NoError(t, m.f.db.Where("id=7").Take(&tenant).Error)
			ctx := context.WithValue(m.f.ctx, types.TenantInfoContextKey, &tenant)
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
			cm := &types.ChatManage{}
			cm.TenantID = 7
			cm.SearchTargets = []*types.SearchTarget{
				{Type: types.SearchTargetTypeKnowledgeBase, TenantID: 7, KnowledgeBaseID: "kb"},
			}
			ctx, err := manager.BeginSourceLineage(ctx, cm, access.LineageReadScope{Targets: cm.SearchTargets})
			require.NoError(t, err)
			rows, err := svc.HybridSearch(
				ctx,
				"kb",
				types.SearchParams{
					QueryText:             "current employee policy",
					QueryEmbedding:        []float32{.5},
					MatchCount:            2,
					SkipContextEnrichment: true,
				},
			)
			require.NoError(t, err)
			require.Len(t, rows, 2, "both actual short text and its generated summary were retrieved")
			var summaryResult *types.SearchResult
			for _, row := range rows {
				require.NoError(t, row.SourceLineage.RequireComplete())
				if row.ID == summary.ID {
					summaryResult = row
				}
			}
			require.NotNil(t, summaryResult)
			switch mode {
			case "source-revoked":
				m.personal.Store(false)
			case "missing-summary-proof":
				summaryResult.SourceLineage = types.UnknownSourceLineage()
			case "changed-stored-parent":
				require.NoError(
					t,
					m.f.db.Model(&types.Chunk{}).
						Where("id=?", summary.ID).
						Update("parent_chunk_id", "unrelated-parent").
						Error,
				)
			case "forged-parent-selector":
				summaryResult.ParentChunkID = "unrelated-parent"
			case "changed-cached-summary":
				summaryResult.Content = "unadmitted replacement summary"
			case "unsupported-image-parent":
				summaryResult.ChunkType = types.ChunkTypeImageOCR
			}
			cm.SearchResult = rows
			require.Nil(t, manager.Trigger(ctx, types.CHUNK_MERGE, cm))
			err = chatpipeline.CheckSourceLineageInputs(ctx, cm)
			if mode != "short-summary" && mode != "text-neighbors" {
				require.Error(
					t,
					err,
					"no changed body/relation or missing authorization may reach the next model stage",
				)
				return
			}
			require.NoError(
				t,
				err,
				"a production-generated summary's source association is not an unsupported image parent",
			)
			require.Len(t, cm.MergeResult, 2)
			for _, row := range cm.MergeResult {
				require.NoError(t, row.SourceLineage.RequireComplete())
				if row.ID == summary.ID {
					require.Equal(
						t,
						summary.Content,
						row.Content,
						"summary revalidation never appends its source parent body",
					)
				}
				if mode == "text-neighbors" && row.ID == text[0].ID {
					require.Contains(
						t,
						row.SubChunkID,
						text[0].NextChunkID,
						"actual neighbor was captured under the held source scope",
					)
				}
			}
			require.NoError(t, checkpoint())
			releaseModel()
			require.NoError(t, CheckMessageMaterialOutputBoundary(attach(ctx)))
			m.personal.Store(false)
			require.Error(t, types.RAGSourceLineageFromContext(ctx).Check(ctx))
		})
	}
}

func TestRAGPhysicalActualShortSummarySQLite(t *testing.T) {
	runRAGPhysicalActualShortSummary(t, false)
}

func TestPostgresRAGPhysicalActualShortSummary(t *testing.T) {
	runRAGPhysicalActualShortSummary(t, true)
}
