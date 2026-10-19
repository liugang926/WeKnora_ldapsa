package service

import (
	"context"
	"testing"

	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/application/readlease"
	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
)

func TestRAGPhysicalActualHydrationHeldUntilFinalProducerAndSenderRelease(t *testing.T) {
	for _, mode := range []string{"normal", "borrowed-parent", "expired-before-model", "expired-before-http", "cancel", "no-turn"} {
		t.Run(mode, func(t *testing.T) {
			messages, kg, store, db, ctx, _ := messageMaterialActualFixture(t)
			k := &types.Knowledge{
				ID:              "actual-rag-physical-document",
				TenantID:        7,
				KnowledgeBaseID: "kb",
				Type:            types.KnowledgeTypeManual,
				Channel:         "web",
				Source:          "manual",
				Title:           "actual authored title",
				EnableStatus:    "enabled",
				ParseStatus:     types.ParseStatusCompleted,
				Metadata:        types.JSON(`{"manual":{"content":"actual authored original"}}`),
				CustomMetadata:  types.JSON(`{}`),
			}
			original, err := store.CreateKnowledgeWithInput(
				repository.WithIndependentKnowledgeInput(ctx),
				k,
				"actual authored original",
			)
			require.NoError(t, err)
			build := repository.WithKnowledgeBuildInputProof(ctx, store, original)
			input, err := store.CaptureKnowledgeBuildInputs(
				build,
				types.KnowledgeBuildInputCaptureRequest{
					TenantID:        7,
					KnowledgeBaseID: "kb",
					KnowledgeID:     k.ID,
					Expected:        original,
				},
			)
			require.NoError(t, err)
			build = repository.WithKnowledgeBuildInputProof(build, store, input.Proof)
			chunk := &types.Chunk{
				ID:              "actual-rag-physical-chunk",
				TenantID:        7,
				KnowledgeBaseID: "kb",
				KnowledgeID:     k.ID,
				Content:         "actual original SQL chunk body",
				ChunkType:       types.ChunkTypeText,
				IsEnabled:       true,
				IndexStatus:     "ready",
				Metadata:        types.JSON(`{}`),
			}
			require.NoError(t, repository.NewChunkRepository(db).CreateChunks(build, []*types.Chunk{chunk}))
			svc := &knowledgeBaseService{
				kgRepo:            kg.repo,
				contentLeases:     store.ContentLeaseStore(),
				ragChunkSnapshots: repository.NewRAGChunkSnapshotReader(db, store),
			}
			lineage, err := types.NewCompleteSourceLineage()
			require.NoError(t, err)
			ctx = types.WithRAGSourceLineage(
				ctx,
				types.NewRAGSourceLineage(func(checkCtx context.Context, value *types.SourceLineage) error {
					return messages.sourceLineageGuard.Check(
						checkCtx,
						value,
						access.LineageReadScope{
							Targets: types.SearchTargets{
								&types.SearchTarget{
									Type:            types.SearchTargetTypeKnowledgeBase,
									TenantID:        7,
									KnowledgeBaseID: "kb",
								},
							},
						},
					)
				}),
			)
			var check func() error
			var releaseHTTP, releaseModel func()
			var attach func(context.Context) context.Context
			if mode != "no-turn" {
				ctx, attach, check, releaseHTTP = BeginMessageMaterialHTTPBoundary(ctx)
				defer releaseHTTP()
				var external bool
				ctx, releaseModel, external = retainMessageMaterialProducer(ctx)
				require.True(t, external)
				defer releaseModel()
				markMessageMaterialReady(ctx)
			}
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()
			var borrowed *readlease.NextcloudReadGuard
			if mode == "borrowed-parent" {
				borrowed, err = readlease.BeginNextcloudKBRead(
					ctx,
					store.ContentLeaseStore(),
					[]readlease.NextcloudKBReadScope{{TenantID: 7, KBID: "kb"}},
				)
				require.NoError(t, err)
				defer func() { require.NoError(t, borrowed.Close()) }()
				ctx = borrowed.Context()
			}
			cached := &types.SearchResult{
				KnowledgeID:     k.ID,
				KnowledgeBaseID: "kb",
				Content:         "forged cached index text",
				SourceLineage:   lineage,
			}
			rows, err := svc.HydrateRAGChunkAugmentations(ctx, cached, []string{chunk.ID})
			require.NoError(t, err)
			require.Len(t, rows, 1)
			require.Equal(t, chunk.Content, rows[0].Chunk.Content)
			if borrowed != nil {
				ctx = borrowed.ParentContext(ctx)
				require.NoError(t, borrowed.Close())
			}
			active := func() int64 {
				var n int64
				require.NoError(
					t,
					db.Table("nextcloud_content_leases").
						Where("kind='read' AND released_at_ms IS NULL").
						Count(&n).
						Error,
				)
				return n
			}
			if mode == "no-turn" {
				require.Zero(t, active(), "ordinary one-call reads release after hydration")
				return
			}
			require.Positive(t, active(), "actual SQL hydration return must not release the turn's read fence")
			if mode == "expired-before-model" {
				require.NoError(
					t,
					db.Table("nextcloud_content_leases").
						Where("kind='read' AND released_at_ms IS NULL").
						Update("expires_at_ms", int64(1)).
						Error,
				)
				require.Error(t, check(), "model checkpoint rejects expired original physical fence")
			} else {
				require.NoError(t, check())
				// The source value consumed here is the real typed SQL snapshot,
				// while the actual read fence remains alive during the model turn.
				require.Equal(t, "actual original SQL chunk body", rows[0].Chunk.Content)
			}
			releaseModel()
			require.Positive(t, active(), "model completion cannot release the HTTP sender's fence")
			switch mode {
			case "expired-before-http":
				require.NoError(
					t,
					db.Table("nextcloud_content_leases").
						Where("kind='read' AND released_at_ms IS NULL").
						Update("expires_at_ms", int64(1)).
						Error,
				)
			case "cancel":
				cancel()
			}
			if mode == "normal" || mode == "borrowed-parent" {
				require.NoError(t, CheckMessageMaterialOutputBoundary(attach(ctx)))
			} else {
				require.Error(t, CheckMessageMaterialOutputBoundary(attach(ctx)))
			}
			releaseHTTP()
			require.Zero(t, active(), "last actual sender/model reference releases all physical fences")
			require.Error(t, check())
		})
	}
}
