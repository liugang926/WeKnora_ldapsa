package handler

import (
	"context"
	"net/http/httptest"
	"testing"

	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/application/service"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

type chunkOutputHTTPService struct {
	interfaces.ChunkService
	hint *types.Chunk
}

func (s chunkOutputHTTPService) GetChunkByIDOnly(context.Context, string) (*types.Chunk, error) {
	return s.hint, nil
}

func chunkOutputHTTPFixture(
	t *testing.T,
) (
	*ChunkHandler,
	*gorm.DB,
	*types.Knowledge,
	*outputHTTPGroups,
	*repository.KnowledgeInputProvenanceStore,
	context.Context,
) {
	t.Helper()
	base, db, k, groups := outputHTTPFixture(t, true)
	store := repository.NewKnowledgeInputProvenanceStore(db)
	repository.ConfigureKnowledgeInputActorGroupAccess(
		store,
		service.NewGroupAccessService(repository.NewGroupAccessRepository(db)),
	)
	actual, err := store.CaptureKnowledgeOutputInputs(
		context.Background(),
		types.KnowledgeBuildInputCaptureRequest{
			TenantID:        k.TenantID,
			KnowledgeBaseID: k.KnowledgeBaseID,
			KnowledgeID:     k.ID,
		},
	)
	require.NoError(t, err)
	buildCtx := repository.WithKnowledgeBuildInputProof(context.Background(), store, actual.Proof)
	chunk := &types.Chunk{
		ID:              "actual-chunk",
		TenantID:        k.TenantID,
		KnowledgeBaseID: k.KnowledgeBaseID,
		KnowledgeID:     k.ID,
		Content:         "actual original chunk body",
		ChunkType:       types.ChunkTypeText,
		IsEnabled:       true,
		IndexStatus:     "ready",
		Metadata:        types.JSON(`{}`),
	}
	require.NoError(t, repository.NewChunkRepository(db).CreateChunks(buildCtx, []*types.Chunk{chunk}))
	var kb types.KnowledgeBase
	require.NoError(t, db.Where("id = ?", k.KnowledgeBaseID).Take(&kb).Error)
	guard := access.NewNextcloudSourceLineageGuard(nil, outputHTTPKB{row: &kb}, nil, nil, groups, nil, store)
	h := &ChunkHandler{service: chunkOutputHTTPService{hint: chunk}, kgService: base.kgService}
	ConfigureChunkHandlerOutputLineage(h, service.NewChunkOutputAuthorizer(store, guard))
	return h, db, k, groups, store, buildCtx
}

func serveChunkOutput(h *ChunkHandler, path string) *httptest.ResponseRecorder {
	gin.SetMode(gin.TestMode)
	router := documentHandlerRouter()
	router.Use(func(c *gin.Context) {
		ctx := types.WithCaller(c.Request.Context(), types.Caller{TenantID: 7, UserID: "user"})
		ctx = types.WithPrincipal(ctx, types.Principal{Type: types.PrincipalWebUser, ID: "user"})
		ctx = types.WithExecutionTenant(ctx, 7)
		c.Request = c.Request.WithContext(ctx)
	})
	router.GET("/chunks/by-id/:id", h.GetChunkByIDOnly)
	router.GET("/chunks/:knowledge_id", h.ListKnowledgeChunks)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, httptest.NewRequest("GET", path, nil))
	return w
}

func TestChunkOutputHTTPActualOriginalBodyReplacesServiceHint(t *testing.T) {
	h, _, _, _, _, _ := chunkOutputHTTPFixture(t)
	ctx := types.WithCaller(
		context.WithValue(context.Background(), types.TenantIDContextKey, uint64(7)),
		types.Caller{TenantID: 7, UserID: "user"},
	)
	ctx = types.WithPrincipal(ctx, types.Principal{Type: types.PrincipalWebUser, ID: "user"})
	ctx = types.WithExecutionTenant(ctx, 7)
	_, originalErr := h.outputAuthorizer.Begin(
		ctx,
		types.ChunkOutputReadRequest{
			TenantID:        7,
			KnowledgeBaseID: "kb",
			KnowledgeID:     "document",
			ChunkIDs:        []string{"actual-chunk"},
		},
	)
	require.NoError(t, originalErr, "actual fixture material/actor authority before any cached service hint")
	fake := *h.service.(chunkOutputHTTPService).hint
	fake.Content = "stale cached body must not be restamped"
	fake.Metadata = types.JSON(`{"source_lineage":{"version":1,"state":"complete","sources":[]}}`)
	h.service = chunkOutputHTTPService{hint: &fake}
	w := serveChunkOutput(h, "/chunks/by-id/actual-chunk")
	require.Equal(t, 200, w.Code, w.Body.String())
	require.Contains(t, w.Body.String(), "actual original chunk body")
	require.NotContains(t, w.Body.String(), "stale cached body")
	require.NotContains(t, w.Body.String(), "source_lineage")
}

func TestChunkOutputHTTPNonmatchingLegacyCandidateClosesBeforePageAndCount(t *testing.T) {
	h, db, k, _, _, _ := chunkOutputHTTPFixture(t)
	require.NoError(
		t,
		db.Create(
			&types.Chunk{
				ID:              "legacy-hidden-image",
				TenantID:        k.TenantID,
				KnowledgeBaseID: k.KnowledgeBaseID,
				KnowledgeID:     k.ID,
				Content:         "hidden unproved source",
				ChunkType:       types.ChunkTypeImageCaption,
				IsEnabled:       true,
				IndexStatus:     "ready",
			},
		).Error,
	)
	w := serveChunkOutput(h, "/chunks/document?page=1&page_size=1")
	require.Equal(t, 403, w.Code, w.Body.String())
	require.NotContains(t, w.Body.String(), "original chunk body")
	require.NotContains(t, w.Body.String(), "hidden unproved source")
	require.NotContains(t, w.Body.String(), `"total"`)
}

func TestChunkOutputHTTPFreshCandidatesNormalPageAndTotal(t *testing.T) {
	h, db, k, _, _, ctx := chunkOutputHTTPFixture(t)
	require.NoError(
		t,
		repository.NewChunkRepository(db).
			CreateChunks(ctx, []*types.Chunk{{
				ID:              "second-text",
				TenantID:        k.TenantID,
				KnowledgeBaseID: k.KnowledgeBaseID,
				KnowledgeID:     k.ID,
				Content:         "second accounted chunk",
				ChunkType:       types.ChunkTypeText,
				ChunkIndex:      2,
				IsEnabled:       true,
				IndexStatus:     "ready",
			}, {
				ID:              "accounted-image",
				TenantID:        k.TenantID,
				KnowledgeBaseID: k.KnowledgeBaseID,
				KnowledgeID:     k.ID,
				Content:         "accounted image body",
				ChunkType:       types.ChunkTypeImageCaption,
				IsEnabled:       true,
				IndexStatus:     "ready",
			}}),
	)
	w := serveChunkOutput(h, "/chunks/document?page=2&page_size=1")
	require.Equal(t, 200, w.Code, w.Body.String())
	require.Contains(t, w.Body.String(), "second accounted chunk")
	require.Contains(t, w.Body.String(), `"total":2`)
	require.NotContains(t, w.Body.String(), "accounted image body")
}

func TestChunkOutputHTTPGrantOrOriginalBodyChangedAtFinalBoundaryClosed(t *testing.T) {
	for _, mode := range []string{"grant", "body ABA", "deleted"} {
		t.Run(mode, func(t *testing.T) {
			h, db, _, groups, _, _ := chunkOutputHTTPFixture(t)
			groups.hook = func(n int) {
				if n == 2 {
					switch mode {
					case "grant":
						groups.allowed = false
					case "body ABA":
						require.NoError(
							t,
							db.Model(&types.Chunk{}).Where("id = 'actual-chunk'").Update("content", "changed").Error,
						)
						require.NoError(
							t,
							db.Model(&types.Chunk{}).
								Where("id = 'actual-chunk'").
								Update("content", "actual original chunk body").
								Error,
						)
					case "deleted":
						require.NoError(t, db.Unscoped().Where("id = 'actual-chunk'").Delete(&types.Chunk{}).Error)
					}
				}
			}
			w := serveChunkOutput(h, "/chunks/by-id/actual-chunk")
			require.Equal(t, 403, w.Code, w.Body.String())
			require.NotContains(t, w.Body.String(), "actual original chunk body")
		})
	}
}
