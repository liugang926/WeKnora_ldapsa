package handler

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/application/service"
	"github.com/Tencent/WeKnora/internal/middleware"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

type galleryAfterRead struct {
	interfaces.ChunkService
	after func()
}

func (s *galleryAfterRead) ListImagesByKnowledgeBaseID(
	ctx context.Context, kb string, p *types.Pagination, f *types.ImageListFilter,
) (*types.PageResult, error) {
	page, err := s.ChunkService.ListImagesByKnowledgeBaseID(ctx, kb, p, f)
	if err == nil && s.after != nil {
		s.after()
		s.after = nil
	}
	return page, err
}

func TestGalleryGETAndPOSTRecheckWorkspaceAccessAtWrite(t *testing.T) {
	t.Setenv("WEKNORA_NEXTCLOUD_READ_AUTHORITY", "weknora")
	db := leaseHandlerDB(t)
	require.NoError(t, db.AutoMigrate(&types.Chunk{}, &types.Knowledge{}, &types.KnowledgeBase{}))
	for _, path := range []string{"000053_knowledge_input_provenance.up.sql", "000098_chunk_images.up.sql"} {
		raw, err := os.ReadFile("../../migrations/sqlite/" + path)
		require.NoError(t, err)
		require.NoError(t, db.Exec(string(raw)).Error)
	}
	// Existing workspace-owned rows predate original-input receipts.
	doc := &types.Knowledge{ID: "doc", TenantID: 1, KnowledgeBaseID: "kb-1", Title: "source title"}
	require.NoError(t, db.Create(doc).Error)
	chunk := &types.Chunk{
		ID: "image", TenantID: 1, KnowledgeBaseID: "kb-1", KnowledgeID: "doc",
		ImageInfo: `[{"url":"gallery-url","ocr_text":"gallery-ocr"}]`,
	}
	require.NoError(t, db.Create(chunk).Error)
	chunks := &galleryAfterRead{ChunkService: service.NewChunkService(
		repository.NewChunkRepository(db), repository.NewKnowledgeRepository(db), nil, nil, nil, nil, nil, nil,
	)}
	groups := &dataSourceGroupAccessStub{allowed: true}
	h := &KnowledgeHandler{kbService: &galleryTestKBService{}, chunkService: chunks, groupAccess: groups}
	router := gin.New()
	router.Use(middleware.ErrorHandler(), func(c *gin.Context) {
		c.Set(types.TenantIDContextKey.String(), uint64(1))
		c.Set(types.UserIDContextKey.String(), "reader")
		c.Next()
	})
	router.GET("/knowledge-bases/:id/images", h.ListImages)
	router.POST("/knowledge-bases/:id/images", h.ListImagesPost)
	for _, method := range []string{http.MethodGet, http.MethodPost} {
		t.Run(method, func(t *testing.T) {
			request := func() *httptest.ResponseRecorder {
				w := httptest.NewRecorder()
				router.ServeHTTP(w, httptest.NewRequest(method, "/knowledge-bases/kb-1/images", strings.NewReader("")))
				return w
			}
			groups.allowed = true
			allowed := request()
			require.Equal(t, http.StatusOK, allowed.Code, allowed.Body.String())
			for _, value := range []string{"gallery-url", "gallery-ocr", "source title", `"total":1`} {
				require.Contains(t, allowed.Body.String(), value)
			}
			chunks.after = func() { groups.allowed = false }
			denied := request()
			require.Equal(t, http.StatusForbidden, denied.Code, denied.Body.String())
			for _, value := range []string{"gallery-url", "gallery-ocr", "source title", "total"} {
				require.NotContains(t, denied.Body.String(), value)
			}
		})
	}
}
