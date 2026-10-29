package handler

import (
	"context"
	"net/http/httptest"
	"testing"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

type officialChunkReadService struct {
	interfaces.ChunkService
	chunk       *types.Chunk
	getErr      error
	listResult  *types.PageResult
	listErr     error
	listTypes   []types.ChunkType
	listPage    types.Pagination
	listKnowIDs []string
}

func (s *officialChunkReadService) GetChunkByIDOnly(context.Context, string) (*types.Chunk, error) {
	return s.chunk, s.getErr
}

func (s *officialChunkReadService) ListPagedChunksByKnowledgeID(
	_ context.Context,
	knowledgeID string,
	page *types.Pagination,
	chunkTypes []types.ChunkType,
) (*types.PageResult, error) {
	s.listKnowIDs = append(s.listKnowIDs, knowledgeID)
	s.listPage = *page
	s.listTypes = append([]types.ChunkType(nil), chunkTypes...)
	return s.listResult, s.listErr
}

func serveOfficialChunkRead(t *testing.T, h *ChunkHandler, path string) *httptest.ResponseRecorder {
	t.Helper()
	t.Setenv("WEKNORA_NEXTCLOUD_READ_AUTHORITY", "weknora")
	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.GET("/chunks/by-id/:id", h.GetChunkByIDOnly)
	router.GET("/chunks/:knowledge_id", h.ListKnowledgeChunks)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, httptest.NewRequest("GET", path, nil))
	return w
}

func TestGetChunkByIDOnlyUsesWeKnoraACLWithoutSourceReceipts(t *testing.T) {
	service := &officialChunkReadService{
		chunk: &types.Chunk{
			ID:              "chunk",
			TenantID:        7,
			KnowledgeBaseID: "kb",
			KnowledgeID:     "document",
			Content:         "official read content",
			ChunkType:       types.ChunkTypeText,
		},
	}

	w := serveOfficialChunkRead(t, &ChunkHandler{service: service}, "/chunks/by-id/chunk")

	require.Equal(t, 200, w.Code, w.Body.String())
	require.Contains(t, w.Body.String(), "official read content")
	require.NotContains(t, w.Body.String(), "Current source and original material access is required")
}

func TestListKnowledgeChunksReturnsEmptyPageWithoutMaterialReceipts(t *testing.T) {
	service := &officialChunkReadService{
		listResult: types.NewPageResult(0, &types.Pagination{Page: 2, PageSize: 25}, []*types.Chunk{}),
	}

	w := serveOfficialChunkRead(
		t,
		&ChunkHandler{service: service},
		"/chunks/failed-nextcloud-document?page=2&page_size=25",
	)

	require.Equal(t, 200, w.Code, w.Body.String())
	require.JSONEq(t, `{"data":[],"page":2,"page_size":25,"success":true,"total":0}`, w.Body.String())
	require.Equal(t, []string{"failed-nextcloud-document"}, service.listKnowIDs)
	require.Equal(t, types.Pagination{Page: 2, PageSize: 25}, service.listPage)
	require.Equal(t, []types.ChunkType{types.ChunkTypeText}, service.listTypes)
}

func TestListKnowledgeChunksPreservesRequestedTypes(t *testing.T) {
	service := &officialChunkReadService{
		listResult: types.NewPageResult(0, &types.Pagination{Page: 1, PageSize: 10}, []*types.Chunk{}),
	}

	w := serveOfficialChunkRead(
		t,
		&ChunkHandler{service: service},
		"/chunks/document?chunk_type=image_caption&chunk_type=summary",
	)

	require.Equal(t, 200, w.Code, w.Body.String())
	require.Equal(
		t,
		[]types.ChunkType{types.ChunkTypeImageCaption, types.ChunkTypeSummary},
		service.listTypes,
	)
}
