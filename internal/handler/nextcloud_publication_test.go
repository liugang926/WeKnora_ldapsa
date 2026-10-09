package handler

import (
	"bytes"
	"context"
	"encoding/json"
	stderrors "errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/middleware"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

type publicationHandlerSources struct {
	source *types.DataSource
	err    error
}

func (s publicationHandlerSources) FindByID(context.Context, string) (*types.DataSource, error) {
	return s.source, s.err
}

type publicationHandlerDirectories struct{}

func (publicationHandlerDirectories) GetIdentityByUserID(context.Context, string) ([]*types.DirectoryIdentity, error) {
	return nil, nil
}

func (publicationHandlerDirectories) GetLoginSnapshot(context.Context, string,
	string,
) (*types.DirectoryLoginSnapshot, error) {
	return nil, nil
}

type publicationHandlerKnowledge struct {
	interfaces.KnowledgeService
	row *types.Knowledge
}

func (s publicationHandlerKnowledge) GetKnowledgeByIDOnly(context.Context, string) (*types.Knowledge, error) {
	return s.row, nil
}

func (s publicationHandlerKnowledge) GetKnowledgeByID(context.Context, string) (*types.Knowledge, error) {
	return s.row, nil
}

type publicationHandlerChunks struct {
	interfaces.ChunkService
	chunk *types.Chunk
}

func (s publicationHandlerChunks) GetChunkByIDOnly(context.Context, string) (*types.Chunk, error) {
	return s.chunk, nil
}

func publicationHandlerFixture(t *testing.T, selected bool) (*types.Knowledge, *types.DataSource) {
	t.Helper()
	metadata, err := json.Marshal(map[string]string{
		"datasource_id":         "source-1",
		"external_id":           "nextcloud:instance-1:42",
		"source_resource_id":    "binding-1",
		"nextcloud_instance_id": "instance-1",
		"nextcloud_binding_id":  "binding-1",
		"nextcloud_file_id":     "42",
	})
	require.NoError(t, err)
	resources := []string{}
	if selected {
		resources = []string{"binding-1"}
	}
	config, err := json.Marshal(types.DataSourceConfig{
		Type:        types.ConnectorTypeNextcloud,
		Settings:    map[string]interface{}{"base_url": "https://nextcloud.example"},
		Credentials: map[string]interface{}{"token": "source-token"},
		ResourceIDs: resources,
	})
	require.NoError(t, err)
	return &types.Knowledge{
			ID: "doc-1", TenantID: 7, KnowledgeBaseID: "kb-1",
			Channel: types.ConnectorTypeNextcloud, Metadata: types.JSON(metadata),
			Title: "secret Nextcloud title",
		}, &types.DataSource{
			ID: "source-1", TenantID: 7, KnowledgeBaseID: "kb-1",
			Type: types.ConnectorTypeNextcloud, Status: types.DataSourceStatusActive,
			Config: types.JSON(config),
		}
}

func TestGetKnowledgeFailsClosedForNextcloud(t *testing.T) {
	for _, tc := range []struct {
		name      string
		selected  bool
		sourceErr error
		wantHTTP  int
	}{
		{"source unavailable", true, stderrors.New("source database unavailable"), http.StatusServiceUnavailable},
		{"withdrawn binding", false, nil, http.StatusForbidden},
		{"missing linked identity", true, nil, http.StatusForbidden},
	} {
		t.Run(tc.name, func(t *testing.T) {
			knowledge, source := publicationHandlerFixture(t, tc.selected)
			h := &KnowledgeHandler{
				kgService: publicationHandlerKnowledge{row: knowledge},
				publicationGuard: access.NewNextcloudPublicationGuard(
					publicationHandlerDirectories{}, publicationHandlerSources{source: source, err: tc.sourceErr},
				),
			}
			r := documentHandlerRouter()
			r.GET("/knowledge/:id", h.GetKnowledge)
			w := httptest.NewRecorder()
			r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/knowledge/doc-1", nil))
			require.Equal(t, tc.wantHTTP, w.Code)
			require.NotContains(t, w.Body.String(), knowledge.Title)
		})
	}
}

func TestDirectChunkReadFailsClosedWithoutPublicationVerifier(t *testing.T) {
	knowledge, _ := publicationHandlerFixture(t, true)
	h := NewChunkHandler(
		publicationHandlerChunks{chunk: &types.Chunk{
			ID: "chunk-1", KnowledgeID: knowledge.ID,
			Content: "secret chunk text",
		}},
		publicationHandlerKnowledge{row: knowledge},
	)
	r := documentHandlerRouter()
	r.GET("/chunks/by-id/:id", h.GetChunkByIDOnly)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/chunks/by-id/chunk-1", nil))
	require.Equal(t, http.StatusServiceUnavailable, w.Code)
	require.False(t, strings.Contains(w.Body.String(), "secret chunk text"))
}

func TestUnrelatedKnowledgeDoesNotNeedNextcloudVerifier(t *testing.T) {
	knowledge := &types.Knowledge{ID: "ordinary", TenantID: 7, KnowledgeBaseID: "kb-1", Channel: "web"}
	h := &KnowledgeHandler{kgService: publicationHandlerKnowledge{row: knowledge}}
	r := documentHandlerRouter()
	r.GET("/knowledge/:id", h.GetKnowledge)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/knowledge/ordinary", nil))
	require.Equal(t, http.StatusOK, w.Code)
}

func TestBatchDownloadRejectsNextcloudBeforeOpeningAnyFile(t *testing.T) {
	knowledge, source := publicationHandlerFixture(t, true)
	knowledge.FilePath = "resource://source-file"
	svc := &downloadKnowledgeStub{items: []*types.Knowledge{knowledge}}
	h := &KnowledgeHandler{
		kgService: svc,
		kbService: &downloadKBStub{kb: &types.KnowledgeBase{
			ID: "kb-1", TenantID: 7,
		}},
		publicationGuard: access.NewNextcloudPublicationGuard(
			publicationHandlerDirectories{}, publicationHandlerSources{source: source},
		),
	}
	r := gin.New()
	r.Use(middleware.ErrorHandler(), func(c *gin.Context) {
		ctx := types.WithCaller(c.Request.Context(), types.Caller{
			TenantID: 7, UserID: "user-1", Role: types.TenantRoleContributor,
		})
		ctx = types.WithExecutionTenant(ctx, 7)
		c.Request = c.Request.WithContext(ctx)
		c.Set(types.TenantIDContextKey.String(), uint64(7))
		c.Next()
	})
	r.POST("/knowledge-bases/:id/knowledge/batch-download", h.BatchDownloadKnowledge)
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost,
		"/knowledge-bases/kb-1/knowledge/batch-download",
		bytes.NewBufferString(`{"ids":["doc-1"]}`))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusForbidden, w.Code)
	require.Empty(t, svc.opened)
}
