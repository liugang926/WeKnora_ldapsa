//go:build sqlite_fts5

package handler

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/application/repository"
	sqlindex "github.com/Tencent/WeKnora/internal/application/repository/retriever/sqlite"
	"github.com/Tencent/WeKnora/internal/application/service"
	chatpipeline "github.com/Tencent/WeKnora/internal/application/service/chat_pipeline"
	"github.com/Tencent/WeKnora/internal/application/service/retriever"
	"github.com/Tencent/WeKnora/internal/models/embedding"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/gin-gonic/gin"
	"github.com/panjf2000/ants/v2"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// IDs and hashes are not model credentials. Provider rows remain encrypted in
// the owned producer database; the JWT reader uses the real model service.
type documentHistoryVectorSpec struct {
	KnowledgeBaseID  string
	KnowledgeID      string
	EmbeddingModelID string
	AnswerSHA256     string
}

type documentHTTPVectorModels struct {
	interfaces.ModelService
	queries atomic.Int64
}

type documentHTTPVectorEmbedder struct {
	embedding.Embedder
	queries *atomic.Int64
}

func (m *documentHTTPVectorModels) GetEmbeddingModel(ctx context.Context, id string) (embedding.Embedder, error) {
	provider, err := m.ModelService.GetEmbeddingModel(ctx, id)
	if err != nil {
		return nil, err
	}
	return documentHTTPVectorEmbedder{Embedder: provider, queries: &m.queries}, nil
}
func (e documentHTTPVectorEmbedder) Embed(ctx context.Context, input string) ([]float32, error) {
	e.queries.Add(1)
	return e.Embedder.Embed(ctx, input)
}

func documentHistoryVectorHTTPServices(t *testing.T, db *gorm.DB, store *repository.KnowledgeInputProvenanceStore,
	guard *access.NextcloudSourceLineageGuard, publication *access.NextcloudPublicationGuard,
	sources interfaces.DataSourceRepository, spec *documentHistoryVectorSpec,
) (interfaces.KnowledgeBaseService, *documentHTTPVectorModels) {
	t.Helper()
	require.NotNil(t, spec)
	require.NotEmpty(t, spec.KnowledgeBaseID)
	require.NotEmpty(t, spec.KnowledgeID)
	require.NotEmpty(t, spec.EmbeddingModelID)
	require.Len(t, spec.AnswerSHA256, 64)
	var tenant types.Tenant
	require.NoError(t, db.Where("id=7").Take(&tenant).Error)
	require.True(t, tenant.ChatHistoryConfig.IsConfigured())
	require.Equal(t, spec.KnowledgeBaseID, tenant.ChatHistoryConfig.KnowledgeBaseID)
	require.Equal(t, spec.EmbeddingModelID, tenant.ChatHistoryConfig.EmbeddingModelID)
	require.NotNil(t, tenant.RetrievalConfig)
	require.Empty(t, tenant.RetrievalConfig.RerankModelID, "this profile does not claim an actual reranker")
	pool, err := ants.NewPool(1)
	require.NoError(t, err)
	t.Cleanup(pool.Release)
	models := &documentHTTPVectorModels{ModelService: service.NewModelService(repository.NewModelRepository(db), repository.NewKnowledgeBaseRepository(db), nil, nil, embedding.NewBatchEmbedder(pool), nil)}
	registry := retriever.NewRetrieveEngineRegistry(nil, nil)
	require.NoError(t, registry.Register(retriever.NewKVHybridRetrieveEngine(sqlindex.NewSQLiteRetrieveEngineRepository(db), types.SQLiteRetrieverEngineType)))
	kbs := service.NewKnowledgeBaseService(repository.NewKnowledgeBaseRepository(db), repository.NewKnowledgeRepository(db), repository.NewChunkRepository(db), nil, nil,
		models, registry, nil, repository.NewTenantRepository(db), nil, nil, nil, nil, nil, nil, sources, nil, nil, nil, nil, nil, publication, repository.NewNextcloudContentLeaseStore(db))
	service.ConfigureKnowledgeBaseGroupAccess(kbs, service.NewGroupAccessService(repository.NewGroupAccessRepository(db)))
	service.ConfigureKnowledgeBaseRAGChunkHydration(kbs, repository.NewRAGChunkSnapshotReader(db, store), guard)
	return kbs, models
}

func documentHistoryVectorHTTPCheck(t *testing.T, router *gin.Engine, db *gorm.DB, token, sessionID string, spec *documentHistoryVectorSpec, models *documentHTTPVectorModels) func(bool) {
	t.Helper()
	return func(revoked bool) {
		stage := "before"
		if revoked {
			stage = "source_revoked"
		}
		for _, mode := range []string{"vector", "hybrid"} {
			t.Run("jwt_history_vector_"+stage+"_"+mode, func(t *testing.T) {
				payload, err := json.Marshal(map[string]any{"query": "Who does Alice know on Project Cedar?", "mode": mode, "limit": 3, "session_ids": []string{sessionID}})
				require.NoError(t, err)
				request := httptest.NewRequest(http.MethodPost, "/messages/search", bytes.NewReader(payload))
				request.Header.Set("Content-Type", "application/json")
				request.Header.Set("Authorization", "Bearer "+token)
				before := models.queries.Load()
				response := httptest.NewRecorder()
				router.ServeHTTP(response, request)
				require.Greater(t, models.queries.Load(), before, "JWT search must query actual BGE via production model service")
				if revoked {
					require.Equal(t, http.StatusForbidden, response.Code, "known NC share deletion must be explicit denial, never 200/zero; 503 is incomplete verification and cannot pass acceptance")
					for _, privateText := range []string{"Alice", "Bob", "Project Cedar"} {
						require.False(t, strings.Contains(response.Body.String(), privateText), "denied/unavailable HTTP output must contain no original body")
					}
					return
				}
				require.Equal(t, http.StatusOK, response.Code)
				var result struct {
					Success bool                      `json:"success"`
					Data    types.MessageSearchResult `json:"data"`
				}
				require.NoError(t, json.Unmarshal(response.Body.Bytes(), &result))
				require.True(t, result.Success)
				require.Equal(t, 1, result.Data.Total)
				require.Len(t, result.Data.Items, 1)
				group := result.Data.Items[0]
				require.Equal(t, sessionID, group.SessionID)
				require.Equal(t, sha256.Sum256([]byte("Who does Alice know on Project Cedar?")), sha256.Sum256([]byte(group.QueryContent)), "HTTP must publish the actual saved user question")
				require.True(t, strings.Contains(strings.ToLower(group.AnswerContent), "bob"), "actual RAG answer must retain the expected document answer")
				digest := sha256.Sum256([]byte(group.AnswerContent))
				require.Equal(t, spec.AnswerSHA256, hex.EncodeToString(digest[:]), "HTTP must publish the actual persisted RAG answer")
			})
		}
	}
}

// Genuine missing transaction authority, over the unchanged real model/index
// and original producer database. Vector/hybrid must report unavailable and
// cannot relabel an unknown original-material read as successful zero hits.
func documentHistoryVectorUnavailableAuthority(t *testing.T, router *gin.Engine, db *gorm.DB, guard *access.NextcloudSourceLineageGuard, publication *access.NextcloudPublicationGuard,
	sources interfaces.DataSourceRepository, kbs interfaces.KnowledgeBaseService, models interfaces.ModelService, token, sessionID string) {
	t.Helper()
	missing := repository.NewKnowledgeInputProvenanceStore(db)
	repository.ConfigureMessageOriginalBodyAvailability(missing, repository.CheckOriginalBodyAvailableInTx)
	messages := service.NewMessageService(repository.NewMessageRepository(db), repository.NewSessionRepository(db), nil, kbs, nil, models, nil, sources, publication)
	service.ConfigureMessageMaterialProvenance(messages, missing)
	service.ConfigureMessageSourceLineage(messages, nil, &service.SessionForkService{}, chatpipeline.NewEventManager(), guard)
	router.POST("/messages/search-no-material-authority", NewMessageHandler(messages, nil, nil).SearchMessages)
	for _, mode := range []string{"vector", "hybrid"} {
		t.Run("jwt_history_vector_unknown_authority_"+mode, func(t *testing.T) {
			payload, err := json.Marshal(map[string]any{"query": "Who does Alice know on Project Cedar?", "mode": mode, "limit": 3, "session_ids": []string{sessionID}})
			require.NoError(t, err)
			request := httptest.NewRequest(http.MethodPost, "/messages/search-no-material-authority", bytes.NewReader(payload))
			request.Header.Set("Content-Type", "application/json")
			request.Header.Set("Authorization", "Bearer "+token)
			response := httptest.NewRecorder()
			router.ServeHTTP(response, request)
			require.Equal(t, http.StatusServiceUnavailable, response.Code)
			for _, privateText := range []string{"Alice", "Bob", "Project Cedar"} {
				require.False(t, strings.Contains(response.Body.String(), privateText), "denied/unavailable HTTP output must contain no original body")
			}
		})
	}
}
