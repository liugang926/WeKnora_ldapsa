//go:build sqlite_fts5

package handler

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/application/service"
	chatpipeline "github.com/Tencent/WeKnora/internal/application/service/chat_pipeline"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// Attach the production search handler to the same JWT router and actual live
// producer database. No answer, source envelope or original receipt is seeded.
func addDocumentLiveHistorySearch(t *testing.T, router *gin.Engine, db *gorm.DB,
	store *repository.KnowledgeInputProvenanceStore, guard *access.NextcloudSourceLineageGuard,
	publication *access.NextcloudPublicationGuard, sources interfaces.DataSourceRepository,
	kbs interfaces.KnowledgeBaseService, knowledge interfaces.KnowledgeService,
	sessionID, token string,
) func(bool) {
	t.Helper()
	require.NotEmpty(t, sessionID)
	messages := service.NewMessageService(repository.NewMessageRepository(db), repository.NewSessionRepository(db), nil, kbs, knowledge, nil, nil, sources, publication)
	service.ConfigureMessageMaterialProvenance(messages, store)
	service.ConfigureMessageSourceLineage(messages, nil, &service.SessionForkService{}, chatpipeline.NewEventManager(), guard)
	repository.ConfigureMessageOriginalBodyAvailability(store, repository.CheckOriginalBodyAvailableInTx)
	h := NewMessageHandler(messages, nil, nil)
	router.POST("/messages/search", h.SearchMessages)
	return func(revoked bool) {
		stage := "before"
		if revoked {
			stage = "source_revoked"
		}
		for _, item := range []struct{ name, query string }{{"answer", "Bob"}, {"question_partner", "Who does Alice know on Project Cedar?"}} {
			t.Run("jwt_history_search_"+stage+"_"+item.name, func(t *testing.T) {
				body, err := json.Marshal(map[string]any{"query": item.query, "mode": "keyword", "limit": 20, "session_ids": []string{sessionID}})
				require.NoError(t, err)
				request := httptest.NewRequest(http.MethodPost, "/messages/search", bytes.NewReader(body))
				request.Header.Set("Content-Type", "application/json")
				request.Header.Set("Authorization", "Bearer "+token)
				w := httptest.NewRecorder()
				router.ServeHTTP(w, request)
				require.Equal(t, http.StatusOK, w.Code, "authenticated history search must remain usable")
				var response struct {
					Success bool                      `json:"success"`
					Data    types.MessageSearchResult `json:"data"`
				}
				require.NoError(t, json.Unmarshal(w.Body.Bytes(), &response))
				require.True(t, response.Success)
				if revoked {
					require.Zero(t, response.Data.Total)
					require.Empty(t, response.Data.Items)
					require.NotContains(t, strings.ToLower(w.Body.String()), "bob")
				} else {
					require.Equal(t, 1, response.Data.Total)
					require.Len(t, response.Data.Items, 1)
					require.Equal(t, sessionID, response.Data.Items[0].SessionID)
					require.Contains(t, strings.ToLower(response.Data.Items[0].AnswerContent), "bob")
				}
			})
		}
	}
}
