package handler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"

	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/middleware"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/stretchr/testify/require"
)

type stubFolderKBService struct {
	interfaces.KnowledgeBaseService
}

func (s *stubFolderKBService) GetKnowledgeBaseByID(_ context.Context, id string) (*types.KnowledgeBase, error) {
	if id != "kb-1" {
		return nil, errors.New("kb not found")
	}
	return &types.KnowledgeBase{ID: "kb-1", TenantID: 1, Type: types.KnowledgeBaseTypeDocument}, nil
}

type stubFolderKGService struct {
	interfaces.KnowledgeService

	gotFilter      types.KnowledgeListFilter
	tree           *types.KnowledgeFolderTree
	treeErr        error
	nextcloudTotal int64
}

func (s *stubFolderKGService) ListPagedKnowledgeByKnowledgeBaseID(
	_ context.Context,
	_ string,
	page *types.Pagination,
	filter types.KnowledgeListFilter,
) (*types.PageResult, error) {
	s.gotFilter = filter
	if filter.Source == types.ConnectorTypeNextcloud {
		return types.NewPageResult(s.nextcloudTotal, page, []*types.Knowledge{}), nil
	}
	return types.NewPageResult(0, page, []*types.Knowledge{}), nil
}

func (s *stubFolderKGService) ListKnowledgeFolderTree(
	_ context.Context,
	_ string,
) (*types.KnowledgeFolderTree, error) {
	return s.tree, s.treeErr
}

func newFolderRouter(kg interfaces.KnowledgeService) *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(middleware.ErrorHandler())
	r.Use(func(c *gin.Context) {
		c.Set(types.TenantIDContextKey.String(), uint64(1))
		c.Set(types.UserIDContextKey.String(), "u-test")
		c.Next()
	})
	h := &KnowledgeHandler{kbService: &stubFolderKBService{}, kgService: kg}
	r.GET("/knowledge-bases/:id/knowledge", h.ListKnowledge)
	r.GET("/knowledge-bases/:id/knowledge/folders", h.ListKnowledgeFolders)
	return r
}

// originalFolderFixture authors fresh literal documents through the original
// input writer. It does not certify pre-existing folder names or cached totals.
func originalFolderFixture(t *testing.T, kbID string, folders ...string) *KnowledgeHandler {
	t.Helper()
	h, _, store, _, _, _ := collectionHTTPFixture(t)
	ctx := collectionHumanInputContext(7)
	if kbID != "kb" {
		kb := &types.KnowledgeBase{
			ID:       kbID,
			TenantID: 7,
			Type:     types.KnowledgeBaseTypeDocument,
			Name:     "Original folder test KB",
		}
		require.NoError(
			t,
			store.CreateKnowledgeBaseWithInput(
				repository.WithIndependentKnowledgeBaseNameInput(
					repository.WithIndependentKnowledgeInput(ctx),
					"",
					kb.Name,
				),
				kb,
			),
		)
	}
	for index, folder := range folders {
		row := &types.Knowledge{
			ID: fmt.Sprintf("folder-%d", index), TenantID: 7, KnowledgeBaseID: kbID,
			Type: types.KnowledgeTypeManual, Channel: "web", Source: "manual",
			Title: "New original folder document", FolderPath: folder,
			ParseStatus: types.ParseStatusCompleted, EnableStatus: "enabled",
			Metadata:       types.JSON(`{"manual":{"content":"new original folder body","version":1}}`),
			CustomMetadata: types.JSON(`{}`),
		}
		_, err := store.CreateKnowledgeWithInput(repository.WithIndependentKnowledgeInput(ctx), row, row.Metadata)
		require.NoError(t, err)
	}
	return h
}

// The folder dimension is keyed off parameter presence, because an empty
// folder_path is a meaningful value (the knowledge base root) and must be
// distinguishable from "browse every folder".
func TestListKnowledge_FolderScopeFromQuery(t *testing.T) {
	tests := []struct {
		name    string
		query   string
		wantIDs []string
	}{
		{"absent parameter keeps the flat listing", "", []string{"folder-0", "folder-1", "folder-2", "folder-3"}},
		{"empty parameter selects the root", "?folder_path=", []string{"folder-0"}},
		{"folder selects that folder only", "?folder_path=docs/spec", []string{"folder-2"}},
		{"recursive selects the subtree", "?folder_path=docs&folder_recursive=true", []string{"folder-1", "folder-2"}},
		{"recursive false stays exact", "?folder_path=docs&folder_recursive=false", []string{"folder-1"}},
		{"path is normalized", "?folder_path=/docs//spec/", []string{"folder-2"}},
		{"traversal cannot escape the root", "?folder_path=../../spec", []string{"folder-3"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := originalFolderFixture(t, "folder-kb", "", "docs", "docs/spec", "spec")
			w := serveCollectionHTTP(h, "/knowledge-bases/folder-kb/knowledge"+tt.query, false)

			if w.Code != http.StatusOK {
				t.Fatalf("status = %d, want 200; body=%s", w.Code, w.Body.String())
			}
			var body struct {
				Data []*types.Knowledge `json:"data"`
			}
			require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
			ids := make([]string, 0, len(body.Data))
			for _, row := range body.Data {
				ids = append(ids, row.ID)
			}
			require.ElementsMatch(t, tt.wantIDs, ids)
		})
	}
}

func TestListKnowledgeFolders_ReturnsTree(t *testing.T) {
	h := originalFolderFixture(t, "folder-kb", "", "docs/spec", "docs/spec")
	w := serveCollectionHTTP(h, "/knowledge-bases/folder-kb/knowledge/folders", false)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", w.Code, w.Body.String())
	}

	var body struct {
		Success bool                      `json:"success"`
		Data    types.KnowledgeFolderTree `json:"data"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("unmarshal response: %v; body=%s", err, w.Body.String())
	}
	if !body.Success {
		t.Fatalf("success = false; body=%s", w.Body.String())
	}
	if body.Data.RootDocumentCount != 1 || body.Data.TotalDocumentCount != 3 {
		t.Fatalf("counts = root %d total %d, want 1 / 3",
			body.Data.RootDocumentCount, body.Data.TotalDocumentCount)
	}
	if len(body.Data.Folders) != 1 || body.Data.Folders[0].Path != "docs" {
		t.Fatalf("folders = %+v, want a single \"docs\" root folder", body.Data.Folders)
	}
	if len(body.Data.Folders[0].Children) != 1 || body.Data.Folders[0].Children[0].Path != "docs/spec" {
		t.Fatalf("children = %+v, want \"docs/spec\"", body.Data.Folders[0].Children)
	}
}

func TestListKnowledgeFoldersRejectsUnverifiedNextcloudAggregates(t *testing.T) {
	kg := &stubFolderKGService{
		nextcloudTotal: 1,
		tree: types.BuildKnowledgeFolderTree([]*types.KnowledgeFolderCount{
			{FolderPath: "secret", Count: 1},
		}),
	}
	router := newFolderRouter(kg)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, httptest.NewRequest(
		http.MethodGet, "/knowledge-bases/kb-1/knowledge/folders", nil,
	))
	// No collection verifier is configured: fail before reading aggregate rows.
	if w.Code != http.StatusServiceUnavailable || strings.Contains(w.Body.String(), "secret") ||
		!strings.Contains(w.Body.String(), "Original knowledge collection verifier is unavailable") {
		t.Fatalf("folder aggregate leaked: status=%d body=%s", w.Code, w.Body.String())
	}
}
