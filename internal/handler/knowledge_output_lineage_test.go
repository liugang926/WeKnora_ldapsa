package handler

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/application/service"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

type outputHTTPKB struct {
	interfaces.KnowledgeBaseService
	row *types.KnowledgeBase
}

func (k outputHTTPKB) GetKnowledgeBaseByIDOnly(context.Context, string) (*types.KnowledgeBase, error) {
	return k.row, nil
}

type outputHTTPFileReader struct {
	reader *strings.Reader
	closed bool
	onRead func()
}

func (f *outputHTTPFileReader) Read(p []byte) (int, error) {
	if f.onRead != nil {
		f.onRead()
		f.onRead = nil
	}
	return f.reader.Read(p)
}
func (f *outputHTTPFileReader) Close() error { f.closed = true; return nil }

type outputHTTPFiles struct {
	publicationHandlerKnowledge
	input *outputHTTPFileReader
}

func (f outputHTTPFiles) GetKnowledgeFile(context.Context, string) (io.ReadCloser, string, error) {
	return f.input, "authored.md", nil
}

func TestKnowledgeOutputFileActualHTTPBytesAndRevocation(t *testing.T) {
	for _, route := range []string{"download", "preview"} {
		for _, change := range []string{"exact_artifact", "range", "different_bytes", "withdraw_during_read"} {
			t.Run(route+"/"+change, func(t *testing.T) {
				temp := t.TempDir()
				t.Setenv("TMPDIR", temp)
				h, _, row, groups := outputHTTPFixture(t, true)
				body := "owned-output-body"
				if change == "different_bytes" {
					body = "unaccounted-file-bytes"
				}
				input := &outputHTTPFileReader{reader: strings.NewReader(body)}
				if change == "withdraw_during_read" {
					input.onRead = func() { groups.allowed = false }
				}
				h.kgService = outputHTTPFiles{publicationHandlerKnowledge: publicationHandlerKnowledge{row: row}, input: input}
				r := documentHandlerRouter()
				r.Use(func(c *gin.Context) {
					c.Request = c.Request.WithContext(context.WithValue(c.Request.Context(), types.PrincipalContextKey, types.Principal{Type: types.PrincipalWebUser, ID: "user"}))
					c.Next()
				})
				if route == "download" {
					r.GET("/knowledge/:id/file", h.DownloadKnowledgeFile)
				} else {
					r.GET("/knowledge/:id/file", h.PreviewKnowledgeFile)
				}
				request := httptest.NewRequest(http.MethodGet, "/knowledge/document/file", nil)
				if change == "range" {
					request.Header.Set("Range", "bytes=0-4")
				}
				w := httptest.NewRecorder()
				r.ServeHTTP(w, request)
				switch change {
				case "exact_artifact":
					require.Equal(t, http.StatusOK, w.Code, w.Body.String())
					require.Equal(t, "owned-output-body", w.Body.String())
				case "range":
					require.Equal(t, http.StatusPartialContent, w.Code, w.Body.String())
					require.Equal(t, "owned", w.Body.String())
				default:
					require.Equal(t, http.StatusForbidden, w.Code, w.Body.String())
					require.NotContains(t, w.Body.String(), "owned-output-body")
					require.NotContains(t, w.Body.String(), "unaccounted-file-bytes")
				}
				require.True(t, input.closed)
				leftover, err := filepath.Glob(filepath.Join(temp, "weknora-output-*"))
				require.NoError(t, err)
				require.Empty(t, leftover)
			})
		}
	}
}

type outputHTTPGroups struct {
	allowed bool
	calls   int
	hook    func(int)
}

func (g *outputHTTPGroups) EffectivePermission(context.Context, uint64, types.ResourceType, string, types.ResourceAction, time.Time) (types.EffectiveResourcePermission, error) {
	g.calls++
	if g.hook != nil {
		g.hook(g.calls)
	}
	return types.EffectiveResourcePermission{Allowed: g.allowed}, nil
}

func outputHTTPFixture(t *testing.T, admitted bool) (*KnowledgeHandler, *gorm.DB, *types.Knowledge, *outputHTTPGroups) {
	t.Helper()
	db, err := gorm.Open(sqlite.Open(filepath.Join(t.TempDir(), "output.db")), &gorm.Config{})
	require.NoError(t, err)
	sqlDB, err := db.DB()
	require.NoError(t, err)
	t.Cleanup(func() { _ = sqlDB.Close() })
	require.NoError(t, db.AutoMigrate(&types.KnowledgeBase{}, &types.Knowledge{}, &types.Chunk{}))
	// These empty local observation tables are not used as a source-free
	// proof. Only the explicit human admission below may certify this row.
	require.NoError(t, db.Exec(`CREATE TABLE nextcloud_source_versions(candidate_knowledge_id TEXT); CREATE TABLE nextcloud_source_revisions(candidate_knowledge_id TEXT)`).Error)
	script, err := os.ReadFile("../../migrations/sqlite/000053_knowledge_input_provenance.up.sql")
	require.NoError(t, err)
	require.NoError(t, db.Exec(string(script)).Error)
	db.Config.DisableForeignKeyConstraintWhenMigrating = true
	require.NoError(t, db.AutoMigrate(&types.Session{}, &types.Message{}))
	db.Config.DisableForeignKeyConstraintWhenMigrating = false
	script, err = os.ReadFile("../../migrations/sqlite/000054_knowledge_configuration_history_inputs.up.sql")
	require.NoError(t, err)
	require.NoError(t, db.Exec(string(script)).Error)
	script, err = os.ReadFile("../../migrations/sqlite/000055_knowledge_actual_config_controls.up.sql")
	require.NoError(t, err)
	require.NoError(t, db.Exec(string(script)).Error)
	kb := &types.KnowledgeBase{ID: "kb", TenantID: 7, Name: "human KB", Type: types.KnowledgeBaseTypeDocument}
	k := &types.Knowledge{ID: "document", TenantID: 7, KnowledgeBaseID: kb.ID, Type: types.KnowledgeTypeManual,
		Channel: "web", Source: "manual", Title: "owned-output-title", Metadata: types.JSON(`{"manual":{"content":"owned-output-body","version":1}}`), CustomMetadata: types.JSON(`{}`)}
	store := repository.NewKnowledgeInputProvenanceStore(db)
	require.NoError(t, store.CreateKnowledgeBaseWithInput(repository.WithIndependentKnowledgeInput(context.Background()), kb))
	if admitted {
		_, err = store.CreateKnowledgeWithInput(repository.WithIndependentKnowledgeInput(context.Background()), k, "owned-output-body")
	} else {
		err = db.Create(k).Error
	}
	require.NoError(t, err)
	require.NoError(t, db.Where("id = ?", k.ID).Take(k).Error)
	groups := &outputHTTPGroups{allowed: true}
	guard := access.NewNextcloudSourceLineageGuard(nil, outputHTTPKB{row: kb}, nil, nil, groups, nil)
	h := &KnowledgeHandler{kgService: publicationHandlerKnowledge{row: k}, kbService: outputHTTPKB{row: kb}}
	ConfigureKnowledgeHandlerOutputLineage(h, service.NewKnowledgeOutputAuthorizer(store, guard))
	return h, db, k, groups
}

func serveKnowledgeOutputHTTP(h *KnowledgeHandler) *httptest.ResponseRecorder {
	gin.SetMode(gin.TestMode)
	r := documentHandlerRouter()
	r.Use(func(c *gin.Context) {
		ctx := context.WithValue(c.Request.Context(), types.PrincipalContextKey, types.Principal{Type: types.PrincipalWebUser, ID: "user"})
		c.Request = c.Request.WithContext(ctx)
		c.Next()
	})
	r.GET("/knowledge/:id", h.GetKnowledge)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/knowledge/document", nil))
	return w
}

func TestKnowledgeOutputLineageActualHTTPRequiresOriginalMaterial(t *testing.T) {
	for _, tc := range []struct {
		name     string
		admitted bool
		want     int
	}{
		{"explicit_human_metadata", true, http.StatusOK},
		{"legacy_unproved_metadata", false, http.StatusForbidden},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h, _, _, _ := outputHTTPFixture(t, tc.admitted)
			w := serveKnowledgeOutputHTTP(h)
			require.Equal(t, tc.want, w.Code, w.Body.String())
			if tc.admitted {
				require.Contains(t, w.Body.String(), "owned-output-title")
				require.Contains(t, w.Body.String(), "owned-output-body")
				require.NotContains(t, w.Body.String(), "admission_id")
				require.NotContains(t, w.Body.String(), "source_lineage")
			} else {
				require.NotContains(t, w.Body.String(), "owned-output-title")
				require.NotContains(t, w.Body.String(), "owned-output-body")
			}
		})
	}
}

func TestKnowledgeOutputLineageActualHTTPStopsChangesBeforeResponse(t *testing.T) {
	for _, change := range []string{"withdraw_group", "wire_alias", "database_aba"} {
		t.Run(change, func(t *testing.T) {
			h, db, row, groups := outputHTTPFixture(t, true)
			groups.hook = func(call int) {
				// Begin has checked twice. Change state during the next actual
				// verification, before JSON rendering/output can disclose it.
				if call != 3 {
					return
				}
				switch change {
				case "withdraw_group":
					groups.allowed = false
				case "wire_alias":
					row.Title = "unaccounted changed wire body"
				case "database_aba":
					require.NoError(t, db.Model(row).Update("title", "changed").Error)
					require.NoError(t, db.Model(row).Update("title", "owned-output-title").Error)
				}
			}
			w := serveKnowledgeOutputHTTP(h)
			require.Equal(t, http.StatusForbidden, w.Code, w.Body.String())
			require.GreaterOrEqual(t, groups.calls, 3)
			require.NotContains(t, w.Body.String(), "owned-output-body")
			require.NotContains(t, w.Body.String(), "owned-output-title")
			require.NotContains(t, w.Body.String(), "unaccounted changed wire body")
		})
	}
}
