package session

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/middleware"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

type rawSearchSessionStub struct {
	interfaces.SessionService
	results  []*types.SearchResult
	onSearch func()
}

func (s *rawSearchSessionStub) SearchKnowledge(context.Context, []string, []string,
	[]types.TagScope, string,
) ([]*types.SearchResult, error) {
	if s.onSearch != nil {
		s.onSearch()
	}
	return s.results, nil
}

type rawSearchKnowledgeStub struct {
	interfaces.KnowledgeService
	row     *types.Knowledge
	checks  atomic.Int32
	onCheck func(int32) error
}

func (s *rawSearchKnowledgeStub) GetKnowledgeByIDOnly(context.Context, string) (*types.Knowledge, error) {
	return s.row, nil
}

func (s *rawSearchKnowledgeStub) CheckKnowledgePublication(context.Context, *types.Knowledge) error {
	n := s.checks.Add(1)
	if s.onCheck != nil {
		return s.onCheck(n)
	}
	return nil
}

type rawSearchKBStub struct {
	interfaces.KnowledgeBaseService
	kb *types.KnowledgeBase
}

func (s *rawSearchKBStub) GetKnowledgeBaseByIDOnly(context.Context, string) (*types.KnowledgeBase, error) {
	return s.kb, nil
}

func rawSearchLeaseDB(t *testing.T) *gorm.DB {
	t.Helper()
	path := filepath.Join(t.TempDir(), "raw-search-leases.db")
	db, err := gorm.Open(sqlite.Open("file:"+path+"?_busy_timeout=5000&_journal_mode=WAL"), &gorm.Config{})
	require.NoError(t, err)
	sqlDB, err := db.DB()
	require.NoError(t, err)
	sqlDB.SetMaxOpenConns(8)
	t.Cleanup(func() { _ = sqlDB.Close() })
	script, err := os.ReadFile("../../../migrations/sqlite/000046_nextcloud_content_leases.up.sql")
	require.NoError(t, err)
	require.NoError(t, db.Exec(string(script)).Error)
	return db
}

func rawSearchSource(t *testing.T) (*types.Knowledge, *types.SearchResult) {
	t.Helper()
	metadata, err := json.Marshal(map[string]string{
		"datasource_id": "source-1", "external_id": "nextcloud:instance:1",
		"nextcloud_instance_id": "instance", "nextcloud_file_id": "1",
	})
	require.NoError(t, err)
	row := &types.Knowledge{
		ID: "doc-1", TenantID: 7, KnowledgeBaseID: "kb-1",
		Channel: types.ConnectorTypeNextcloud, Metadata: types.JSON(metadata),
	}
	result := &types.SearchResult{
		ID: "chunk-1", KnowledgeID: row.ID,
		KnowledgeBaseID: row.KnowledgeBaseID, KnowledgeChannel: row.Channel,
		Metadata: row.GetMetadata(), Content: "private source chunk",
	}
	return row, result
}

func rawSearchScope(row *types.Knowledge) repository.NextcloudContentScope {
	return repository.NextcloudContentScope{
		TenantID:        row.TenantID,
		KnowledgeBaseID: row.KnowledgeBaseID, KnowledgeID: row.ID,
		DataSourceID: "source-1", ExternalID: "nextcloud:instance:1",
	}
}

func rawSearchRetiredAt(t *testing.T, db *gorm.DB, row *types.Knowledge) time.Time {
	t.Helper()
	var ms int64
	require.NoError(t, db.Table("nextcloud_content_fences").Select("retired_at_ms").
		Where("tenant_id = ? AND knowledge_base_id = ? AND knowledge_id = ?",
			row.TenantID, row.KnowledgeBaseID, row.ID).Row().Scan(&ms))
	return time.UnixMilli(ms)
}

func rawSearchCoverage(t *testing.T, db *gorm.DB) {
	t.Helper()
	ms := time.Now().UnixMilli() - 1000
	require.NoError(t, db.Exec(`INSERT INTO nextcloud_content_lease_coverage
		(tenant_id, knowledge_base_id, activated_at_ms, legacy_drained_at_ms,
		 reader_revision, builder_revision)
		VALUES (7, 'kb-1', ?, ?, 'test-reader', 'test-builder')`, ms, ms).Error)
}

func rawSearchResponse(h *Handler, body string) *httptest.ResponseRecorder {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(middleware.ErrorHandler(), func(c *gin.Context) {
		ctx := types.WithCaller(c.Request.Context(), types.Caller{
			TenantID: 7, UserID: "user-1", Role: types.TenantRoleContributor,
		})
		c.Request = c.Request.WithContext(ctx)
		c.Set(types.TenantIDContextKey.String(), uint64(7))
		c.Set(types.UserIDContextKey.String(), "user-1")
		c.Next()
	})
	r.POST("/knowledge-search", h.SearchKnowledge)
	w := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/knowledge-search", strings.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, request)
	return w
}

func rawSearchHandler(row *types.Knowledge, result *types.SearchResult,
	store *repository.NextcloudContentLeaseStore,
) (*Handler, *rawSearchSessionStub, *rawSearchKnowledgeStub) {
	session := &rawSearchSessionStub{results: []*types.SearchResult{result}}
	knowledge := &rawSearchKnowledgeStub{row: row}
	h := &Handler{
		sessionService: session, searchKnowledgeService: knowledge,
		knowledgebaseService: &rawSearchKBStub{kb: &types.KnowledgeBase{ID: "kb-1", TenantID: 7}},
		searchContentLeases:  store,
	}
	return h, session, knowledge
}

func assertRawSearchLeasesReleased(t *testing.T, db *gorm.DB, want int64) {
	t.Helper()
	var total, released int64
	require.NoError(t, db.Table("nextcloud_content_leases").Count(&total).Error)
	require.NoError(t, db.Table("nextcloud_content_leases").
		Where("released_at_ms IS NOT NULL").Count(&released).Error)
	require.Equal(t, want, total)
	require.Equal(t, want, released)
}

// A tag-only request still identifies its KB. The broad lease must exist
// before the retrieval service reads any chunk; retirement may hide results,
// but GC cannot claim the old generation until that request exits.
func TestRawSearchBroadLeaseCoversRetrievalAndBlocksGC(t *testing.T) {
	db := rawSearchLeaseDB(t)
	rawSearchCoverage(t, db)
	store := repository.NewNextcloudContentLeaseStore(db)
	row, result := rawSearchSource(t)
	h, session, knowledge := rawSearchHandler(row, result, store)
	var retired atomic.Bool
	var retireErr, claimErr error
	session.onSearch = func() {
		retireErr = store.RetireKnowledge(context.Background(), rawSearchScope(row))
		if retireErr != nil {
			return
		}
		retired.Store(true)
		_, claimErr = store.ClaimKnowledgeGC(context.Background(), rawSearchScope(row),
			rawSearchRetiredAt(t, db, row), time.Minute)
	}
	knowledge.onCheck = func(int32) error {
		if retired.Load() {
			return access.ErrNextcloudPublicationDenied
		}
		return nil
	}
	w := rawSearchResponse(h, `{"query":"private","mentioned_items":[{"type":"tag","id":"tag-1","kb_id":"kb-1"}]}`)
	require.NoError(t, retireErr)
	require.ErrorIs(t, claimErr, repository.ErrNextcloudContentGCBusy)
	require.Equal(t, http.StatusForbidden, w.Code, w.Body.String())
	require.NotContains(t, w.Body.String(), result.Content)
	assertRawSearchLeasesReleased(t, db, 1)
	claim, err := store.ClaimKnowledgeGC(context.Background(), rawSearchScope(row),
		rawSearchRetiredAt(t, db, row), time.Minute)
	require.NoError(t, err)
	require.NoError(t, store.FinishGCClaim(context.Background(), claim, false))
}

// A source can retire after the exact result lease is acquired and after
// JSON rendering starts. The writer performs a fresh source check before the
// first byte, while both leases still block GC claim.
func TestRawSearchOutputBoundaryRejectsRetiredResultAndBlocksGC(t *testing.T) {
	db := rawSearchLeaseDB(t)
	rawSearchCoverage(t, db)
	store := repository.NewNextcloudContentLeaseStore(db)
	row, result := rawSearchSource(t)
	h, _, knowledge := rawSearchHandler(row, result, store)
	var retireErr, claimErr error
	knowledge.onCheck = func(n int32) error {
		if n != 2 {
			return nil
		}
		retireErr = store.RetireKnowledge(context.Background(), rawSearchScope(row))
		if retireErr != nil {
			return retireErr
		}
		_, claimErr = store.ClaimKnowledgeGC(context.Background(), rawSearchScope(row),
			rawSearchRetiredAt(t, db, row), time.Minute)
		return access.ErrNextcloudPublicationDenied
	}
	w := rawSearchResponse(h, `{"query":"private","knowledge_base_id":"kb-1"}`)
	require.NoError(t, retireErr)
	require.ErrorIs(t, claimErr, repository.ErrNextcloudContentGCBusy)
	require.Equal(t, http.StatusForbidden, w.Code, w.Body.String())
	require.NotContains(t, w.Body.String(), result.Content)
	assertRawSearchLeasesReleased(t, db, 2)
}

func TestRawSearchDirectDocumentScopeReturnsSourceUnderExactLease(t *testing.T) {
	db := rawSearchLeaseDB(t)
	store := repository.NewNextcloudContentLeaseStore(db)
	row, result := rawSearchSource(t)
	h, _, knowledge := rawSearchHandler(row, result, store)
	w := rawSearchResponse(h, `{"query":"private","knowledge_ids":["doc-1"]}`)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	require.Contains(t, w.Body.String(), result.Content)
	require.GreaterOrEqual(t, knowledge.checks.Load(), int32(2))
	assertRawSearchLeasesReleased(t, db, 2)
}

func TestRawSearchRejectsChunkWithoutPersistedDocumentIdentity(t *testing.T) {
	h := &Handler{sessionService: &rawSearchSessionStub{results: []*types.SearchResult{{
		Content: "unattributed source text",
	}}}}
	w := rawSearchResponse(h, `{"query":"source","knowledge_base_id":"kb-1"}`)
	require.Equal(t, http.StatusServiceUnavailable, w.Code, w.Body.String())
	require.NotContains(t, w.Body.String(), "unattributed source text")
}
