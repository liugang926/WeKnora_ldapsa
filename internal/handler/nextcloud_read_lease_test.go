package handler

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/application/repository"
	apperrors "github.com/Tencent/WeKnora/internal/errors"
	"github.com/Tencent/WeKnora/internal/middleware"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

type leasePreviewService struct {
	interfaces.KnowledgeService
	knowledge *types.Knowledge
	open      func(context.Context) (io.ReadCloser, error)
}

type leaseSearchService struct {
	interfaces.KnowledgeService
	rows []*types.Knowledge
}

func (s *leaseSearchService) SearchKnowledge(context.Context, string, int, int,
	[]string,
) ([]*types.Knowledge, bool, int64, error) {
	return s.rows, false, int64(len(s.rows)), nil
}

func leaseSearchRouter(h *KnowledgeHandler) *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(middleware.ErrorHandler(), func(c *gin.Context) {
		c.Set(types.TenantIDContextKey.String(), uint64(7))
		c.Set(types.UserIDContextKey.String(), "test-user")
		c.Next()
	})
	r.GET("/knowledge/search", h.SearchKnowledge)
	return r
}

func (s *leasePreviewService) GetKnowledgeByIDOnly(context.Context, string) (*types.Knowledge, error) {
	return s.knowledge, nil
}

func (s *leasePreviewService) GetKnowledgeFile(ctx context.Context, _ string) (io.ReadCloser, string, error) {
	reader, err := s.open(ctx)
	return reader, "source.txt", err
}

type twoPhaseSource struct {
	step   int
	second func()
}

type leaseSeekFile struct{ *strings.Reader }

func (*leaseSeekFile) Close() error { return nil }

type leaseAcquireHookStore struct {
	*repository.NextcloudContentLeaseStore
	onAcquire func()
}

type leaseRevocableShare struct {
	interfaces.KBShareService
	revoked atomic.Bool
}

func (s *leaseRevocableShare) CheckTenantKBPermission(
	context.Context, string, uint64, types.TenantRole,
) (types.OrgMemberRole, bool, error) {
	if s.revoked.Load() {
		return "", false, nil
	}
	return types.OrgRoleViewer, true, nil
}

func (s *leaseAcquireHookStore) AcquireKnowledge(ctx context.Context, scope repository.NextcloudContentScope,
	kind repository.NextcloudContentLeaseKind, owner string, ttl time.Duration,
) (repository.NextcloudContentLease, error) {
	lease, err := s.NextcloudContentLeaseStore.AcquireKnowledge(ctx, scope, kind, owner, ttl)
	if err == nil && s.onAcquire != nil {
		s.onAcquire()
	}
	return lease, err
}

func (s *twoPhaseSource) Read(p []byte) (int, error) {
	switch s.step {
	case 0:
		s.step++
		return copy(p, "first"), nil
	case 1:
		s.step++
		if s.second != nil {
			s.second()
		}
		return copy(p, "second"), nil
	default:
		return 0, io.EOF
	}
}

func (*twoPhaseSource) Close() error { return nil }

func leaseHandlerDB(t *testing.T) *gorm.DB {
	t.Helper()
	path := filepath.Join(t.TempDir(), "handler-leases.db")
	db, err := gorm.Open(sqlite.Open("file:"+path+"?_busy_timeout=5000&_journal_mode=WAL"), &gorm.Config{})
	require.NoError(t, err)
	sqlDB, err := db.DB()
	require.NoError(t, err)
	sqlDB.SetMaxOpenConns(8)
	t.Cleanup(func() { _ = sqlDB.Close() })
	script, err := os.ReadFile("../../migrations/sqlite/000046_nextcloud_content_leases.up.sql")
	require.NoError(t, err)
	require.NoError(t, db.Exec(string(script)).Error)
	return db
}

func leaseHandlerKnowledge(t *testing.T) *types.Knowledge {
	t.Helper()
	metadata, err := json.Marshal(map[string]string{
		"datasource_id": "source-1", "external_id": "nextcloud:instance:1",
	})
	require.NoError(t, err)
	return &types.Knowledge{
		ID: "doc-1", TenantID: 7, KnowledgeBaseID: "kb-1",
		Channel: types.ConnectorTypeNextcloud, Metadata: types.JSON(metadata),
	}
}

func assertLeaseReleased(t *testing.T, db *gorm.DB) {
	t.Helper()
	var count, released int64
	require.NoError(t, db.Table("nextcloud_content_leases").Count(&count).Error)
	require.NoError(t, db.Table("nextcloud_content_leases").
		Where("released_at_ms IS NOT NULL").Count(&released).Error)
	require.EqualValues(t, 1, count)
	require.EqualValues(t, 1, released)
}

func TestGlobalKnowledgeSearchRetirementBlocksGCAndSourceOutput(t *testing.T) {
	db := leaseHandlerDB(t)
	store := repository.NewNextcloudContentLeaseStore(db)
	row := leaseHandlerKnowledge(t)
	row.Title = "private source title"
	nowMS := time.Now().UnixMilli()
	require.NoError(t, db.Exec(`INSERT INTO nextcloud_content_lease_coverage
		(tenant_id, knowledge_base_id, activated_at_ms, legacy_drained_at_ms,
		 reader_revision, builder_revision)
		VALUES (7, 'kb-1', ?, ?, 'test-reader', 'test-builder')`, nowMS-1000, nowMS-1000).Error)
	scope := repository.NextcloudContentScope{
		TenantID: 7, KnowledgeBaseID: "kb-1",
		KnowledgeID: row.ID, DataSourceID: "source-1", ExternalID: "nextcloud:instance:1",
	}
	var retired atomic.Bool
	var retireErr, claimErr error
	h := &KnowledgeHandler{
		kgService: &leaseSearchService{rows: []*types.Knowledge{row}},
		contentLeases: &leaseAcquireHookStore{
			NextcloudContentLeaseStore: store,
			onAcquire: func() {
				retireErr = store.RetireKnowledge(context.Background(), scope)
				if retireErr != nil {
					return
				}
				retired.Store(true)
				var retiredAtMS int64
				claimErr = db.Table("nextcloud_content_fences").Select("retired_at_ms").
					Where("tenant_id = 7 AND knowledge_base_id = 'kb-1' AND knowledge_id = ?", row.ID).
					Row().Scan(&retiredAtMS)
				if claimErr == nil {
					_, claimErr = store.ClaimKnowledgeGC(context.Background(), scope,
						time.UnixMilli(retiredAtMS), time.Minute)
				}
			},
		},
		publicationCheck: func(context.Context, *types.Knowledge) error {
			if retired.Load() {
				return apperrors.NewForbiddenError("source revoked")
			}
			return nil
		},
	}
	w := httptest.NewRecorder()
	leaseSearchRouter(h).ServeHTTP(w,
		httptest.NewRequest(http.MethodGet, "/knowledge/search?keyword=private", nil))
	require.NoError(t, retireErr)
	require.ErrorIs(t, claimErr, repository.ErrNextcloudContentGCBusy,
		"the active exact read lease must block a GC claim after retirement")
	require.Equal(t, http.StatusForbidden, w.Code)
	require.NotContains(t, w.Body.String(), row.Title)
	assertLeaseReleased(t, db)
	var retiredAtMS int64
	require.NoError(t, db.Table("nextcloud_content_fences").Select("retired_at_ms").
		Where("tenant_id = 7 AND knowledge_base_id = 'kb-1' AND knowledge_id = ?", row.ID).
		Row().Scan(&retiredAtMS))
	claim, err := store.ClaimKnowledgeGC(context.Background(), scope,
		time.UnixMilli(retiredAtMS), time.Minute)
	require.NoError(t, err, "the test-only coverage marker permits a claim after release")
	require.NoError(t, store.FinishGCClaim(context.Background(), claim, false))
}

func TestGlobalKnowledgeSearchRechecksPublicationAtJSONWrite(t *testing.T) {
	db := leaseHandlerDB(t)
	store := repository.NewNextcloudContentLeaseStore(db)
	row := leaseHandlerKnowledge(t)
	row.Title = "private source title"
	var checks atomic.Int32
	h := &KnowledgeHandler{
		kgService:     &leaseSearchService{rows: []*types.Knowledge{row}},
		contentLeases: store,
		publicationCheck: func(context.Context, *types.Knowledge) error {
			if checks.Add(1) >= 3 {
				return apperrors.NewForbiddenError("source revoked before response write")
			}
			return nil
		},
	}
	w := httptest.NewRecorder()
	leaseSearchRouter(h).ServeHTTP(w,
		httptest.NewRequest(http.MethodGet, "/knowledge/search?keyword=private", nil))
	require.GreaterOrEqual(t, checks.Load(), int32(3))
	require.Equal(t, http.StatusForbidden, w.Code)
	require.NotContains(t, w.Body.String(), row.Title)
	assertLeaseReleased(t, db)
}

func TestPreviewLeaseRetireDuringReadStopsLaterBytes(t *testing.T) {
	db := leaseHandlerDB(t)
	store := repository.NewNextcloudContentLeaseStore(db)
	knowledge := leaseHandlerKnowledge(t)
	var revoked atomic.Bool
	var retiredErr error
	svc := &leasePreviewService{knowledge: knowledge}
	svc.open = func(context.Context) (io.ReadCloser, error) {
		return &twoPhaseSource{second: func() {
			retiredErr = store.RetireKnowledge(context.Background(), repository.NextcloudContentScope{
				TenantID: 7, KnowledgeBaseID: "kb-1", KnowledgeID: "doc-1",
				DataSourceID: "source-1", ExternalID: "nextcloud:instance:1",
			})
			revoked.Store(true)
		}}, nil
	}
	h := &KnowledgeHandler{
		kgService: svc, contentLeases: store,
		kbService: &downloadKBStub{kb: &types.KnowledgeBase{ID: "kb-1", TenantID: 7}},
		publicationCheck: func(context.Context, *types.Knowledge) error {
			if revoked.Load() {
				return apperrors.NewForbiddenError("source revoked")
			}
			return nil
		},
	}
	r := documentHandlerRouter()
	r.GET("/knowledge/:id/preview", h.PreviewKnowledgeFile)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/knowledge/doc-1/preview", nil))
	require.NoError(t, retiredErr)
	require.NotContains(t, w.Body.String(), "second")
	assertLeaseReleased(t, db)
}

func TestPreviewLeaseAcquisitionRechecksPublicationBeforeFirstByte(t *testing.T) {
	db := leaseHandlerDB(t)
	store := repository.NewNextcloudContentLeaseStore(db)
	knowledge := leaseHandlerKnowledge(t)
	var revoked atomic.Bool
	svc := &leasePreviewService{
		knowledge: knowledge,
		open: func(context.Context) (io.ReadCloser, error) {
			return io.NopCloser(strings.NewReader("secret")), nil
		},
	}
	h := &KnowledgeHandler{
		kgService: svc,
		kbService: &downloadKBStub{kb: &types.KnowledgeBase{ID: "kb-1", TenantID: 7}},
		contentLeases: &leaseAcquireHookStore{
			NextcloudContentLeaseStore: store,
			onAcquire:                  func() { revoked.Store(true) },
		},
		publicationCheck: func(context.Context, *types.Knowledge) error {
			if revoked.Load() {
				return apperrors.NewForbiddenError("source revoked")
			}
			return nil
		},
	}
	r := documentHandlerRouter()
	r.GET("/knowledge/:id/preview", h.PreviewKnowledgeFile)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/knowledge/doc-1/preview", nil))
	require.NotContains(t, w.Body.String(), "secret")
	assertLeaseReleased(t, db)
}

func TestPreviewStopsAfterKnowledgeBaseShareRevokedMidstream(t *testing.T) {
	db := leaseHandlerDB(t)
	store := repository.NewNextcloudContentLeaseStore(db)
	knowledge := leaseHandlerKnowledge(t)
	knowledge.TenantID = 8
	share := &leaseRevocableShare{}
	svc := &leasePreviewService{
		knowledge: knowledge,
		open: func(context.Context) (io.ReadCloser, error) {
			return &twoPhaseSource{second: func() { share.revoked.Store(true) }}, nil
		},
	}
	h := &KnowledgeHandler{
		kgService: svc, contentLeases: store, kbShareService: share,
		kbService:        &downloadKBStub{kb: &types.KnowledgeBase{ID: "kb-1", TenantID: 8}},
		publicationCheck: func(context.Context, *types.Knowledge) error { return nil },
	}
	r := documentHandlerRouter()
	r.GET("/knowledge/:id/preview", h.PreviewKnowledgeFile)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/knowledge/doc-1/preview", nil))
	require.NotContains(t, w.Body.String(), "second")
	assertLeaseReleased(t, db)
}

func TestPreviewLeaseExpiryAndCancellationStopLaterBytes(t *testing.T) {
	for _, mode := range []string{"expired", "canceled"} {
		t.Run(mode, func(t *testing.T) {
			db := leaseHandlerDB(t)
			store := repository.NewNextcloudContentLeaseStore(db)
			knowledge := leaseHandlerKnowledge(t)
			var cancel context.CancelFunc
			var updateErr error
			svc := &leasePreviewService{knowledge: knowledge}
			svc.open = func(context.Context) (io.ReadCloser, error) {
				return &twoPhaseSource{second: func() {
					if mode == "canceled" {
						cancel()
					} else {
						updateErr = db.Exec(`UPDATE nextcloud_content_leases
							SET expires_at_ms = 1 WHERE knowledge_id = 'doc-1'`).Error
					}
				}}, nil
			}
			h := &KnowledgeHandler{
				kgService: svc, contentLeases: store,
				kbService:        &downloadKBStub{kb: &types.KnowledgeBase{ID: "kb-1", TenantID: 7}},
				leaseCheckpoint:  time.Nanosecond,
				publicationCheck: func(context.Context, *types.Knowledge) error { return nil },
			}
			r := documentHandlerRouter()
			r.GET("/knowledge/:id/preview", h.PreviewKnowledgeFile)
			base, stop := context.WithCancel(context.Background())
			cancel = stop
			defer stop()
			w := httptest.NewRecorder()
			r.ServeHTTP(w, httptest.NewRequest(http.MethodGet,
				"/knowledge/doc-1/preview", nil).WithContext(base))
			require.NoError(t, updateErr)
			require.NotContains(t, w.Body.String(), "second")
			assertLeaseReleased(t, db)
		})
	}
}

func TestPreviewWriteBoundaryRejectsRevocationAfterRead(t *testing.T) {
	db := leaseHandlerDB(t)
	store := repository.NewNextcloudContentLeaseStore(db)
	knowledge := leaseHandlerKnowledge(t)
	leaselist, err := acquireNextcloudHTTPReadLeases(context.Background(), store,
		[]*types.Knowledge{knowledge}, time.Minute)
	require.NoError(t, err)
	defer leaselist.Close()
	file := wrapNextcloudLeasedFile(io.NopCloser(strings.NewReader("secret")),
		leaselist, func(context.Context) error { return nil }, time.Minute)
	buf := make([]byte, 32)
	n, err := file.Read(buf)
	require.NoError(t, err)
	require.Equal(t, "secret", string(buf[:n]))
	_ = file.Close()
	var revoked atomic.Bool
	revoked.Store(true) // source changed after Read and before Write
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	guarded := &nextcloudAuthorizationWriter{
		ResponseWriter: c.Writer, leases: leaselist,
		check: func(context.Context) error {
			if revoked.Load() {
				return apperrors.NewForbiddenError("source revoked")
			}
			return nil
		},
	}
	written, err := guarded.Write(buf[:n])
	require.Error(t, err)
	require.Zero(t, written)
	require.Empty(t, w.Body.String())
}

func TestNextcloudAuthorizationWriterGinJSONDenialHasForbiddenStatus(t *testing.T) {
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c.Writer = &nextcloudAuthorizationWriter{
		ResponseWriter: c.Writer,
		leases:         &nextcloudHTTPReadLeases{ctx: ctx, cancel: cancel},
		check: func(context.Context) error {
			return apperrors.NewForbiddenError("source revoked")
		},
	}
	c.JSON(http.StatusOK, gin.H{"data": "source secret"})
	require.Equal(t, http.StatusForbidden, w.Code)
	require.NotContains(t, w.Body.String(), "source secret")
}

func TestPreviewWriteBoundaryRejectsRetiredLeaseAfterRead(t *testing.T) {
	db := leaseHandlerDB(t)
	store := repository.NewNextcloudContentLeaseStore(db)
	knowledge := leaseHandlerKnowledge(t)
	leases, err := acquireNextcloudHTTPReadLeases(context.Background(), store,
		[]*types.Knowledge{knowledge}, time.Minute)
	require.NoError(t, err)
	defer leases.Close()

	file := wrapNextcloudLeasedFile(io.NopCloser(strings.NewReader("secret")), leases,
		func(context.Context) error { return nil }, time.Minute)
	buf := make([]byte, 32)
	n, err := file.Read(buf)
	require.NoError(t, err)
	require.NoError(t, file.Close())
	require.NoError(t, store.RetireKnowledge(context.Background(), repository.NextcloudContentScope{
		TenantID: 7, KnowledgeBaseID: "kb-1", KnowledgeID: "doc-1",
		DataSourceID: "source-1", ExternalID: "nextcloud:instance:1",
	}))
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	guarded := &nextcloudAuthorizationWriter{ResponseWriter: c.Writer, leases: leases}
	written, err := guarded.Write(buf[:n])
	require.Error(t, err)
	require.Zero(t, written)
	require.Empty(t, w.Body.String())
}

func TestPreviewHeaderBoundaryHidesFileMetadataAfterRevocation(t *testing.T) {
	db := leaseHandlerDB(t)
	store := repository.NewNextcloudContentLeaseStore(db)
	leases, err := acquireNextcloudHTTPReadLeases(context.Background(), store,
		[]*types.Knowledge{leaseHandlerKnowledge(t)}, time.Minute)
	require.NoError(t, err)
	defer leases.Close()
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	guarded := &nextcloudAuthorizationWriter{
		ResponseWriter: c.Writer, leases: leases,
		check: func(context.Context) error { return apperrors.NewForbiddenError("source revoked") },
	}
	guarded.Header().Set("Content-Disposition", `attachment; filename="secret.txt"`)
	guarded.Header().Set("Content-Length", "100")
	guarded.WriteHeader(http.StatusOK)
	require.Equal(t, http.StatusForbidden, guarded.Status())
	require.Empty(t, guarded.Header().Get("Content-Disposition"))
	require.Empty(t, guarded.Header().Get("Content-Length"))
}

func TestPreviewBufferedRangeAndHeadResponses(t *testing.T) {
	db := leaseHandlerDB(t)
	store := repository.NewNextcloudContentLeaseStore(db)
	svc := &leasePreviewService{
		knowledge: leaseHandlerKnowledge(t),
		open: func(context.Context) (io.ReadCloser, error) {
			return &leaseSeekFile{Reader: strings.NewReader("abcdef")}, nil
		},
	}
	h := &KnowledgeHandler{
		kgService: svc, contentLeases: store,
		kbService:        &downloadKBStub{kb: &types.KnowledgeBase{ID: "kb-1", TenantID: 7}},
		publicationCheck: func(context.Context, *types.Knowledge) error { return nil },
	}
	r := documentHandlerRouter()
	r.GET("/knowledge/:id/preview", h.PreviewKnowledgeFile)
	r.HEAD("/knowledge/:id/preview", h.PreviewKnowledgeFile)
	for _, tc := range []struct {
		method, rangeHeader, body string
		status                    int
	}{
		{method: http.MethodGet, rangeHeader: "bytes=2-4", body: "cde", status: http.StatusPartialContent},
		{method: http.MethodHead, body: "", status: http.StatusOK},
	} {
		req := httptest.NewRequest(tc.method, "/knowledge/doc-1/preview", nil)
		if tc.rangeHeader != "" {
			req.Header.Set("Range", tc.rangeHeader)
		}
		w := httptest.NewRecorder()
		r.ServeHTTP(w, req)
		require.Equal(t, tc.status, w.Code, w.Body.String())
		require.Equal(t, tc.body, w.Body.String())
		if tc.rangeHeader != "" {
			require.Equal(t, "bytes 2-4/6", w.Header().Get("Content-Range"))
		}
	}
}

func TestLeasedFileChecksSourceAtCheckpointRatherThanEverySmallRead(t *testing.T) {
	db := leaseHandlerDB(t)
	store := repository.NewNextcloudContentLeaseStore(db)
	leases, err := acquireNextcloudHTTPReadLeases(context.Background(), store,
		[]*types.Knowledge{leaseHandlerKnowledge(t)}, time.Minute)
	require.NoError(t, err)
	defer leases.Close()

	checks := 0
	reader := wrapNextcloudLeasedFile(io.NopCloser(strings.NewReader("abcdef")), leases,
		func(context.Context) error { checks++; return nil }, time.Minute)
	defer func() { require.NoError(t, reader.Close()) }()
	buf := make([]byte, 1)
	for range 3 {
		_, err := reader.Read(buf)
		require.NoError(t, err)
	}
	require.Equal(t, 1, checks)
	require.NoError(t, reader.(*nextcloudLeasedFile).checkpoint(true))
	require.Equal(t, 2, checks)
}
