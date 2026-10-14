package handler

import (
	"context"
	"net/http"
	"net/http/httptest"
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
	"gorm.io/gorm"
)

type leasedListKnowledgeService struct {
	interfaces.KnowledgeService
	rows   []*types.Knowledge
	onList func()
}

func (s *leasedListKnowledgeService) ListPagedKnowledgeByKnowledgeBaseID(
	_ context.Context, _ string, page *types.Pagination, _ types.KnowledgeListFilter,
) (*types.PageResult, error) {
	if s.onList != nil {
		s.onList()
	}
	return types.NewPageResult(int64(len(s.rows)), page, s.rows), nil
}

type leasedListKBService struct {
	interfaces.KnowledgeBaseService
	kb *types.KnowledgeBase
}

func (s *leasedListKBService) GetKnowledgeBaseByID(context.Context, string) (*types.KnowledgeBase, error) {
	return s.kb, nil
}

func (s *leasedListKBService) GetKnowledgeBaseByIDOnly(context.Context, string) (*types.KnowledgeBase, error) {
	return s.kb, nil
}

func leasedListRouter(h *KnowledgeHandler) *gin.Engine {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.Use(middleware.ErrorHandler(), func(c *gin.Context) {
		ctx := types.WithCaller(c.Request.Context(), types.Caller{
			TenantID: 7, UserID: "user-1", Role: types.TenantRoleContributor,
		})
		c.Request = c.Request.WithContext(ctx)
		c.Set(types.TenantIDContextKey.String(), uint64(7))
		c.Set(types.UserIDContextKey.String(), "user-1")
		c.Next()
	})
	router.GET("/knowledge-bases/:id/knowledge", h.ListKnowledge)
	return router
}

func leaseListRequest(h *KnowledgeHandler) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	leasedListRouter(h).ServeHTTP(w, httptest.NewRequest(http.MethodGet,
		"/knowledge-bases/kb-1/knowledge?page=1&page_size=10", nil))
	return w
}

func markListLeaseCoverage(t *testing.T, db *gorm.DB, tenantID uint64) {
	t.Helper()
	nowMS := time.Now().UnixMilli() - 1000
	require.NoError(t, db.Exec(`INSERT INTO nextcloud_content_lease_coverage
		(tenant_id, knowledge_base_id, activated_at_ms, legacy_drained_at_ms,
		 reader_revision, builder_revision)
		VALUES (?, 'kb-1', ?, ?, 'test-reader', 'test-builder')`,
		tenantID, nowMS, nowMS).Error)
}

func listLeaseScope(row *types.Knowledge) repository.NextcloudContentScope {
	return repository.NextcloudContentScope{
		TenantID:        row.TenantID,
		KnowledgeBaseID: row.KnowledgeBaseID, KnowledgeID: row.ID,
		DataSourceID: "source-1", ExternalID: "nextcloud:instance:1",
	}
}

func retiredListLeaseTime(t *testing.T, db *gorm.DB, row *types.Knowledge) time.Time {
	t.Helper()
	var retiredAtMS int64
	require.NoError(t, db.Table("nextcloud_content_fences").Select("retired_at_ms").
		Where("tenant_id = ? AND knowledge_base_id = ? AND knowledge_id = ?",
			row.TenantID, row.KnowledgeBaseID, row.ID).
		Row().Scan(&retiredAtMS))
	return time.UnixMilli(retiredAtMS)
}

func assertListLeasesReleased(t *testing.T, db *gorm.DB, want int64) {
	t.Helper()
	var total, released int64
	require.NoError(t, db.Table("nextcloud_content_leases").Count(&total).Error)
	require.NoError(t, db.Table("nextcloud_content_leases").
		Where("released_at_ms IS NOT NULL").Count(&released).Error)
	require.Equal(t, want, total)
	require.Equal(t, want, released)
}

// Retirement between the list query and exact pin must not race a physical
// delete: the pre-query KB lease blocks GC while the handler denies the row.
func TestListKnowledgeBroadLeaseBlocksGCBeforeExactPin(t *testing.T) {
	db := leaseHandlerDB(t)
	markListLeaseCoverage(t, db, 7)
	store := repository.NewNextcloudContentLeaseStore(db)
	row := leaseHandlerKnowledge(t)
	row.Title = "source list secret"
	var retireErr, claimErr error
	var retired atomic.Bool
	svc := &leasedListKnowledgeService{rows: []*types.Knowledge{row}}
	svc.onList = func() {
		retireErr = store.RetireKnowledge(context.Background(), listLeaseScope(row))
		if retireErr != nil {
			return
		}
		retired.Store(true)
		_, claimErr = store.ClaimKnowledgeGC(context.Background(), listLeaseScope(row),
			retiredListLeaseTime(t, db, row), time.Minute)
	}
	h := &KnowledgeHandler{
		kgService:     svc,
		kbService:     &leasedListKBService{kb: &types.KnowledgeBase{ID: "kb-1", TenantID: 7}},
		contentLeases: store,
		publicationCheck: func(context.Context, *types.Knowledge) error {
			if retired.Load() {
				return apperrors.NewForbiddenError("source retired")
			}
			return nil
		},
	}
	w := leaseListRequest(h)
	require.NoError(t, retireErr)
	require.ErrorIs(t, claimErr, repository.ErrNextcloudContentGCBusy)
	require.Equal(t, http.StatusForbidden, w.Code, w.Body.String())
	require.NotContains(t, w.Body.String(), row.Title)
	assertListLeasesReleased(t, db, 1)
	claim, err := store.ClaimKnowledgeGC(context.Background(), listLeaseScope(row),
		retiredListLeaseTime(t, db, row), time.Minute)
	require.NoError(t, err, "GC may claim only after the broad reader exits")
	require.NoError(t, store.FinishGCClaim(context.Background(), claim, false))
}

// A source can retire after exact acquisition. Both broad and exact leases
// must block GC, and the response boundary must suppress its title.
func TestListKnowledgeExactLeaseBlocksGCAndRevokedOutput(t *testing.T) {
	db := leaseHandlerDB(t)
	markListLeaseCoverage(t, db, 7)
	store := repository.NewNextcloudContentLeaseStore(db)
	row := leaseHandlerKnowledge(t)
	row.Title = "source list secret"
	var retired atomic.Bool
	var retireErr, claimErr error
	wrapped := &leaseAcquireHookStore{
		NextcloudContentLeaseStore: store,
		onAcquire: func() {
			retireErr = store.RetireKnowledge(context.Background(), listLeaseScope(row))
			if retireErr != nil {
				return
			}
			retired.Store(true)
			_, claimErr = store.ClaimKnowledgeGC(context.Background(), listLeaseScope(row),
				retiredListLeaseTime(t, db, row), time.Minute)
		},
	}
	h := &KnowledgeHandler{
		kgService:     &leasedListKnowledgeService{rows: []*types.Knowledge{row}},
		kbService:     &leasedListKBService{kb: &types.KnowledgeBase{ID: "kb-1", TenantID: 7}},
		contentLeases: wrapped,
		publicationCheck: func(context.Context, *types.Knowledge) error {
			if retired.Load() {
				return apperrors.NewForbiddenError("source retired")
			}
			return nil
		},
	}
	w := leaseListRequest(h)
	require.NoError(t, retireErr)
	require.ErrorIs(t, claimErr, repository.ErrNextcloudContentGCBusy)
	require.Equal(t, http.StatusForbidden, w.Code, w.Body.String())
	require.NotContains(t, w.Body.String(), row.Title)
	assertListLeasesReleased(t, db, 2)
}

// A cached route grant cannot survive a share revocation between the final
// handler check and Gin's JSON write.
func TestListKnowledgeWriteBoundaryRechecksCurrentShare(t *testing.T) {
	db := leaseHandlerDB(t)
	store := repository.NewNextcloudContentLeaseStore(db)
	row := leaseHandlerKnowledge(t)
	row.TenantID = 8
	row.Title = "shared source secret"
	share := &leaseRevocableShare{}
	var checks atomic.Int32
	wrapped := &leaseAcquireHookStore{
		NextcloudContentLeaseStore: store,
		onAcquire:                  func() { share.revoked.Store(true) },
	}
	h := &KnowledgeHandler{
		kgService:      &leasedListKnowledgeService{rows: []*types.Knowledge{row}},
		kbService:      &leasedListKBService{kb: &types.KnowledgeBase{ID: "kb-1", TenantID: 8}},
		kbShareService: share, contentLeases: wrapped,
		publicationCheck: func(context.Context, *types.Knowledge) error {
			checks.Add(1)
			return nil
		},
	}
	w := leaseListRequest(h)
	require.Equal(t, int32(1), checks.Load())
	require.True(t, share.revoked.Load())
	require.Equal(t, http.StatusForbidden, w.Code, w.Body.String())
	require.False(t, strings.Contains(w.Body.String(), row.Title))
	assertListLeasesReleased(t, db, 2)
}

func TestListKnowledgeSourcePageReturnsUnderBothLeases(t *testing.T) {
	db := leaseHandlerDB(t)
	store := repository.NewNextcloudContentLeaseStore(db)
	row := leaseHandlerKnowledge(t)
	row.Title = "current source title"
	var checks atomic.Int32
	h := &KnowledgeHandler{
		kgService:     &leasedListKnowledgeService{rows: []*types.Knowledge{row}},
		kbService:     &leasedListKBService{kb: &types.KnowledgeBase{ID: "kb-1", TenantID: 7}},
		contentLeases: store,
		publicationCheck: func(context.Context, *types.Knowledge) error {
			checks.Add(1)
			return nil
		},
	}
	w := leaseListRequest(h)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	require.Contains(t, w.Body.String(), row.Title)
	require.GreaterOrEqual(t, checks.Load(), int32(2))
	assertListLeasesReleased(t, db, 2)
}
