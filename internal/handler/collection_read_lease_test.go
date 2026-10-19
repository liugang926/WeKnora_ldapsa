package handler

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/application/service"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

func fixtureRetiredCollectionGeneration(
	t *testing.T,
	db *gorm.DB,
) (*repository.NextcloudContentLeaseStore, repository.NextcloudContentScope, time.Time) {
	t.Helper()
	store := repository.NewNextcloudContentLeaseStore(db)
	scope := repository.NextcloudContentScope{
		TenantID:        7,
		KnowledgeBaseID: "kb",
		KnowledgeID:     "old-generation",
		DataSourceID:    "old-source",
		ExternalID:      "nextcloud:instance:99",
	}
	require.NoError(t, store.RetireKnowledge(context.Background(), scope))
	var retired int64
	require.NoError(
		t,
		db.Table("nextcloud_content_fences").
			Where("knowledge_id=?", scope.KnowledgeID).
			Select("retired_at_ms").
			Scan(&retired).
			Error,
	)
	// This fixture enables only a claim contract; production coverage stays absent.
	require.NoError(
		t,
		db.Exec(
			("INSERT INTO nextcloud_content_lease_coverage VALUES (7,'kb',?,?, 'isolated"+
				"-reader-contract','isolated-builder-contract')"),
			retired,
			retired,
		).Error,
	)
	return store, scope, time.UnixMilli(retired)
}

func activeCollectionLeases(t *testing.T, db *gorm.DB) int64 {
	t.Helper()
	var n int64
	require.NoError(
		t,
		db.Table("nextcloud_content_leases").
			Where("tenant_id=7 AND knowledge_base_id='kb' AND released_at_ms IS NULL").
			Count(&n).
			Error,
	)
	return n
}

func TestCollectionHTTPReadLeaseBlocksGCThroughActualFAQTagAndList(t *testing.T) {
	for _, kind := range []string{"faq", "tag", "knowledge"} {
		t.Run(kind, func(t *testing.T) {
			var db *gorm.DB
			var groups *outputHTTPGroups
			var serve func() int
			switch kind {
			case "faq":
				h, _, actualDB, inputs, _, _, ctx := faqActualHTTPFixture(t)
				db = actualDB
				groups = &outputHTTPGroups{allowed: true}
				guard := access.NewNextcloudSourceLineageGuard(
					nil,
					faqHTTPKB{collectionHTTPKB{db: db}},
					nil,
					nil,
					groups,
					nil,
					inputs,
				)
				service.ConfigureFAQInputProvenance(h.knowledgeService, inputs, guard)
				serve = func() int { return serveFAQHTTP(ctx, h, http.MethodGet, "/knowledge-bases/kb/faq/entries", "").Code }
			case "tag":
				h, actualDB, _, actualGroups := tagHTTPFixture(t)
				db, groups = actualDB, actualGroups
				serve = func() int {
					return serveTagHTTP(

						collectionHumanInputContext(7), h,

						http.MethodGet,
						"/knowledge-bases/kb/tags",
						"",
					).Code
				}
			case "knowledge":
				h, actualDB, _, actualGroups, _, _ := collectionHTTPFixture(t)
				db, groups = actualDB, actualGroups
				serve = func() int {
					return serveCollectionHTTP(h, "/knowledge-bases/kb/knowledge", false).Code
				}
			}
			store, retired, notBefore := fixtureRetiredCollectionGeneration(t, db)
			observed := false
			groups.hook = func(int) {
				if observed {
					return
				}
				observed = true
				require.Positive(t, activeCollectionLeases(t, db))
				_, err := store.ClaimKnowledgeGC(context.Background(), retired, notBefore, time.Minute)
				require.ErrorIs(t, err, repository.ErrNextcloudContentGCBusy)
			}
			require.Equal(t, http.StatusOK, serve())
			require.True(t, observed)
			require.Zero(t, activeCollectionLeases(t, db))
			_, err := store.ClaimKnowledgeGC(context.Background(), retired, notBefore, time.Minute)
			require.NoError(t, err)
		})
	}
}

func TestCollectionHTTPReadLeaseExpirationSuppressesActualOutput(t *testing.T) {
	h, db, _, groups := tagHTTPFixture(t)
	groups.hook = func(call int) {
		if call == 1 {
			require.Positive(t, activeCollectionLeases(t, db))
			require.NoError(
				t,
				db.Exec("UPDATE nextcloud_content_leases SET expires_at_ms=1 WHERE released_at_ms IS NULL").Error,
			)
		}
	}
	w := serveTagHTTP(collectionHumanInputContext(7), h, http.MethodGet, "/knowledge-bases/kb/tags", "")
	require.NotEqual(t, http.StatusOK, w.Code)
	require.NotContains(t, w.Body.String(), "knowledge_count")
	require.Zero(t, activeCollectionLeases(t, db))
}

func TestCollectionHTTPReadLeaseRetirementCancelAndMissingStoreDeny(t *testing.T) {
	for _, mode := range []string{"retired", "cancelled", "missing_store"} {
		t.Run(mode, func(t *testing.T) {
			h, db, _, _ := tagHTTPFixture(t)
			ctx := collectionHumanInputContext(7)
			switch mode {
			case "retired":
				require.NoError(t, repository.NewNextcloudContentLeaseStore(db).RetireKB(context.Background(), 7, "kb"))
			case "cancelled":
				cancelled, cancel := context.WithCancel(ctx)
				cancel()
				ctx = cancelled
			case "missing_store":
				h.outputAuthorizer = &service.TagOutputAuthorizer{}
			}
			w := serveTagHTTP(ctx, h, http.MethodGet, "/knowledge-bases/kb/tags", "")
			require.NotEqual(t, http.StatusOK, w.Code)
			require.NotContains(t, w.Body.String(), "knowledge_count")
			require.Zero(t, activeCollectionLeases(t, db))
		})
	}
}
