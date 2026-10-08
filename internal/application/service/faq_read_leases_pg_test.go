package service

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
)

func TestFAQPostgresActualParentReadPlansAndDurableLeaseCoverage(t *testing.T) {
	s, store, db, ctx := faqPostgresActualFixture(t)
	ddl, err := os.ReadFile("../../../migrations/versioned/000127_nextcloud_content_leases.up.sql")
	require.NoError(t, err)
	require.NoError(t, db.Exec(string(ddl)).Error)
	sqlDB, err := db.DB()
	require.NoError(t, err)
	sqlDB.SetMaxOpenConns(1)
	sqlDB.SetMaxIdleConns(1)
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	parentKB := &types.KnowledgeBase{ID: "pg-original-parent", TenantID: 7, Type: types.KnowledgeBaseTypeDocument, Name: "actual PostgreSQL parent"}
	require.NoError(t, store.CreateKnowledgeBaseWithInput(repository.WithIndependentKnowledgeBaseNameInput(repository.WithIndependentKnowledgeInput(ctx), "", parentKB.Name), parentKB))
	parent := &types.Knowledge{ID: "pg-parent", TenantID: 7, KnowledgeBaseID: parentKB.ID, Type: types.KnowledgeTypeManual, Channel: types.ChannelWeb, Source: types.KnowledgeTypeManual, Title: "actual original parent", Metadata: types.JSON(`{"manual":{"content":"actual PostgreSQL parent body","version":1}}`), CustomMetadata: types.JSON(`{}`)}
	_, err = store.CreateKnowledgeWithInput(repository.WithIndependentKnowledgeInput(ctx), parent, parent.Metadata)
	require.NoError(t, err)
	original, err := store.CaptureKnowledgeOutputInputs(ctx, types.KnowledgeBuildInputCaptureRequest{TenantID: 7, KnowledgeBaseID: parentKB.ID, KnowledgeID: parent.ID})
	require.NoError(t, err)
	input := &types.FAQEntryPayload{StandardQuestion: "actual PostgreSQL parent derivative", Answers: []string{"derived original parent answer"}}
	derived, err := store.WithDerivedFAQInputs(ctx, "kb", types.FAQInputOperation{Kind: "create", Entry: input}, []*types.KnowledgeBuildInputProof{original.Proof})
	require.NoError(t, err)
	_, err = s.CreateFAQEntry(derived, "kb", input)
	require.NoError(t, err)
	request := types.FAQInputReadRequest{TenantID: 7, KnowledgeBaseID: "kb", AllEntries: true}
	plan, err := store.PeekFAQInputReadPlan(ctx, request)
	require.NoError(t, err)
	require.Len(t, plan.Scopes, 2)
	unleased, err := repository.WithFAQInputReadPlan(ctx, plan)
	require.NoError(t, err)
	_, err = store.CaptureFAQInputs(unleased, request)
	require.ErrorIs(t, err, repository.ErrNextcloudContentGCUncovered)
	leases := store.ContentLeaseStore()
	actual := []repository.NextcloudContentLease{}
	for _, scope := range plan.Scopes {
		lease, err := leases.AcquireKBRead(ctx, scope.TenantID, scope.KBID, "actual-PG-FAQ-reader", time.Minute)
		require.NoError(t, err)
		actual = append(actual, lease)
	}
	t.Cleanup(func() {
		for _, lease := range actual {
			require.NoError(t, leases.ReleaseLease(context.Background(), lease.ID))
		}
	})
	covered, err := repository.WithFAQReadLeaseCoverage(ctx, actual)
	require.NoError(t, err)
	marked, err := repository.WithFAQInputReadPlan(covered, plan)
	require.NoError(t, err)
	rows, err := s.ListFAQEntries(marked, "kb", &types.Pagination{Page: 1, PageSize: 20}, nil, 0, "", "", "", nil)
	require.NoError(t, err)
	require.Equal(t, int64(1), rows.Total)
	require.NoError(t, db.Exec("UPDATE nextcloud_content_leases SET expires_at_ms=1 WHERE lease_id=?", actual[1].ID).Error)
	_, err = store.CaptureFAQInputs(marked, request)
	require.ErrorIs(t, err, repository.ErrNextcloudContentLeaseExpired)
}
