package router

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/application/service"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/hibiken/asynq"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

type enrichmentAccessProbe struct {
	interfaces.GroupAccessService
	allow     bool
	principal types.Principal
}

func (p *enrichmentAccessProbe) EffectivePermission(
	ctx context.Context, _ uint64, _ types.ResourceType, _ string, _ types.ResourceAction, _ time.Time,
) (types.EffectiveResourcePermission, error) {
	p.principal, _ = types.PrincipalFromContext(ctx)
	return types.EffectiveResourcePermission{Allowed: p.allow, Reason: "test"}, nil
}

type enrichmentKBLookup struct {
	interfaces.KnowledgeBaseService
}

func (enrichmentKBLookup) GetKnowledgeBaseByID(context.Context, string) (*types.KnowledgeBase, error) {
	return &types.KnowledgeBase{ID: "kb", TenantID: 7}, nil
}

type enrichmentAttemptTracker struct{ service.SpanTracker }

func (enrichmentAttemptTracker) LatestAttempt(context.Context, string) int { return 2 }

func TestEnrichmentMiddlewareRevocationStopsExecutionAndFinalizesAttempt(t *testing.T) {
	for _, tc := range []struct {
		name      string
		initiator types.TaskInitiator
		kb        string
		allow     bool
		newer     bool
	}{
		{name: "revoked user", initiator: types.TaskInitiator{UserID: "user-1"}, kb: "kb"},
		{name: "legacy payload", kb: "kb"},
		{
			name:      "API key with user attribution",
			initiator: types.TaskInitiator{UserID: "user-1", APIKeyID: 3}, kb: "kb",
		},
		{name: "moved document", initiator: types.TaskInitiator{UserID: "user-1"}, kb: "old-kb"},
		{name: "allowed current user", initiator: types.TaskInitiator{UserID: "user-1"}, kb: "kb", allow: true},
		{name: "legacy task with newer attempt", kb: "kb", newer: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, err := gorm.Open(sqlite.Open(filepath.Join(t.TempDir(), "tasks.db")), &gorm.Config{})
			require.NoError(t, err)
			sqlDB, err := db.DB()
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, sqlDB.Close()) })
			require.NoError(t, db.AutoMigrate(&types.Knowledge{}))
			require.NoError(t, db.Create(&types.Knowledge{
				ID: "doc", TenantID: 7, KnowledgeBaseID: "kb", ParseStatus: types.ParseStatusFinalizing,
				PendingSubtasksCount: 3, SummaryStatus: types.SummaryStatusPending,
			}).Error)
			repo := repository.NewKnowledgeRepository(db)
			probe := &enrichmentAccessProbe{allow: tc.allow}
			called := false
			var tracker service.SpanTracker
			if tc.newer {
				tracker = enrichmentAttemptTracker{}
			}
			handler := taskGroupAccessMiddleware(probe, enrichmentKBLookup{}, repo, nil, tracker)(
				asynq.HandlerFunc(func(context.Context, *asynq.Task) error { called = true; return nil }),
			)
			raw, err := json.Marshal(types.SummaryGenerationPayload{
				TenantID: 7, KnowledgeBaseID: tc.kb, KnowledgeID: "doc", Initiator: tc.initiator,
			})
			require.NoError(t, err)
			err = handler.ProcessTask(context.Background(), asynq.NewTask(types.TypeSummaryGeneration, raw))
			row, readErr := repo.GetKnowledgeByID(context.Background(), 7, "doc")
			require.NoError(t, readErr)
			if tc.allow {
				require.NoError(t, err)
				require.True(t, called)
				require.Equal(t, types.Principal{Type: types.PrincipalWebUser, ID: "user-1"}, probe.principal)
			} else {
				require.ErrorIs(t, err, asynq.SkipRetry)
				require.False(t, called)
				if tc.kb == "kb" && !tc.newer {
					require.Equal(t, types.ParseStatusFailed, row.ParseStatus)
					require.Equal(t, types.SummaryStatusFailed, row.SummaryStatus)
					require.Zero(t, row.PendingSubtasksCount)
				} else {
					require.Equal(t, types.ParseStatusFinalizing, row.ParseStatus)
					require.Equal(t, 3, row.PendingSubtasksCount)
				}
				if tc.initiator.UserID == "" || tc.initiator.APIKeyID > 0 {
					require.NotEqual(t, types.PrincipalWebUser, probe.principal.Type)
				}
			}
		})
	}
}

func TestSyncTaskExecutorStopsSkipRetryImmediately(t *testing.T) {
	var calls atomic.Int32
	done := make(chan struct{})
	executor := NewSyncTaskExecutor()
	executor.RegisterHandler("denied", func(context.Context, *asynq.Task) error {
		if calls.Add(1) == 1 {
			close(done)
		}
		return fmt.Errorf("authorization revoked: %w", asynq.SkipRetry)
	})
	_, err := executor.Enqueue(asynq.NewTask("denied", []byte(`{}`)), asynq.MaxRetry(25))
	require.NoError(t, err)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("executor did not run")
	}
	// A retry would run after five seconds. Verify the worker exits instead.
	time.Sleep(5100 * time.Millisecond)
	require.Equal(t, int32(1), calls.Load())
}
