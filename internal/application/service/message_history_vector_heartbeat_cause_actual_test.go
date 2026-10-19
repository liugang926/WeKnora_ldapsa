//go:build sqlite_fts5

package service

import (
	"context"
	"net/http"
	"sort"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
)

// These tests use the actual connector/RAG/history/child/native-index fixture.
// Only the external NC source and models are controlled. The same production
// heartbeat checkpoint is invoked once, without a fabricated error or proof.
func heldHistoryHeartbeatInputs(t *testing.T, ctx context.Context) ([]*MessageMaterialGuard, []*historyVectorMaterialBridge) {
	t.Helper()
	slot, ok := ctx.Value(messageMaterialOutputKey{}).(*messageMaterialOutputSlot)
	require.True(t, ok)
	slot.mu.Lock()
	guards := append([]*MessageMaterialGuard{}, slot.Guards...)
	bridges := append([]*historyVectorMaterialBridge{}, slot.HistoryVectorReads...)
	slot.mu.Unlock()
	require.NotEmpty(t, guards)
	require.NotEmpty(t, bridges)
	for _, guard := range guards {
		guard.stopOriginalHeartbeat()
	}
	return guards, bridges
}
func awaitHistoryHeartbeatCancel(t *testing.T, bridges []*historyVectorMaterialBridge) {
	t.Helper()
	for _, bridge := range bridges {
		select {
		case <-bridge.Context().Done():
		case <-time.After(time.Second):
			t.Fatal("actual original cancellation did not reach retained bridge")
		}
	}
}

// Obtain nonempty model inputs through the same server-owned native candidate,
// immutable parent admission and original hydration ports as production. No
// SearchResult, body, source proof or cancellation cause is manufactured here.
func actualHeartbeatHydratedInputs(t *testing.T, f *historyVectorActualFixture, ctx context.Context) (*historyVectorMaterialBridge, []*types.SearchResult, []string) {
	t.Helper()
	owner := types.SessionOwnerIDFromContext(ctx)
	params := &types.MessageSearchParams{Query: f.questions[0].Content, OwnerID: owner, Mode: types.MessageSearchModeVector, Limit: 20}
	owned, err := f.messages.materialStore.OwnedHistoryKnowledgeIDs(ctx, 7, historyVectorActualKB, owner, nil)
	require.NoError(t, err)
	require.NotEmpty(t, owned)
	candidates, err := f.kb.SearchHistoryVectorCandidates(ctx, historyVectorActualKB, owned, types.SearchParams{QueryText: params.Query, MatchCount: 20, VectorThreshold: .1, DisableKeywordsMatch: true})
	require.NoError(t, err)
	require.NotEmpty(t, candidates)
	selected := map[string]bool{}
	for _, candidate := range candidates {
		require.NotNil(t, candidate)
		selected[candidate.KnowledgeID] = true
	}
	ids := make([]string, 0, len(selected))
	for id := range selected {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	parents, err := f.messages.materialStore.PeekOwnedHistoryKnowledgeParentPlans(ctx, 7, historyVectorActualKB, owner, ids, nil)
	require.NoError(t, err)
	require.Len(t, parents, len(ids))
	bridge, err := f.messages.beginHistoryVectorMaterialBridge(ctx, params, historyVectorActualKB, parents)
	require.NoError(t, err)
	results, err := f.kb.HydrateHistoryVectorCandidates(bridge.Context(), historyVectorActualKB, candidates)
	require.NoError(t, err)
	require.NotEmpty(t, results)
	documents, err := bridge.modelDocuments(results)
	require.NoError(t, err)
	require.Len(t, documents, len(results))
	require.NotEmpty(t, documents)
	require.NoError(t, bridge.Checkpoint(bridge.Context()))
	return bridge, results, documents
}
func requireHeartbeatLeasesReleased(t *testing.T, f *historyVectorActualFixture) {
	t.Helper()
	require.Zero(t, f.activeLeases(t, ""))
	var bodies int64
	require.NoError(t, f.m.f.db.Table("original_body_leases").Where("released_at_ms IS NULL AND expires_at_ms>?", time.Now().UnixMilli()).Count(&bodies).Error)
	require.Zero(t, bodies)
}

func TestHistoryVectorActualSQLHeartbeatSourceDenialRetainsTypedHTTPRefusal(t *testing.T) {
	f := newHistoryVectorActualFixture(t)
	response := f.httpSearch(t, func(ctx context.Context, result *types.MessageSearchResult) {
		require.Equal(t, 2, result.Total)
		require.NoError(t, CheckMessageMaterialOutputBoundary(ctx))
		guards, bridges := heldHistoryHeartbeatInputs(t, ctx)
		lookups, ranks, streams := f.models.lookups.Load(), f.models.rankCalls.Load(), f.models.chatter.streams.Load()
		f.m.allow.Store(false)
		for _, guard := range guards {
			require.ErrorIs(t, guard.heartbeatRecheck(), access.ErrNextcloudPublicationDenied)
			require.ErrorIs(t, context.Cause(guard.Context()), access.ErrNextcloudPublicationDenied)
		}
		awaitHistoryHeartbeatCancel(t, bridges)
		for _, bridge := range bridges {
			require.ErrorIs(t, context.Cause(bridge.Context()), access.ErrNextcloudPublicationDenied)
		}
		for _, bridge := range bridges {
			_, err := runHistoryVectorRerank(bridge.Context(), &types.RetrievalConfig{RerankModelID: "retained-denial"}, "retained source", nil, nil, func() error { return bridge.Checkpoint(bridge.Context()) }, f.models.GetRerankModel)
			require.ErrorIs(t, err, access.ErrNextcloudPublicationDenied)
		}
		// The external HTTP owner itself is still alive. Only the actual owned
		// original-source checkpoint has cancelled the retained bridge.
		require.NoError(t, ctx.Err())
		require.ErrorIs(t, CheckMessageMaterialOutputBoundary(ctx), access.ErrNextcloudPublicationDenied)
		require.Equal(t, lookups, f.models.lookups.Load())
		require.Equal(t, ranks, f.models.rankCalls.Load())
		require.Equal(t, streams, f.models.chatter.streams.Load())
	})
	require.Equal(t, http.StatusForbidden, response.Code)
	for _, answer := range f.answers {
		require.NotContains(t, response.Body.String(), answer.Content)
	}
	require.Zero(t, f.activeLeases(t, ""))
	var bodies int64
	require.NoError(t, f.m.f.db.Table("original_body_leases").Where("released_at_ms IS NULL AND expires_at_ms>?", time.Now().UnixMilli()).Count(&bodies).Error)
	require.Zero(t, bodies)
}

func TestHistoryVectorActualSQLHeartbeatNonemptySourceDenialRetainsTypedModelRefusal(t *testing.T) {
	f := newHistoryVectorActualFixture(t)
	response := f.httpSearch(t, func(ctx context.Context, result *types.MessageSearchResult) {
		require.Equal(t, 2, result.Total)
		require.NoError(t, CheckMessageMaterialOutputBoundary(ctx))
		bridge, results, documents := actualHeartbeatHydratedInputs(t, f, ctx)
		guards, bridges := heldHistoryHeartbeatInputs(t, ctx)
		lookups, ranks, streams := f.models.lookups.Load(), f.models.rankCalls.Load(), f.models.chatter.streams.Load()
		f.m.allow.Store(false)
		for _, guard := range guards {
			require.ErrorIs(t, guard.heartbeatRecheck(), access.ErrNextcloudPublicationDenied)
			require.ErrorIs(t, context.Cause(guard.Context()), access.ErrNextcloudPublicationDenied)
		}
		awaitHistoryHeartbeatCancel(t, bridges)
		for _, bridge := range bridges {
			require.ErrorIs(t, context.Cause(bridge.Context()), access.ErrNextcloudPublicationDenied)
		}
		out, err := runHistoryVectorRerank(bridge.Context(), &types.RetrievalConfig{RerankModelID: "actual-history-rerank"}, f.questions[0].Content, results, documents, func() error { return bridge.Checkpoint(bridge.Context()) }, f.models.GetRerankModel)
		require.ErrorIs(t, err, access.ErrNextcloudPublicationDenied)
		require.Nil(t, out)
		// The external HTTP owner itself is still alive. Only the actual owned
		// original-source checkpoint has cancelled the retained bridge.
		require.NoError(t, ctx.Err())
		require.ErrorIs(t, CheckMessageMaterialOutputBoundary(ctx), access.ErrNextcloudPublicationDenied)
		require.Equal(t, lookups, f.models.lookups.Load())
		require.Equal(t, ranks, f.models.rankCalls.Load())
		require.Equal(t, streams, f.models.chatter.streams.Load())
	})
	require.Equal(t, http.StatusForbidden, response.Code)
	for _, answer := range f.answers {
		require.NotContains(t, response.Body.String(), answer.Content)
	}
	require.Zero(t, f.activeLeases(t, ""))
	var bodies int64
	require.NoError(t, f.m.f.db.Table("original_body_leases").Where("released_at_ms IS NULL AND expires_at_ms>?", time.Now().UnixMilli()).Count(&bodies).Error)
	require.Zero(t, bodies)
}

func TestHistoryVectorActualSQLHeartbeatReleasedOriginalLeaseRemainsUnavailable(t *testing.T) {
	f := newHistoryVectorActualFixture(t)
	response := f.httpSearch(t, func(ctx context.Context, result *types.MessageSearchResult) {
		require.Equal(t, 2, result.Total)
		guards, bridges := heldHistoryHeartbeatInputs(t, ctx)
		guard := guards[0]
		require.NotEmpty(t, guard.leases)
		require.NoError(t, f.messages.materialStore.ContentLeaseStore().ReleaseLease(f.base, guard.leases[0].ID))
		require.ErrorIs(t, guard.heartbeatRecheck(), repository.ErrNextcloudContentLeaseExpired)
		awaitHistoryHeartbeatCancel(t, bridges)
		require.True(t, f.m.allow.Load(), "source authority itself has not been revoked")
		for _, bridge := range bridges {
			require.ErrorIs(t, context.Cause(bridge.Context()), repository.ErrNextcloudContentLeaseExpired)
		}
		require.ErrorIs(t, CheckMessageMaterialOutputBoundary(ctx), access.ErrNextcloudPublicationUnavailable)
	})
	for _, answer := range f.answers {
		require.NotContains(t, response.Body.String(), answer.Content)
	}
	require.Zero(t, f.activeLeases(t, ""))
}

func TestHistoryVectorActualSQLHeartbeatNonemptyUnknownRefusesModel(t *testing.T) {
	for _, mode := range []string{"original-lease-released", "caller-cancelled", "caller-deadline"} {
		t.Run(mode, func(t *testing.T) {
			f := newHistoryVectorActualFixture(t)
			var request context.Context
			var cancel context.CancelFunc
			if mode == "caller-deadline" {
				// Use a real request deadline after fixture construction. There is no
				// injected cause or clock/lease mutation to stand in for its expiry.
				request, cancel = context.WithTimeout(f.base, 10*time.Second)
			} else {
				request, cancel = context.WithCancel(f.base)
			}
			defer cancel()
			ctx, _, check, releaseHTTP := BeginMessageMaterialHTTPBoundary(request)
			defer releaseHTTP()
			producer, releaseModel, external := retainMessageMaterialProducer(ctx)
			require.True(t, external)
			defer releaseModel()
			bridge, results, documents := actualHeartbeatHydratedInputs(t, f, producer)
			markMessageMaterialReady(ctx)
			releaseModel()
			require.NoError(t, check(), "actual nonempty retained inputs must be admitted before caller/lease failure")
			guards, bridges := heldHistoryHeartbeatInputs(t, ctx)
			lookups, ranks, streams := f.models.lookups.Load(), f.models.rankCalls.Load(), f.models.chatter.streams.Load()
			switch mode {
			case "original-lease-released":
				require.NotEmpty(t, guards[0].leases)
				require.NoError(t, f.messages.materialStore.ContentLeaseStore().ReleaseLease(f.base, guards[0].leases[0].ID))
				require.ErrorIs(t, guards[0].heartbeatRecheck(), repository.ErrNextcloudContentLeaseExpired)
				require.NoError(t, ctx.Err(), "the caller remains live after an original lease failure")
			case "caller-cancelled":
				cancel()
			case "caller-deadline":
				select {
				case <-request.Done():
				case <-time.After(12 * time.Second):
					t.Fatal("real caller deadline did not expire")
				}
			}
			awaitHistoryHeartbeatCancel(t, bridges)
			for _, held := range bridges {
				require.NotErrorIs(t, context.Cause(held.Context()), access.ErrNextcloudPublicationDenied)
				switch mode {
				case "original-lease-released":
					require.ErrorIs(t, context.Cause(held.Context()), repository.ErrNextcloudContentLeaseExpired)
				case "caller-cancelled":
					require.ErrorIs(t, context.Cause(held.Context()), context.Canceled)
				case "caller-deadline":
					require.ErrorIs(t, context.Cause(held.Context()), context.DeadlineExceeded)
				}
			}
			out, err := runHistoryVectorRerank(bridge.Context(), &types.RetrievalConfig{RerankModelID: "actual-history-rerank"}, f.questions[0].Content, results, documents, func() error { return bridge.Checkpoint(bridge.Context()) }, f.models.GetRerankModel)
			require.ErrorIs(t, err, access.ErrNextcloudPublicationUnavailable)
			require.NotErrorIs(t, err, access.ErrNextcloudPublicationDenied)
			require.Nil(t, out)
			require.ErrorIs(t, CheckMessageMaterialOutputBoundary(ctx), access.ErrNextcloudPublicationUnavailable)
			require.ErrorIs(t, check(), access.ErrNextcloudPublicationUnavailable)
			require.True(t, f.m.allow.Load(), "the external source has not been revoked")
			require.Equal(t, lookups, f.models.lookups.Load())
			require.Equal(t, ranks, f.models.rankCalls.Load())
			require.Equal(t, streams, f.models.chatter.streams.Load())
			releaseHTTP()
			requireHeartbeatLeasesReleased(t, f)
		})
	}
}
