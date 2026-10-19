//go:build sqlite_fts5

package service

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
)

// Exercise the production cross-session keyword and partner loading path.
// The only assistant body is the answer already persisted by the real RAG
// producer. The second session contains an independently authored raw user
// question, so a denied source must not suppress unrelated owned history.
func (h *documentLiveHistory) checkKeywordSearch(t *testing.T, answer string) func() {
	t.Helper()
	if os.Getenv("WEKNORA_DOCUMENT_GRAPH_HISTORY_SEARCH") != "1" {
		return func() {}
	}
	emptyTitleHeader, err := types.NewCompleteSourceLineage()
	require.NoError(t, err)
	controlSession, err := h.messages.sessionRepo.Create(h.ctx, &types.Session{
		ID: h.session.ID + "-search-control", TenantID: h.session.TenantID, UserID: h.session.UserID,
		Title: "Unverified legacy search title", TitleSourceLineage: emptyTitleHeader,
	})
	require.NoError(t, err)
	controlQuestion := "Owned independent search control: " + h.user.Content
	control, err := h.messages.CreateMessage(h.ctx, &types.Message{
		SessionID: controlSession.ID, RequestID: h.user.RequestID, Role: "user",
		Content: controlQuestion, IsCompleted: true,
	})
	require.NoError(t, err)
	require.NotEqual(t, h.user.ID, control.ID)
	require.NotContains(
		t,
		strings.ToLower(h.user.Content),
		"bob",
		"answer-only search must actually match the model-produced body",
	)

	search := func(ctx context.Context, query string, sessionIDs []string) *types.MessageSearchResult {
		current := WithMessageMaterialOutputBoundary(ctx)
		defer CloseMessageMaterialOutputBoundary(current)
		result, err := h.messages.SearchMessages(current, &types.MessageSearchParams{
			Query: query, Mode: types.MessageSearchModeKeyword, Limit: 20, SessionIDs: sessionIDs,
		})
		require.NoError(t, err)
		require.NotNil(t, result)
		require.Equal(t, len(result.Items), result.Total)
		require.NoError(t, CheckMessageMaterialOutputBoundary(current))
		return result
	}
	checkPair := func(group *types.MessageSearchGroupItem) {
		require.Equal(t, h.session.ID, group.SessionID)
		require.Equal(t, h.user.RequestID, group.RequestID)
		require.Equal(t, h.user.Content, group.QueryContent)
		require.Equal(t, answer, group.AnswerContent)
	}
	checkOwned := func(revoked bool) {
		// Matching the raw question must load and authorize its saved partner.
		// Both sessions deliberately reuse request_id to prove they cannot be
		// combined into one pair or borrow each other's authority.
		result := search(h.ctx, h.user.Content, nil)
		if revoked {
			require.Equal(t, 1, result.Total)
		} else {
			require.Equal(t, 2, result.Total)
		}
		foundSource, foundControl := false, false
		for _, group := range result.Items {
			require.NotNil(t, group)
			require.Empty(t, group.SessionTitle, "a current title lineage header alone must not certify title bytes")
			switch group.SessionID {
			case h.session.ID:
				require.False(
					t,
					revoked,
					"revoked answer and its matching question must be absent from cross-session search",
				)
				checkPair(group)
				foundSource = true
			case controlSession.ID:
				require.Equal(t, h.user.RequestID, group.RequestID)
				require.Equal(t, controlQuestion, group.QueryContent)
				require.Empty(
					t,
					group.AnswerContent,
					"a same-request-id answer from a different session must never be attached",
				)
				foundControl = true
			default:
				t.Fatalf("unexpected cross-session search result session")
			}
		}
		require.True(t, foundControl, "independent owned raw user history remains searchable")
		require.Equal(t, !revoked, foundSource)
		// A hit in the actual model answer must also disappear after NC share
		// deletion, even though the persisted body still contains the keyword.
		answerHit := search(h.ctx, "Bob", []string{h.session.ID})
		if revoked {
			require.Zero(t, answerHit.Total)
			require.Empty(t, answerHit.Items)
		} else {
			require.Equal(t, 1, answerHit.Total)
			checkPair(answerHit.Items[0])
		}
		// An explicit unrelated session filter cannot widen into the source
		// session while fetchPartnerMessages resolves the shared request id.
		controlOnly := search(h.ctx, h.user.Content, []string{controlSession.ID})
		require.Equal(t, 1, controlOnly.Total)
		require.Equal(t, controlSession.ID, controlOnly.Items[0].SessionID)
		require.Empty(t, controlOnly.Items[0].AnswerContent)
	}
	checkOwned(false)
	// Keep one actual search response under its request owner until the
	// orchestrator deletes the source share. It must carry the saved original
	// guards even though groupByRequestID publishes newly copied strings.
	retained := WithMessageMaterialOutputBoundary(h.ctx)
	t.Cleanup(func() { CloseMessageMaterialOutputBoundary(retained) })
	retainedResult, err := h.messages.SearchMessages(retained, &types.MessageSearchParams{
		Query: "Bob", Mode: types.MessageSearchModeKeyword, Limit: 20,
		SessionIDs: []string{h.session.ID},
	})
	require.NoError(t, err)
	require.Equal(t, 1, retainedResult.Total)
	checkPair(retainedResult.Items[0])
	require.NoError(t, CheckMessageMaterialOutputBoundary(retained))
	originalProjection := retainedResult.Items[0].AnswerContent
	retainedResult.Items[0].AnswerContent = originalProjection + " [unverified projection mutation]"
	require.ErrorIs(t, CheckMessageMaterialOutputBoundary(retained), access.ErrNextcloudPublicationDenied,
		"mutating grouped search output cannot leave original-row guards sufficient to publish")
	retainedResult.Items[0].AnswerContent = originalProjection
	require.NoError(t, CheckMessageMaterialOutputBoundary(retained))
	foreign := types.WithCaller(
		h.ctx,
		types.Caller{
			TenantID: h.session.TenantID,
			UserID:   "live-history-search-other-person",
			Role:     types.TenantRoleViewer,
		},
	)
	foreign = context.WithValue(foreign, types.UserIDContextKey, "live-history-search-other-person")
	foreign = types.WithPrincipal(
		foreign,
		types.Principal{Type: types.PrincipalWebUser, ID: "live-history-search-other-person"},
	)
	foreignResults := search(foreign, h.user.Content, nil)
	require.Zero(
		t,
		foreignResults.Total,
		"cross-session search must remain restricted to the authenticated person's owned conversations",
	)
	t.Log(
		("REAL_HISTORY_SEARCH real persisted RAG answer and question partner passed " +
			"across owned sessions; shared request IDs remain separate"),
	)
	return func() {
		require.Error(
			t,
			CheckMessageMaterialOutputBoundary(retained),
			"an already-loaded search answer must recheck actual current NC authority at output",
		)
		checkOwned(true)
		CloseMessageMaterialOutputBoundary(retained)
		t.Log(
			("REAL_HISTORY_SEARCH actual NC share deletion filtered both direct answer h" +
				"its and question partners; independent owned raw user history remains read" +
				"able"),
		)
	}
}
