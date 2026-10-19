//go:build sqlite_fts5

package service

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/stretchr/testify/require"
)

// These methods are the unfenced legacy SQL body readers. All metadata ports,
// producer writes and material reads still use the actual production database.
type historySearchNoRawBodyRepository struct {
	interfaces.MessageRepository
	historySearchCandidateReader
}

func (r *historySearchNoRawBodyRepository) SearchMessagesByKeyword(
	context.Context,
	uint64,
	string,
	string,
	[]string,
	int,
) ([]*types.MessageWithSession, error) {
	return nil, fmt.Errorf("history search attempted an unfenced keyword SQL body read")
}

func (r *historySearchNoRawBodyRepository) GetMessagesByKnowledgeIDs(
	context.Context,
	[]string,
) ([]*types.MessageWithSession, error) {
	return nil, fmt.Errorf("history search attempted an unfenced vector SQL body read")
}

func (r *historySearchNoRawBodyRepository) GetMessagesByRequestIDs(
	context.Context,
	string,
	[]string,
) ([]*types.MessageWithSession, error) {
	return nil, fmt.Errorf("history search attempted an unfenced partner SQL body read")
}

func TestHistorySearchActualKeywordCursorOriginalBodiesAndOutputMutation(t *testing.T) {
	t.Setenv("WEKNORA_DOCUMENT_GRAPH_HISTORY", "1")
	m := newMachinePipelineFixtureWithDocument(t, false, "md", true, true)
	h := prepareDocumentLiveHistory(t, m, m.f.ctx, "controlled setup question")
	reader, ok := h.messages.messageRepo.(historySearchCandidateReader)
	require.True(t, ok)
	h.messages.messageRepo = &historySearchNoRawBodyRepository{
		MessageRepository:            h.messages.messageRepo,
		historySearchCandidateReader: reader,
	}
	session, err := h.messages.sessionRepo.Create(
		h.ctx,
		&types.Session{ID: h.session.ID + "-cursor", TenantID: 7, UserID: h.session.UserID},
	)
	require.NoError(t, err)
	base := time.Now().Add(-time.Hour).UTC()
	target, err := h.messages.CreateMessage(
		h.ctx,
		&types.Message{
			SessionID:   session.ID,
			RequestID:   "original-target",
			Role:        "user",
			Content:     "Actual authored target 100%_\\literal",
			IsCompleted: true,
			CreatedAt:   base,
		},
	)
	require.NoError(t, err)
	for index := 0; index < historySearchCandidateBudget; index++ {
		_, err = h.messages.CreateMessage(
			h.ctx,
			&types.Message{
				SessionID:   session.ID,
				RequestID:   fmt.Sprintf("padding-%d", index),
				Role:        "user",
				Content:     fmt.Sprintf("independently authored nonmatching question %d", index),
				IsCompleted: true,
				CreatedAt:   base.Add(time.Duration(index+1) * time.Second),
			},
		)
		require.NoError(t, err)
	}
	params := &types.MessageSearchParams{
		Query:      "100%_\\literal",
		Mode:       types.MessageSearchModeKeyword,
		Limit:      1,
		SessionIDs: []string{session.ID},
	}
	firstContext := WithMessageMaterialOutputBoundary(h.ctx)
	first, err := h.messages.SearchMessages(firstContext, params)
	require.NoError(t, err)
	require.Zero(t, first.Total)
	require.True(t, first.HasMore, "a bounded empty page cannot claim the older history has no matches")
	require.NotEmpty(t, first.NextCursor)
	require.NoError(t, CheckMessageMaterialOutputBoundary(firstContext))
	CloseMessageMaterialOutputBoundary(firstContext)
	var leases int64
	require.NoError(
		t,
		m.f.db.Table("original_body_leases").
			Where("released_at_ms IS NULL AND expires_at_ms>?", time.Now().UnixMilli()).
			Count(&leases).
			Error,
	)
	require.Zero(t, leases, "all 200 nonmatching exact original body holds must be released")

	for _, field := range []string{"query", "owner", "tenant", "mode", "sessions"} {
		t.Run("cursor_binding_"+field, func(t *testing.T) {
			ctx := h.ctx
			request := &types.MessageSearchParams{
				Query:      params.Query,
				Mode:       params.Mode,
				Limit:      1,
				SessionIDs: []string{session.ID},
				Cursor:     first.NextCursor,
			}
			switch field {
			case "query":
				request.Query = "different literal"
			case "owner":
				ctx = types.WithCaller(
					context.WithValue(ctx, types.UserIDContextKey, "different-reader"),
					types.Caller{TenantID: 7, UserID: "different-reader", Role: types.TenantRoleViewer},
				)
				ctx = types.WithPrincipal(ctx, types.Principal{Type: types.PrincipalWebUser, ID: "different-reader"})
			case "tenant":
				ctx = types.WithExecutionTenant(ctx, 8)
			case "mode":
				request.Mode = types.MessageSearchModeHybrid
			case "sessions":
				request.SessionIDs = []string{h.session.ID}
			}
			ctx = WithMessageMaterialOutputBoundary(ctx)
			defer CloseMessageMaterialOutputBoundary(ctx)
			got, err := h.messages.SearchMessages(ctx, request)
			require.ErrorIs(t, err, access.ErrNextcloudPublicationDenied)
			require.Nil(t, got)
		})
	}

	secondContext := WithMessageMaterialOutputBoundary(h.ctx)
	t.Cleanup(func() { CloseMessageMaterialOutputBoundary(secondContext) })
	second, err := h.messages.SearchMessages(
		secondContext,
		&types.MessageSearchParams{
			Query:      params.Query,
			Mode:       params.Mode,
			Limit:      1,
			SessionIDs: params.SessionIDs,
			Cursor:     first.NextCursor,
		},
	)
	require.NoError(t, err)
	require.Equal(t, 1, second.Total)
	require.False(t, second.HasMore)
	require.Empty(t, second.NextCursor)
	require.Equal(t, target.Content, second.Items[0].QueryContent)
	require.NoError(t, CheckMessageMaterialOutputBoundary(secondContext))
	require.NoError(
		t,
		m.f.db.Table("original_body_leases").
			Where("released_at_ms IS NULL AND expires_at_ms>?", time.Now().UnixMilli()).
			Count(&leases).
			Error,
	)
	require.Positive(t, leases, "a matching source must retain its real original-body read lease until output")

	// An actual foreign SQL write invalidates the immutable producer receipt.
	// No current row header may re-certify the replacement as a fresh input.
	require.NoError(
		t,
		m.f.db.Model(&types.Message{}).Where("id=?", target.ID).Update("content", "foreign replacement text").Error,
	)
	require.Error(t, CheckMessageMaterialOutputBoundary(secondContext))
	CloseMessageMaterialOutputBoundary(secondContext)
	fresh := WithMessageMaterialOutputBoundary(h.ctx)
	got, err := h.messages.SearchMessages(
		fresh,
		&types.MessageSearchParams{
			Query:      "foreign replacement",
			Mode:       types.MessageSearchModeKeyword,
			Limit:      1,
			SessionIDs: params.SessionIDs,
			Cursor:     first.NextCursor,
		},
	)
	// A cursor bound to a different literal is refused before any body read.
	require.ErrorIs(t, err, access.ErrNextcloudPublicationDenied)
	require.Nil(t, got)
	CloseMessageMaterialOutputBoundary(fresh)
	fresh = WithMessageMaterialOutputBoundary(h.ctx)
	got, err = h.messages.SearchMessages(
		fresh,
		&types.MessageSearchParams{
			Query:      params.Query,
			Mode:       params.Mode,
			Limit:      1,
			SessionIDs: params.SessionIDs,
			Cursor:     first.NextCursor,
		},
	)
	require.NoError(t, err)
	require.Zero(t, got.Total)
	require.False(t, got.HasMore, "unverified old row metadata cannot retain a cursor forever")
	CloseMessageMaterialOutputBoundary(fresh)

	// The current real producer refuses requestless material. Externally
	// clearing a legitimate raw user's request_id must keep the old receipt
	// invalid, not invent a certified standalone legacy body.
	_, err = h.messages.CreateMessage(
		h.ctx,
		&types.Message{SessionID: session.ID, Role: "user", Content: "requestless authored input", IsCompleted: true},
	)
	require.Error(t, err)
	require.NoError(t, m.f.db.Model(&types.Message{}).Where("id=?", target.ID).Update("request_id", "").Error)
	fresh = WithMessageMaterialOutputBoundary(h.ctx)
	got, err = h.messages.SearchMessages(
		fresh,
		&types.MessageSearchParams{
			Query:      params.Query,
			Mode:       params.Mode,
			Limit:      1,
			SessionIDs: params.SessionIDs,
			Cursor:     first.NextCursor,
		},
	)
	require.NoError(t, err)
	require.Zero(t, got.Total)
	require.False(t, got.HasMore)
	CloseMessageMaterialOutputBoundary(fresh)
	require.NoError(
		t,
		m.f.db.Table("original_body_leases").
			Where("released_at_ms IS NULL AND expires_at_ms>?", time.Now().UnixMilli()).
			Count(&leases).
			Error,
	)
	require.Zero(t, leases)
}

// A grouping/navigation regression, not a fabricated original-material
// receipt. The actual requestless-producer refusal is tested above.
func TestHistorySearchSoloGroupIdentityConsumesCorrectCursorCandidate(t *testing.T) {
	ctx := types.WithCaller(
		types.WithExecutionTenant(context.Background(), 7),
		types.Caller{TenantID: 7, UserID: "owner"},
	)
	params := &types.MessageSearchParams{Query: "same", Mode: types.MessageSearchModeKeyword, OwnerID: "owner"}
	first := &types.Message{
		ID:        "solo-1",
		SessionID: "session",
		Role:      "user",
		Content:   "same",
		CreatedAt: time.Now().UTC(),
	}
	second := &types.Message{
		ID:        "solo-2",
		SessionID: "session",
		Role:      "user",
		Content:   "same",
		CreatedAt: first.CreatedAt.Add(-time.Second),
	}
	groups := groupByRequestID(
		[]*types.MessageSearchResultItem{
			{MessageWithSession: types.MessageWithSession{Message: *first}},
			{MessageWithSession: types.MessageWithSession{Message: *second}},
		},
	)
	require.Len(t, groups, 2)
	require.Equal(t, first.ID, groups[0].MessageID)
	require.Equal(t, second.ID, groups[1].MessageID)
	cursor := historySearchCursorBinding(ctx, params)
	cursor.Before, cursor.BeforeID = second.CreatedAt, second.ID
	page := &historyKeywordPage{
		Hits:   []*types.MessageWithSession{{Message: *first}, {Message: *second}},
		Cursor: cursor,
	}
	result := &types.MessageSearchResult{Items: groups[:1], Total: 1}
	require.NoError(t, completeHistorySearchCursor(page, result))
	require.True(t, result.HasMore)
	params.Cursor = result.NextCursor
	decoded, err := decodeHistorySearchCursor(ctx, params)
	require.NoError(t, err)
	require.Equal(
		t,
		[]string{second.ID},
		decoded.PendingIDs,
		"returned solo identity must be consumed and only the omitted original requeued",
	)
	next := &historyKeywordPage{Hits: []*types.MessageWithSession{{Message: *second}}, Cursor: decoded}
	next.Cursor.PendingIDs = nil
	final := &types.MessageSearchResult{Items: groups[1:], Total: 1}
	require.NoError(t, completeHistorySearchCursor(next, final))
	require.False(t, final.HasMore)
	require.Empty(t, final.NextCursor)
}
