//go:build sqlite_fts5

package service

import (
	"context"
	"os"
	"testing"

	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
)

// All history rows are written by the production message service. The final
// body and lineage come from the real RAG stream, never a prebuilt answer.
type documentLiveHistory struct {
	messages        *messageService
	ctx             context.Context
	session         *types.Session
	user, assistant *types.Message
}

func prepareDocumentLiveHistory(
	t *testing.T,
	m *machinePipelineFixture,
	ctx context.Context,
	query string,
) *documentLiveHistory {
	t.Helper()
	if os.Getenv("WEKNORA_DOCUMENT_GRAPH_HISTORY") != "1" {
		return nil
	}
	db := m.f.db
	if !db.Migrator().HasColumn(&types.Message{}, "artifacts") {
		require.NoError(t, db.Exec("ALTER TABLE messages ADD COLUMN artifacts TEXT").Error)
	}
	for _, name := range []string{"000023_message_artifacts_table.up.sql", "000026_message_artifacts_deleted_at.up.sql"} {
		ddl, err := os.ReadFile("../../../migrations/sqlite/" + name)
		require.NoError(t, err)
		require.NoError(t, db.Exec(string(ddl)).Error)
	}
	require.NoError(t, repository.InstallOriginalBodyRetentionTriggers(db))
	require.NoError(t, repository.UpgradeMessageProducerAdvanceSchema(db))
	messages := &messageService{
		messageRepo:        repository.NewMessageRepository(db),
		sessionRepo:        repository.NewSessionRepository(db),
		sourceLineageGuard: m.f.guard,
		kbService:          m.f.s.kbService,
		knowService:        m.f.s,
	}
	ConfigureMessageMaterialProvenance(messages, m.f.store)
	repository.ConfigureMessageOriginalBodyAvailability(m.f.store, repository.CheckOriginalBodyAvailableInTx)
	ctx = context.WithValue(ctx, types.UserIDContextKey, types.CallerFromContext(ctx).UserID)
	session := &types.Session{ID: "live-history-" + m.ds.ID, TenantID: 7, UserID: types.CallerFromContext(ctx).UserID}
	require.NoError(t, db.Create(session).Error)
	m.liveHistorySessionID = session.ID
	execution := types.MessageExecutionContext{KnowledgeBaseIDs: []string{m.ds.KnowledgeBaseID}}
	user, err := messages.CreateMessage(
		ctx,
		&types.Message{
			SessionID:        session.ID,
			RequestID:        "live-rag-history-turn",
			Role:             "user",
			Content:          query,
			IsCompleted:      true,
			ExecutionContext: execution,
		},
	)
	require.NoError(t, err)
	assistant, err := messages.CreateMessage(
		ctx,
		&types.Message{
			SessionID:        session.ID,
			RequestID:        user.RequestID,
			Role:             "assistant",
			ExecutionContext: execution,
		},
	)
	require.NoError(t, err)
	return &documentLiveHistory{messages: messages, ctx: ctx, session: session, user: user, assistant: assistant}
}

func (h *documentLiveHistory) complete(
	t *testing.T,
	ctx context.Context,
	answer string,
	lineage *types.SourceLineage,
) func() {
	t.Helper()
	if h == nil {
		return func() {}
	}
	h.assistant.Content = answer
	h.assistant.SourceLineage = lineage
	h.assistant.IsCompleted = true
	require.NoError(t, h.messages.UpdateMessage(ctx, h.assistant))
	// New reader contexts cannot borrow the model's in-memory body or proof.
	read := WithMessageMaterialOutputBoundary(h.ctx)
	t.Cleanup(func() { CloseMessageMaterialOutputBoundary(read) })
	saved, err := h.messages.GetMessage(read, h.session.ID, h.assistant.ID)
	require.NoError(t, err)
	require.Equal(t, answer, saved.Content)
	require.Equal(t, lineage, saved.SourceLineage)
	require.NoError(t, CheckMessageMaterialOutputBoundary(read))
	checkPage := func(recent bool, revoked bool) {
		current := WithMessageMaterialOutputBoundary(h.ctx)
		defer CloseMessageMaterialOutputBoundary(current)
		var rows []*types.Message
		var e error
		if recent {
			rows, e = h.messages.GetRecentMessagesBySession(current, h.session.ID, 20)
		} else {
			rows, e = h.messages.GetMessagesBySession(current, h.session.ID, 1, 20)
		}
		if revoked {
			require.Error(t, e)
			require.Empty(t, rows)
			return
		}
		require.NoError(t, e)
		found := false
		for _, row := range rows {
			if row.ID == h.assistant.ID {
				found = true
				require.Equal(t, answer, row.Content)
				require.Equal(t, lineage, row.SourceLineage)
			}
		}
		require.True(t, found, "persisted assistant must be returned before revocation")
		require.NoError(t, CheckMessageMaterialOutputBoundary(current))
	}
	checkPage(false, false)
	checkPage(true, false)
	checkSearchRevoked := h.checkKeywordSearch(t, answer)
	t.Log("REAL_HISTORY actual streamed answer persisted with original material proof; detail/list/recent reads passed")
	return func() {
		require.Error(
			t,
			CheckMessageMaterialOutputBoundary(read),
			"retained historical body must recheck current source authority",
		)
		current := WithMessageMaterialOutputBoundary(h.ctx)
		denied, e := h.messages.GetMessage(current, h.session.ID, h.assistant.ID)
		require.Error(t, e)
		require.Nil(t, denied)
		CloseMessageMaterialOutputBoundary(current)
		checkPage(false, true)
		checkPage(true, true)
		checkSearchRevoked()
		CloseMessageMaterialOutputBoundary(read)
	}
}
