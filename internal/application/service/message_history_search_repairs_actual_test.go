//go:build sqlite_fts5

package service

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func historySearchRepairFixture(
	t *testing.T,
) (*machinePipelineFixture, *messageService, context.Context, *types.Session) {
	t.Helper()
	t.Setenv("WEKNORA_DOCUMENT_GRAPH_HISTORY", "1")
	m := newMachinePipelineFixtureWithDocument(t, false, "md", true, true)
	h := prepareDocumentLiveHistory(m.f.ctx, t, m, "independent setup question")
	reader, ok := h.messages.messageRepo.(historySearchCandidateReader)
	require.True(t, ok)
	h.messages.messageRepo = &historySearchNoRawBodyRepository{
		MessageRepository:            h.messages.messageRepo,
		historySearchCandidateReader: reader,
	}
	session, err := h.messages.sessionRepo.Create(
		h.ctx,
		&types.Session{ID: uuid.NewString(), TenantID: 7, UserID: h.session.UserID},
	)
	require.NoError(t, err)
	return m, h.messages, h.ctx, session
}

// A test-only narrowing of the public projection decision. Actual material
// read guards still use the embedded real CheckBatch implementation; this
// adapter neither creates a receipt nor authorizes an otherwise denied source.
type historySearchClosedProjectionPolicy struct {
	access.SourceLineageAuthorizer
}

func (p historySearchClosedProjectionPolicy) Check(
	ctx context.Context,
	lineage *types.SourceLineage,
	scope access.LineageReadScope,
) error {
	if err := p.SourceLineageAuthorizer.Check(ctx, lineage, scope); err != nil {
		return err
	}
	return access.ErrNextcloudPublicationDenied
}

func TestHistorySearchActualClosedProjectionDoesNotMonopolizeCursor(t *testing.T) {
	_, messages, ctx, session := historySearchRepairFixture(t)
	base := time.Now().Add(-time.Hour).UTC()
	good, err := messages.CreateMessage(
		ctx,
		&types.Message{
			SessionID:   session.ID,
			RequestID:   "older-open",
			Role:        "user",
			Content:     "search marker older authored question",
			IsCompleted: true,
			CreatedAt:   base,
		},
	)
	require.NoError(t, err)
	question, err := messages.CreateMessage(
		ctx,
		&types.Message{
			SessionID:   session.ID,
			RequestID:   "closed-projection",
			Role:        "user",
			Content:     "independent question without the keyword",
			IsCompleted: true,
			CreatedAt:   base.Add(time.Second),
		},
	)
	require.NoError(t, err)
	// Follow the actual producer's empty placeholder -> completed body path.
	// This controlled literal completion is not a real-model acceptance claim.
	answer, err := messages.CreateMessage(
		ctx,
		&types.Message{
			SessionID: session.ID,
			RequestID: question.RequestID,
			Role:      "assistant",
			CreatedAt: base.Add(2 * time.Second),
		},
	)
	require.NoError(t, err)
	answer.Content = "search marker controlled producer answer"
	answer.SourceLineage, err = types.NewCompleteSourceLineage()
	require.NoError(t, err)
	answer.IsCompleted = true
	require.NoError(t, messages.UpdateMessage(ctx, answer))
	original, err := messages.beginMessageMaterialRead(
		ctx,
		types.MessageMaterialReadRequest{
			TenantID:   7,
			SessionID:  session.ID,
			MessageIDs: []string{question.ID, answer.ID},
		},
	)
	require.NoError(t, err, "the regression needs actual admitted material, not an unknown row")
	var originalAnswer *types.Message
	for _, row := range original.snapshot.Messages {
		if row.ID == answer.ID && row.Role == "assistant" {
			originalAnswer = row
		}
	}
	require.NotNil(t, originalAnswer)
	require.NoError(t, messages.CheckMessagePublication(original.Context(), originalAnswer))
	// Exercise the independently closed public projection without pretending
	// that any unsupported generated field has a valid producer contract.
	messages.sourceLineageGuard = historySearchClosedProjectionPolicy{
		SourceLineageAuthorizer: messages.sourceLineageGuard,
	}
	require.ErrorIs(
		t,
		messages.CheckMessagePublication(original.Context(), originalAnswer),
		access.ErrNextcloudPublicationDenied,
	)
	original.Close()

	current := WithMessageMaterialOutputBoundary(ctx)
	first, err := messages.SearchMessages(
		current,
		&types.MessageSearchParams{
			Query:      "search marker",
			Mode:       types.MessageSearchModeKeyword,
			Limit:      1,
			SessionIDs: []string{session.ID},
		},
	)
	require.NoError(t, err)
	require.Zero(t, first.Total)
	require.True(t, first.HasMore)
	require.NotEmpty(t, first.NextCursor)
	require.NoError(t, CheckMessageMaterialOutputBoundary(current))
	CloseMessageMaterialOutputBoundary(current)
	bound := &types.MessageSearchParams{
		Query:      "search marker",
		Mode:       types.MessageSearchModeKeyword,
		Limit:      1,
		SessionIDs: []string{session.ID},
		OwnerID:    session.UserID,
		Cursor:     first.NextCursor,
	}
	decoded, err := decodeHistorySearchCursor(ctx, bound)
	require.NoError(t, err)
	require.Empty(t, decoded.PendingIDs, "a closed projection is not a ranking-limit omission")

	current = WithMessageMaterialOutputBoundary(ctx)
	defer CloseMessageMaterialOutputBoundary(current)
	second, err := messages.SearchMessages(current, bound)
	require.NoError(t, err)
	require.Equal(t, 1, second.Total)
	require.Equal(t, good.Content, second.Items[0].QueryContent)
	require.False(t, second.HasMore)
	require.Empty(t, second.NextCursor)
	require.NoError(t, CheckMessageMaterialOutputBoundary(current))
}

func TestHistorySearchActualConsumedSteerPreservesAllOriginalQuestions(t *testing.T) {
	m, messages, ctx, session := historySearchRepairFixture(t)
	user, err := messages.CreateMessage(
		ctx,
		&types.Message{
			SessionID:   session.ID,
			RequestID:   "actual-search-steer-run",
			Role:        "user",
			Content:     "initial authored question",
			IsCompleted: true,
		},
	)
	require.NoError(t, err)
	run, err := messages.CreateMessage(
		ctx,
		&types.Message{SessionID: session.ID, RequestID: user.RequestID, Role: "assistant"},
	)
	require.NoError(t, err)
	_, generation, err := messages.MessageNativeLiveIdentity(ctx, session.ID, run.ID)
	require.NoError(t, err)
	request := &types.SteerAuthoredRequest{
		TenantID:        7,
		SessionID:       session.ID,
		RunMessageID:    run.ID,
		RunGenerationID: generation,
		RunRequestID:    run.RequestID,
		ClientSteerID:   uuid.NewString(),
		Query:           "authored injected search marker",
		Channel:         "web",
		Delivery:        types.SteerDeliveryInject,
	}
	marked, err := WithActualSteerHTTPInput(ctx, request)
	require.NoError(t, err)
	input, err := messages.AdmitActualSteerInput(marked, request)
	require.NoError(t, err)
	queue, _, closeQueue, err := messages.BeginSteerMaterialRead(
		ctx,
		&types.SteerMaterialRequest{
			TenantID:        7,
			SessionID:       session.ID,
			RunMessageID:    run.ID,
			RunGenerationID: generation,
			Use:             types.SteerMaterialInject,
		},
	)
	require.NoError(t, err)
	defer closeQueue()
	require.Len(t, queue.Inputs, 1)
	require.Equal(t, input.Proof.OriginID, queue.Inputs[0].Proof.OriginID)
	producer := WithMessageMaterialOutputBoundary(ctx)
	defer CloseMessageMaterialOutputBoundary(producer)
	producer = context.WithValue(producer, agentHistoryCurrentRequestKey{}, run.RequestID)
	_, err = messages.GetMessage(producer, session.ID, user.ID)
	require.NoError(t, err)
	require.NoError(t, messages.bindInitialMessageMaterial(producer, run.ID, user.ID, run.RequestID))
	_, err = messages.admittedMessagePage(producer, session.ID, time.Time{}, 20, 1, false)
	require.NoError(t, err)
	steer, consumption, check, closeInput, err := messages.ConsumeActualSteerIntoMessage(
		producer,
		queue.Inputs[0],
		run.ID,
		generation,
		run.RequestID,
	)
	require.NoError(t, err)
	defer closeInput()
	require.NoError(t, check())
	require.Equal(t, user.RequestID, steer.RequestID)
	require.NotEqual(t, user.ID, steer.ID)
	require.NoError(t, messages.materialStore.CheckSteerConsumption(producer, consumption))
	run.Content = "controlled completed answer after consumed guidance"
	run.SourceLineage, err = types.NewCompleteSourceLineage()
	require.NoError(t, err)
	run.IsCompleted = true
	require.NoError(t, messages.UpdateMessage(producer, run))
	require.NoError(t, CheckMessageMaterialOutputBoundary(producer))
	// Assert that the actual completion sealed the initial input and steer
	// parents; no source/header-based synthesized search receipt is installed.
	var stored struct{ ParentManifest string }
	require.NoError(
		t,
		m.f.db.Table("message_completion_input_manifests c").
			Select("c.parent_manifest").
			Joins("JOIN message_material_receipts r ON r.admission_id=c.assistant_admission_id").
			Where("r.message_id=?", run.ID).
			Take(&stored).
			Error,
	)
	var manifest types.MessageCompletionInputManifest
	require.NoError(t, json.Unmarshal([]byte(stored.ParentManifest), &manifest))
	require.Equal(t, user.ID, manifest.Initial.MessageID)
	require.Len(t, manifest.Steers, 1)
	require.Equal(t, steer.ID, manifest.Steers[0].User.MessageID)

	current := WithMessageMaterialOutputBoundary(ctx)
	defer CloseMessageMaterialOutputBoundary(current)
	result, err := messages.SearchMessages(
		current,
		&types.MessageSearchParams{
			Query:      "injected search marker",
			Mode:       types.MessageSearchModeKeyword,
			Limit:      1,
			SessionIDs: []string{session.ID},
		},
	)
	require.NoError(t, err)
	require.Equal(t, 1, result.Total)
	require.Equal(t, user.Content+"\n\n"+steer.Content, result.Items[0].QueryContent)
	require.Equal(t, run.Content, result.Items[0].AnswerContent)
	require.Equal(t, run.RequestID, result.Items[0].RequestID)
	require.NoError(t, CheckMessageMaterialOutputBoundary(current))

	// An externally changed consumed steer must invalidate the already-loaded
	// output and cannot be admitted through the surviving other group rows.
	require.NoError(
		t,
		m.f.db.Model(&types.Message{}).
			Where("id=?", steer.ID).
			Update("content", "foreign consumed-steer replacement").
			Error,
	)
	require.Error(t, CheckMessageMaterialOutputBoundary(current))
	CloseMessageMaterialOutputBoundary(current)
	fresh := WithMessageMaterialOutputBoundary(ctx)
	defer CloseMessageMaterialOutputBoundary(fresh)
	denied, err := messages.SearchMessages(
		fresh,
		&types.MessageSearchParams{
			Query:      "initial authored",
			Mode:       types.MessageSearchModeKeyword,
			Limit:      1,
			SessionIDs: []string{session.ID},
		},
	)
	require.NoError(t, err)
	require.Zero(t, denied.Total)
}

func TestHistorySearchMaterialBudgetDefersWholeExchangeWithoutSkipping(t *testing.T) {
	_, messages, ctx, session := historySearchRepairFixture(t)
	base := time.Now().Add(-time.Hour).UTC()
	for index, text := range []string{"earlier input", "later target marker"} {
		_, err := messages.CreateMessage(
			ctx,
			&types.Message{
				SessionID:   session.ID,
				RequestID:   "multi-input",
				Role:        "user",
				Content:     text,
				IsCompleted: true,
				CreatedAt:   base.Add(time.Duration(index) * time.Second),
			},
		)
		require.NoError(t, err)
	}
	reader := messages.messageRepo.(historySearchCandidateReader)
	metadata, err := reader.ListOwnedHistorySearchCandidates(
		ctx,
		7,
		session.UserID,
		[]string{session.ID},
		time.Time{},
		"",
		1,
	)
	require.NoError(t, err)
	require.Len(t, metadata, 1)
	remaining := 1
	guard, _, err := messages.beginHistorySearchPair(
		ctx,
		&types.MessageSearchParams{OwnerID: session.UserID},
		metadata[0],
		&remaining,
	)
	require.ErrorIs(t, err, errHistorySearchPageMaterialBudget)
	require.Nil(t, guard)
	require.Equal(t, 1, remaining, "no original body should be acquired when the complete cohort does not fit")
	remaining = 2
	guard, _, err = messages.beginHistorySearchPair(
		ctx,
		&types.MessageSearchParams{OwnerID: session.UserID},
		metadata[0],
		&remaining,
	)
	require.NoError(t, err)
	defer guard.Close()
	require.Len(t, guard.snapshot.Messages, 2)
	require.Zero(t, remaining)
}
