package compaction

import (
	"context"
	"errors"
	"testing"

	"github.com/Tencent/WeKnora/internal/models/chat"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
)

func compactionIdentity(file, etag string, epoch uint64) types.SourceIdentity {
	return types.SourceIdentity{
		Provider:        "nextcloud",
		TenantID:        12,
		KnowledgeBaseID: "kb",
		DataSourceID:    "ds",
		PairOperationID: "pair",
		InstanceID:      "instance",
		BindingID:       "binding",
		FileID:          file,
		ExternalID:      "nextcloud:instance:" + file,
		KnowledgeID:     "k-" + file,
		Revision:        epoch,
		ETag:            etag,
	}
}

func compactionComplete(t *testing.T, identities ...types.SourceIdentity) *types.SourceLineage {
	t.Helper()
	l, err := types.NewCompleteSourceLineage(identities...)
	require.NoError(t, err)
	return l
}

func accountedStoredTurns(t *testing.T, a, b *types.SourceLineage) []chat.Message {
	return []chat.Message{
		{Role: "system", Content: "literal system", SourceLineage: compactionComplete(t)},
		{Role: "user", Content: "first human request", TurnID: "turn-1", SourceLineage: compactionComplete(t)},
		{Role: "assistant", Content: filler(1200), TurnID: "turn-1", SourceLineage: a},
		{Role: "user", Content: "second human request", TurnID: "turn-2", SourceLineage: compactionComplete(t)},
		{Role: "assistant", Content: filler(1200), TurnID: "turn-2", SourceLineage: b},
		{Role: "user", Content: "fresh raw question", SourceLineage: compactionComplete(t)},
	}
}

func TestAgentLineageCompactionCompleteCheckpointAndRawFailureInherit(t *testing.T) {
	a := compactionComplete(t, compactionIdentity("1", "original", 7))
	b := compactionComplete(t, compactionIdentity("1", "new", 8), compactionIdentity("2", "second", 5))
	union, _ := types.UnionSourceLineage(a, b)
	for _, failure := range []bool{false, true} {
		t.Run(map[bool]string{false: "summary", true: "failed raw archive"}[failure], func(t *testing.T) {
			llm := &stubChat{response: "## Goal\nCitation-free compacted meaning"}
			if failure {
				llm.err = errors.New("local stub failed")
			}
			settings := testSettings()
			settings.KeepRecentTokens = 250
			c := New(llm, newEstimator(t), settings)
			c.SetSourceLineageVerifier(
				func(_ context.Context, l *types.SourceLineage) error { return l.RequireComplete() },
			)
			result, err := c.Compact(context.Background(), accountedStoredTurns(t, a, b), ReasonThreshold)
			require.NoError(t, err)
			require.NotNil(t, result.Checkpoint)
			require.Equal(t, "turn-2", result.Checkpoint.TurnID)
			require.Equal(t, union, result.SourceLineage)
			require.Equal(t, union, result.Messages[1].SourceLineage)
			require.Equal(t, union, result.Checkpoint.SourceLineage)
			require.Equal(t, failure, result.Degraded)
			// The next/forked loader must carry the checkpoint's own envelope.
			next := SummaryMessage(result.Checkpoint.Summary)
			next.SourceLineage = result.Checkpoint.SourceLineage
			require.Equal(t, union, lineageOf(compactionComplete(t), []chat.Message{next}))
		})
	}
}

func TestAgentLineageCompactionSplitAndPreviousSummaryInheritBeforeOmission(t *testing.T) {
	a := compactionComplete(t, compactionIdentity("1", "original", 7))
	b := compactionComplete(t, compactionIdentity("2", "second", 5))
	union, _ := types.UnionSourceLineage(a, b)
	previous := SummaryMessage("previous source paraphrase with /workspace/old.txt")
	previous.SourceLineage = b
	messages := reactTurn(30)
	for i := range messages {
		messages[i].SourceLineage = a
	}
	messages[0].SourceLineage = compactionComplete(t)
	messages = append(messages[:1], append([]chat.Message{previous}, messages[1:]...)...)
	settings := testSettings()
	settings.KeepRecentTokens = 300
	settings.MaxContextTokens = 6000
	settings.ReserveTokens = 3000
	c := New(&stubChat{response: "## Goal\nretained meaning"}, newEstimator(t), settings)
	c.SetSourceLineageVerifier(func(_ context.Context, l *types.SourceLineage) error { return l.RequireComplete() })
	result, err := c.Compact(context.Background(), messages, ReasonOverflow)
	require.NoError(t, err)
	require.True(t, result.SplitTurn)
	require.Greater(t, result.Omitted, 0)
	require.Equal(t, union, result.SourceLineage)
	require.Equal(t, union, result.Messages[1].SourceLineage)
	require.Nil(t, result.Checkpoint, "split live turn must not certify stored host")
}

type revokeSummaryModel struct {
	*stubChat
	revoked *bool
}

func (m *revokeSummaryModel) ChatStream(
	ctx context.Context,
	messages []chat.Message,
	opts *chat.ChatOptions,
) (<-chan types.StreamResponse, error) {
	stream, err := m.stubChat.ChatStream(ctx, messages, opts)
	*m.revoked = true
	return stream, err
}

func TestAgentLineageCompactionRevocationCannotFallBackToReadableRawArchive(t *testing.T) {
	l := compactionComplete(t, compactionIdentity("1", "original", 7))
	revoked := false
	model := &revokeSummaryModel{stubChat: &stubChat{response: "source-derived summary"}, revoked: &revoked}
	c := New(model, newEstimator(t), testSettings())
	c.SetSourceLineageVerifier(func(_ context.Context, l *types.SourceLineage) error {
		if revoked {
			return errors.New("revoked")
		}
		return l.RequireComplete()
	})
	result, err := c.Compact(context.Background(), accountedStoredTurns(t, l, l), ReasonThreshold)
	require.Error(t, err)
	require.Nil(t, result)
	require.Equal(t, 1, model.calls)
}

func TestAgentLineageCompactionLegacyUnknownAndMissingVerifierBlockBeforeModel(t *testing.T) {
	l := compactionComplete(t)
	messages := accountedStoredTurns(t, l, l)
	for _, missing := range []bool{false, true} {
		t.Run(map[bool]string{false: "unknown input", true: "missing checker"}[missing], func(t *testing.T) {
			model := &stubChat{response: "must not run"}
			c := New(model, newEstimator(t), testSettings())
			if !missing {
				c.SetSourceLineageVerifier(
					func(_ context.Context, l *types.SourceLineage) error { return l.RequireComplete() },
				)
				messages[2].SourceLineage = nil
			}
			result, err := c.Compact(context.Background(), messages, ReasonThreshold)
			require.Error(t, err)
			require.Nil(t, result)
			require.Zero(t, model.calls)
		})
	}
}
