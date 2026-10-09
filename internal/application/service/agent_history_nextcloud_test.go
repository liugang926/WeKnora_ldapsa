package service

import (
	"context"
	"errors"
	"testing"

	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/stretchr/testify/require"
)

type instanceHistorySources struct {
	interfaces.DataSourceRepository
	ever           bool
	err            error
	calls          int
	becomeSourceAt int
}

func (s *instanceHistorySources) HasEverNextcloudSourceGlobally(context.Context) (bool, error) {
	s.calls++
	return s.ever || (s.becomeSourceAt > 0 && s.calls >= s.becomeSourceAt), s.err
}

func TestAgentHistoryAdmissionBeforeCheckpointAndTokenPage(t *testing.T) {
	for _, scenario := range []string{"revoked-older-than-page", "checkpoint", "unavailable", "missing-verifier"} {
		t.Run(scenario, func(t *testing.T) {
			rows := storedTurnsOfSize(250, 100)
			rows[1].KnowledgeReferences = types.References{{
				KnowledgeID:      "old-denied-source",
				KnowledgeChannel: types.ConnectorTypeNextcloud,
			}}
			rows[len(rows)-1].Content = "new paraphrase without citations"
			repo := &historyRepo{rows: rows, checkpoint: checkpointOn(rows[1], "secret compressed source")}
			sources := &instanceHistorySources{ever: true}
			guard := access.NewNextcloudHistoryGuard(nil, sources, nil)
			if scenario == "unavailable" {
				sources.ever = false
				sources.err = errors.New("DB unavailable")
			}
			if scenario == "missing-verifier" {
				guard = access.NewNextcloudHistoryGuard(nil, nil, nil)
			}
			got, _, err := LoadAgentHistory(context.Background(), repo, "s1", 100, false, guard)
			require.Error(t, err)
			require.Empty(t, got)
			require.Zero(t, repo.pages)
			require.Zero(t, repo.checkpointReads)
		})
	}
}

func TestAgentHistoryAdmissionAllowsOrdinaryCheckpointAndRechecks(t *testing.T) {
	rows := storedTurns(3)
	for _, changed := range []bool{false, true} {
		repo := &historyRepo{rows: rows, checkpoint: checkpointOn(rows[1], "ordinary checkpoint")}
		sources := &instanceHistorySources{}
		if changed {
			sources.becomeSourceAt = 2
		}
		got, _, err := LoadAgentHistory(
			context.Background(),
			repo,
			"s1",
			unlimitedBudget,
			false,
			access.NewNextcloudHistoryGuard(
				nil,
				sources,
				nil,
			))
		require.Equal(t, 2, sources.calls)
		if changed {
			require.ErrorIs(t, err, access.ErrNextcloudPublicationDenied)
			require.Empty(t, got)
		} else {
			require.NoError(t, err)
			require.Contains(t, got[0].Content, "ordinary checkpoint")
		}
	}
}
