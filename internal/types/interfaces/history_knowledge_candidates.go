package interfaces

import (
	"context"

	"github.com/Tencent/WeKnora/internal/types"
)

// HistoryKnowledgeCandidateControlReader reads exact producer identities and
// owned-session controls before native candidate retrieval or parent bodies.
type HistoryKnowledgeCandidateControlReader interface {
	OwnedHistoryKnowledgeIDs(context.Context, uint64, string, string, []string) ([]string, error)
	PeekOwnedHistoryKnowledgeParentPlans(
		context.Context,
		uint64,
		string,
		string,
		[]string,
		[]string,
	) ([]*types.HistoryKnowledgeParentPlan, error)
}
