package interfaces

import (
	"context"

	"github.com/Tencent/WeKnora/internal/types"
)

// CommittedHistoryKnowledgeInputWriter persists inputs of knowledge derived from committed history.
type CommittedHistoryKnowledgeInputWriter interface {
	CreateCommittedHistoryKnowledgeInput(
		context.Context,
		types.CommittedHistoryKnowledgeInputRequest,
	) (*types.CommittedHistoryKnowledgeInput, error)
}
