package interfaces

import (
	"context"

	"github.com/Tencent/WeKnora/internal/types"
)

type CommittedHistoryKnowledgeInputWriter interface {
	CreateCommittedHistoryKnowledgeInput(
		context.Context,
		types.CommittedHistoryKnowledgeInputRequest,
	) (*types.CommittedHistoryKnowledgeInput, error)
}
