package interfaces

import (
	"context"

	"github.com/Tencent/WeKnora/internal/types"
)

// KnowledgeCollectionInputReader captures and rechecks knowledge collection selection inputs.
type KnowledgeCollectionInputReader interface {
	CaptureKnowledgeCollectionInputs(
		context.Context,
		types.KnowledgeCollectionReadRequest,
	) (*types.KnowledgeCollectionInputSnapshot, error)
	SelectKnowledgeCollectionInputs(
		context.Context,
		*types.KnowledgeCollectionInputSnapshot,
		types.KnowledgeCollectionSelectionRequest,
	) (*types.KnowledgeCollectionSelection, error)
	RecheckKnowledgeCollectionInputs(
		context.Context,
		*types.KnowledgeCollectionInputSnapshot,
		*types.KnowledgeCollectionSelection,
	) error
}
