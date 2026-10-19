package interfaces

import (
	"context"

	"github.com/Tencent/WeKnora/internal/types"
)

// KnowledgeHistoryMaterialRequestReader resolves original history parents
// from an immutable admission. A nil request positively identifies an ordinary
// producer; current knowledge JSON or caller-declared channel is not used.
type KnowledgeHistoryMaterialRequestReader interface {
	KnowledgeHistoryMaterialRequest(context.Context, *types.KnowledgeBuildInputProof) (*types.MessageMaterialReadRequest, error)
}

// KnowledgeHistoryTaskMaterialRestorer obtains actual parent guards for a
// worker. The closer releases only guards acquired by this invocation; caller
// or sibling guards are borrowed and remain owned by their original lifetime.
type KnowledgeHistoryTaskMaterialRestorer interface {
	BeginKnowledgeHistoryTaskMaterial(context.Context, *types.KnowledgeBuildInputProof) (context.Context, func(), error)
}
