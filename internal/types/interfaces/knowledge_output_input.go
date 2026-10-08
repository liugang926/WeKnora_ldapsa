package interfaces

import (
	"context"

	"github.com/Tencent/WeKnora/internal/types"
)

// KnowledgeOutputInputReader captures fresh server-resolved material using its
// persisted original receipt. It cannot certify a supplied body or revive an
// old task. Replays and workers must retain their own original input proof.
type KnowledgeOutputInputReader interface {
	CaptureKnowledgeOutputInputs(context.Context, types.KnowledgeBuildInputCaptureRequest) (*types.KnowledgeBuildInputSnapshot, error)
	RecheckKnowledgeBuildInputs(context.Context, *types.KnowledgeBuildInputProof) error
	RecheckKnowledgeOutputInputs(context.Context, []*types.KnowledgeBuildInputProof) error
}

type KnowledgeOutputAuxiliaryInputReader interface {
	CaptureKnowledgeOutputAuxiliary(context.Context, []*types.KnowledgeBuildInputProof, []types.KnowledgeOutputDisplaySelection) (*types.KnowledgeOutputAuxiliaryInputSnapshot, error)
	RecheckKnowledgeOutputAuxiliary(context.Context, *types.KnowledgeOutputAuxiliaryInputSnapshot) error
}
