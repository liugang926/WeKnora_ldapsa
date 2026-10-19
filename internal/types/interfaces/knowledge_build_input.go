package interfaces

import (
	"context"

	"github.com/Tencent/WeKnora/internal/types"
)

// KnowledgeBuildInputReader captures actual rows and their original admitted
// producer context. Recheck verifies the original proof at every model/write/
// transaction-output boundary; it never mints a replacement for old text.
type KnowledgeBuildInputReader interface {
	CaptureKnowledgeBuildInputs(
		context.Context,
		types.KnowledgeBuildInputCaptureRequest,
	) (*types.KnowledgeBuildInputSnapshot, error)
	RecheckKnowledgeBuildInputs(context.Context, *types.KnowledgeBuildInputProof) error
}

// KnowledgeInputWriter is an internal admitted producer boundary. The input
// artifact is the actual sanitized text or upload bytes, never client proof.
type KnowledgeInputWriter interface {
	CreateKnowledgeWithInput(context.Context, *types.Knowledge, any) (*types.KnowledgeBuildInputProof, error)
	UpdateKnowledgeWithInput(context.Context, *types.Knowledge, any) (*types.KnowledgeBuildInputProof, error)
}

type KnowledgeBaseInputWriter interface {
	CreateKnowledgeBaseWithInput(context.Context, *types.KnowledgeBase) error
	UpdateKnowledgeBaseWithInput(context.Context, *types.KnowledgeBase) error
}
