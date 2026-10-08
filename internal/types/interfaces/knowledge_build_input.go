package interfaces

import (
	"context"
	"github.com/Tencent/WeKnora/internal/types"
)

// KnowledgeBuildInputReader captures actual rows and their original admitted
// producer context. Recheck verifies the original proof at every model/write/
// transaction-output boundary; it never mints a replacement for old text.
type KnowledgeBuildInputReader interface {
	CaptureKnowledgeBuildInputs(context.Context, types.KnowledgeBuildInputCaptureRequest) (*types.KnowledgeBuildInputSnapshot, error)
	RecheckKnowledgeBuildInputs(context.Context, *types.KnowledgeBuildInputProof) error
}
