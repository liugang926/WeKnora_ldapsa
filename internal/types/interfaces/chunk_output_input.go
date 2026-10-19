package interfaces

import (
	"context"

	"github.com/Tencent/WeKnora/internal/types"
)

type ChunkOutputInputReader interface {
	CaptureChunkOutputInputs(context.Context, types.ChunkOutputReadRequest) (*types.ChunkOutputInputSnapshot, error)
	RecheckChunkOutputInputs(context.Context, *types.ChunkOutputInputSnapshot) error
}
