package interfaces

import (
	"context"

	"github.com/Tencent/WeKnora/internal/types"
)

// ChunkOutputInputReader captures and rechecks original chunk output inputs.
type ChunkOutputInputReader interface {
	CaptureChunkOutputInputs(context.Context, types.ChunkOutputReadRequest) (*types.ChunkOutputInputSnapshot, error)
	RecheckChunkOutputInputs(context.Context, *types.ChunkOutputInputSnapshot) error
}
