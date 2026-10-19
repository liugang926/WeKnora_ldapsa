package interfaces

import (
	"context"

	"github.com/Tencent/WeKnora/internal/types"
)

// TagOutputInputReader captures and rechecks original tag output inputs.
type TagOutputInputReader interface {
	CaptureTagOutputInputs(context.Context, types.TagOutputReadRequest) (*types.TagOutputInputSnapshot, error)
	SelectTagOutputInputs(
		context.Context,
		*types.TagOutputInputSnapshot,
		types.TagOutputSelectionRequest,
	) (*types.TagOutputSelection, error)
	RecheckTagOutputInputs(context.Context, *types.TagOutputInputSnapshot, *types.TagOutputSelection) error
}
