package interfaces

import (
	"context"

	"github.com/Tencent/WeKnora/internal/types"
)

type TagOutputInputReader interface {
	CaptureTagOutputInputs(context.Context, types.TagOutputReadRequest) (*types.TagOutputInputSnapshot, error)
	SelectTagOutputInputs(
		context.Context,
		*types.TagOutputInputSnapshot,
		types.TagOutputSelectionRequest,
	) (*types.TagOutputSelection, error)
	RecheckTagOutputInputs(context.Context, *types.TagOutputInputSnapshot, *types.TagOutputSelection) error
}
