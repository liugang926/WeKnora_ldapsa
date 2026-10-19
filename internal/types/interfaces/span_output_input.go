package interfaces

import (
	"context"

	"github.com/Tencent/WeKnora/internal/types"
)

type SpanOutputInputReader interface {
	CaptureSpanOutputInputs(context.Context, types.SpanOutputReadRequest) (*types.SpanOutputInputSnapshot, error)
	RecheckSpanOutputInputs(context.Context, *types.SpanOutputInputSnapshot) error
}
