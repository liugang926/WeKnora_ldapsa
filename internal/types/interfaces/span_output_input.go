package interfaces

import (
	"context"

	"github.com/Tencent/WeKnora/internal/types"
)

// SpanOutputInputReader captures and rechecks original span output inputs.
type SpanOutputInputReader interface {
	CaptureSpanOutputInputs(context.Context, types.SpanOutputReadRequest) (*types.SpanOutputInputSnapshot, error)
	RecheckSpanOutputInputs(context.Context, *types.SpanOutputInputSnapshot) error
}
