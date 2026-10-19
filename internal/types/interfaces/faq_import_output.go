package interfaces

import (
	"context"

	"github.com/Tencent/WeKnora/internal/types"
)

type FAQImportOutputInputReader interface {
	CaptureFAQImportOutputInputs(
		context.Context,
		types.FAQImportOutputReadRequest,
	) (*types.FAQImportOutputSnapshot, error)
	RecheckFAQImportOutputInputs(context.Context, *types.FAQImportOutputSnapshot) error
}
