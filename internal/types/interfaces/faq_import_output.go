package interfaces

import (
	"context"

	"github.com/Tencent/WeKnora/internal/types"
)

// FAQImportOutputInputReader captures and rechecks original FAQ import progress inputs.
type FAQImportOutputInputReader interface {
	CaptureFAQImportOutputInputs(
		context.Context,
		types.FAQImportOutputReadRequest,
	) (*types.FAQImportOutputSnapshot, error)
	RecheckFAQImportOutputInputs(context.Context, *types.FAQImportOutputSnapshot) error
}
