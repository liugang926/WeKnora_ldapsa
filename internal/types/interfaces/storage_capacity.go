package interfaces

import (
	"context"
	"github.com/Tencent/WeKnora/internal/types"
)

// Implemented only by providers that can measure their actual quota/filesystem.
type StorageCapacityProvider interface {
	MeasureStorageCapacity(context.Context) (types.StorageCapacity, error)
}
