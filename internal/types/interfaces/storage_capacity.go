package interfaces

import (
	"context"
	"github.com/Tencent/WeKnora/internal/types"
)

// Implemented only by providers that can measure their actual quota/filesystem.
type StorageCapacityProvider interface {
	MeasureStorageCapacity(context.Context) (types.StorageCapacity, error)
}

// The provider holds the same exclusive process-wide writer fence while the
// callback validates catalog/state and records a conservative charge/release.
type LocalCapacityIntentProvider interface {
	SupportsLocalCapacityIntents() bool
	InspectCapacityIntent(context.Context, types.CapacityLocalObject, func(types.CapacityLocalObject) error) error
}
