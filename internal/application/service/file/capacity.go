package file

import (
	"context"
	"errors"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
)

func measureWrappedCapacity(ctx context.Context, inner interfaces.FileService) (types.StorageCapacity, error) {
	provider, ok := inner.(interfaces.StorageCapacityProvider)
	if !ok {
		return types.StorageCapacity{}, errors.New("storage provider has no authoritative capacity measurement")
	}
	return provider.MeasureStorageCapacity(ctx)
}

func (s *backendScopedFileService) MeasureStorageCapacity(ctx context.Context) (types.StorageCapacity, error) {
	return measureWrappedCapacity(ctx, s.inner)
}

func (s *resourceCatalogFileService) MeasureStorageCapacity(ctx context.Context) (types.StorageCapacity, error) {
	return measureWrappedCapacity(ctx, s.inner)
}

func supportsWrappedIntents(inner interfaces.FileService) bool {
	provider, ok := inner.(interfaces.LocalCapacityIntentProvider)
	return ok && provider.SupportsLocalCapacityIntents()
}

func inspectWrappedIntent(
	ctx context.Context,
	inner interfaces.FileService,
	o types.CapacityLocalObject,
	callback func(types.CapacityLocalObject) error,
) error {
	provider, ok := inner.(interfaces.LocalCapacityIntentProvider)
	if !ok || !provider.SupportsLocalCapacityIntents() {
		return errors.New("local capacity reconciliation unavailable")
	}
	return provider.InspectCapacityIntent(ctx, o, callback)
}

func (s *backendScopedFileService) SupportsLocalCapacityIntents() bool {
	return supportsWrappedIntents(s.inner)
}

func (s *backendScopedFileService) InspectCapacityIntent(
	ctx context.Context,
	o types.CapacityLocalObject,
	callback func(types.CapacityLocalObject) error,
) error {
	if o.BackendID != s.backendID {
		return errors.New("capacity backend mismatch")
	}
	return inspectWrappedIntent(ctx, s.inner, o, callback)
}

func (s *resourceCatalogFileService) SupportsLocalCapacityIntents() bool {
	return supportsWrappedIntents(s.inner)
}

func (s *resourceCatalogFileService) InspectCapacityIntent(
	ctx context.Context,
	o types.CapacityLocalObject,
	callback func(types.CapacityLocalObject) error,
) error {
	return inspectWrappedIntent(ctx, s.inner, o, callback)
}
