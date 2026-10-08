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
