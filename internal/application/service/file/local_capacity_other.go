//go:build !linux && !darwin

package file

import (
	"context"
	"errors"

	"github.com/Tencent/WeKnora/internal/types"
)

func (s *localFileService) MeasureStorageCapacity(context.Context) (types.StorageCapacity, error) {
	return types.StorageCapacity{}, errors.New("local provider capacity is unavailable on this platform")
}
