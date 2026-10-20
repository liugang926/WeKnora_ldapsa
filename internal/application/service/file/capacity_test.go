package file

import (
	"context"
	"testing"

	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/stretchr/testify/require"
)

func TestLocalCapacityActualFilesystemAndDecorators(t *testing.T) {
	inner := NewLocalFileService(t.TempDir(), "")
	provider := inner.(interfaces.StorageCapacityProvider)
	measured, err := provider.MeasureStorageCapacity(context.Background())
	require.NoError(t, err)
	require.Positive(t, measured.TotalBytes)
	require.LessOrEqual(t, measured.AvailableBytes, measured.TotalBytes)
	require.Len(t, measured.ScopeID, 64)
	wrapped := NewBackendScopedFileService("actual-backend", inner).(interfaces.StorageCapacityProvider)
	again, err := wrapped.MeasureStorageCapacity(context.Background())
	require.NoError(t, err)
	require.Equal(t, measured.ScopeID, again.ScopeID)
}
