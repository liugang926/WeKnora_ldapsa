package file

import (
	"context"
	"errors"

	"github.com/Tencent/WeKnora/internal/types/interfaces"
)

func deleteWrappedGC(ctx context.Context, inner interfaces.FileService, path string) (bool, error) {
	provider, ok := inner.(interfaces.NextcloudPhysicalGCDeleter)
	if !ok {
		return false, errors.New("secure local GC provider unavailable")
	}
	return provider.DeleteNextcloudObject(ctx, path)
}
