//go:build !linux && !darwin

package file

import (
	"context"
	"errors"
	"mime/multipart"

	"github.com/Tencent/WeKnora/internal/types"
)

func (s *localFileService) SupportsLocalCapacityIntents() bool { return false }
func (s *localFileService) saveCapacityFile(context.Context, *multipart.FileHeader, uint64, string) (string, error) {
	return "", errors.New("durable capacity writers unavailable on this platform")
}

func (s *localFileService) InspectCapacityIntent(
	context.Context,
	types.CapacityLocalObject,
	func(types.CapacityLocalObject) error,
) error {
	return errors.New("durable capacity reconciliation unavailable on this platform")
}
