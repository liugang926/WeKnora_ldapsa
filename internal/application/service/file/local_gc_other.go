//go:build !linux && !darwin

package file

import (
	"context"
	"errors"
)

func (s *localFileService) DeleteNextcloudObject(context.Context, string) (bool, error) {
	return false, errors.New("secure local GC unavailable on this platform")
}

func (s *backendScopedFileService) DeleteNextcloudObject(context.Context, string) (bool, error) {
	return false, errors.New("secure local GC unavailable on this platform")
}

func (s *resourceCatalogFileService) DeleteNextcloudObject(context.Context, string) (bool, error) {
	return false, errors.New("secure local GC unavailable on this platform")
}
