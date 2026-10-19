//go:build !linux && !darwin

package file

import (
	"errors"
	"os"
)

func (s *localFileService) physicalPathFence(string) (*os.File, error) {
	return nil, errors.New("physical path fencing unavailable on this platform")
}

func (s *localFileService) ensurePhysicalDirectory(string) error {
	return errors.New("physical path fencing unavailable on this platform")
}

func (s *localFileService) createPhysicalFile(string) (*os.File, error) {
	return nil, errors.New("physical path fencing unavailable on this platform")
}
