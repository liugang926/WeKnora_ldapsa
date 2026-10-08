//go:build linux || darwin

package file

import (
	"context"
	"errors"
	"fmt"
	"github.com/Tencent/WeKnora/internal/types"
	"golang.org/x/sys/unix"
	"strings"
)

func (s *localFileService) DeleteNextcloudObject(ctx context.Context, path string) (bool, error) {
	if e := ctx.Err(); e != nil {
		return false, e
	}
	if !strings.HasPrefix(path, "local://") {
		return false, errors.New("GC requires exact local provider path")
	}
	parts := strings.Split(strings.TrimPrefix(path, "local://"), "/")
	if len(parts) < 3 {
		return false, errors.New("GC local path is incomplete")
	}
	for _, part := range parts {
		if part == "" || part == "." || part == ".." || strings.ContainsAny(part, `\`) {
			return false, errors.New("GC local path is invalid")
		}
	}
	root, _, _, e := s.capacityRoot()
	if e != nil {
		return false, e
	}
	defer unix.Close(root)
	filename := parts[len(parts)-1]
	if strings.HasPrefix(filename, "capacity-") {
		if len(filename) < len("capacity-")+36 {
			return false, errors.New("GC capacity identity unavailable")
		}
		token := filename[len("capacity-") : len("capacity-")+36]
		lock, e := capacityWriterLock(root, token)
		if e != nil {
			return false, e
		}
		defer lock.Close()
	}
	current := root
	opened := []int{}
	defer func() {
		for _, fd := range opened {
			unix.Close(fd)
		}
	}()
	for _, part := range parts[:len(parts)-1] {
		next, e := capacityDirectory(current, part, false)
		if errors.Is(e, unix.ENOENT) {
			return false, nil
		}
		if e != nil {
			return false, e
		}
		opened = append(opened, next)
		current = next
	}
	var before unix.Stat_t
	if e = unix.Fstatat(current, filename, &before, unix.AT_SYMLINK_NOFOLLOW); errors.Is(e, unix.ENOENT) {
		return false, nil
	}
	if e != nil {
		return false, e
	}
	if before.Mode&unix.S_IFMT != unix.S_IFREG || before.Nlink != 1 {
		return false, errors.New("GC linked/nonregular object unavailable")
	}
	fd, e := unix.Openat(current, filename, unix.O_RDONLY|unix.O_NOFOLLOW, 0)
	if e != nil {
		return false, e
	}
	defer unix.Close(fd)
	var actual unix.Stat_t
	if e = unix.Fstat(fd, &actual); e != nil {
		return false, e
	}
	if actual.Dev != before.Dev || actual.Ino != before.Ino || actual.Nlink != 1 {
		return false, errors.New("GC object changed")
	}
	if e = ctx.Err(); e != nil {
		return false, e
	}
	if e = unix.Unlinkat(current, filename, 0); e != nil {
		return false, e
	}
	if e = unix.Fsync(current); e != nil {
		return false, fmt.Errorf("GC unlink sync acknowledgement unavailable: %w", e)
	}
	return true, nil
}
func (s *backendScopedFileService) DeleteNextcloudObject(ctx context.Context, path string) (bool, error) {
	id, inner, ok := types.ParseStorageBackendPath(path)
	if !ok || id != s.backendID {
		return false, errors.New("GC backend scope changed")
	}
	return deleteWrappedGC(ctx, s.inner, inner)
}
func (s *resourceCatalogFileService) DeleteNextcloudObject(ctx context.Context, path string) (bool, error) {
	return deleteWrappedGC(ctx, s.inner, path)
}
