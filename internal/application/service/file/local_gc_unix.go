//go:build linux || darwin

package file

import (
	"context"
	"errors"
	"strings"

	"github.com/Tencent/WeKnora/internal/types"
	"golang.org/x/sys/unix"
)

func (s *localFileService) DeleteNextcloudObject(ctx context.Context, path string) (unlinked bool, resultErr error) {
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
	defer func() { _ = unix.Close(root) }() // Read-only directory; GC synchronizes mutations explicitly.
	pathLock, e := s.physicalPathFence(path)
	if e != nil {
		return false, e
	}
	defer func() { resultErr = errors.Join(resultErr, pathLock.Close()) }()
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
		defer func() { resultErr = errors.Join(resultErr, lock.Close()) }()
	}
	if resumed, unlinked, e := gcResumeHolding(ctx, root, path); e != nil || resumed {
		return unlinked, e
	}
	current := root
	opened := []int{}
	defer func() {
		for _, fd := range opened {
			_ = unix.Close(fd) // Read-only directory descriptors; no pending writes.
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
	defer func() { _ = unix.Close(fd) }() // Read-only pinned object; all mutation durability is explicit.
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
	return gcQuarantineUnlink(ctx, root, current, filename, path, actual)
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
