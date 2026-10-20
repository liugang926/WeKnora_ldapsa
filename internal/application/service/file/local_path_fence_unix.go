//go:build linux || darwin

package file

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"strings"

	"golang.org/x/sys/unix"
)

// All application local writes/deletes take this fence through descriptor
// close. It also covers legacy timestamp objects and ordinary non-capacity IO.
func (s *localFileService) physicalPathFence(path string) (*os.File, error) {
	candidate := s.normalizePathForBase(path)
	relative, e := filepath.Rel(s.baseDir, candidate)
	if e != nil || relative == "." || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return nil, errors.New("physical path fence scope invalid")
	}
	root, _, _, e := s.capacityRoot()
	if e != nil {
		return nil, e
	}
	defer func() { _ = unix.Close(root) }() // Read-only directory descriptor.
	dir, e := capacityDirectory(root, ".physical-path-fences", true)
	if e != nil {
		return nil, e
	}
	defer func() { _ = unix.Close(dir) }() // Read-only directory descriptor, not an acquired fence.
	var ds unix.Stat_t
	if e = unix.Fstat(dir, &ds); e != nil || int(ds.Uid) != os.Geteuid() || ds.Mode&0o777 != 0o700 {
		return nil, errors.New("physical path fence directory unavailable")
	}
	hash := sha256.Sum256([]byte(filepath.ToSlash(relative)))
	name := hex.EncodeToString(hash[:]) + ".lock"
	fd, e := unix.Openat(dir, name, unix.O_CREAT|unix.O_RDWR|unix.O_NOFOLLOW, 0o600)
	if e != nil {
		return nil, e
	}
	var st unix.Stat_t
	if e = unix.Fstat(
		fd,
		&st,
	); e != nil || int(st.Uid) != os.Geteuid() || st.Mode&0o777 != 0o600 || st.Mode&unix.S_IFMT != unix.S_IFREG ||
		st.Nlink != 1 {
		_ = unix.Close(fd) // Rejected, unwritten lock descriptor.
		return nil, errors.New("physical path fence file unavailable")
	}
	if e = unix.Flock(fd, unix.LOCK_EX|unix.LOCK_NB); e != nil {
		_ = unix.Close(fd) // Unacquired, unwritten lock descriptor.
		return nil, e
	}
	return os.NewFile(uintptr(fd), name), nil
}

// Walk/create directories using pinned descriptors. No symlinked descendant is
// followed, including between the validation and the actual file open.
func (s *localFileService) physicalParent(path string, create bool) (int, string, error) {
	candidate := s.normalizePathForBase(path)
	rootAbs, e := filepath.Abs(s.baseDir)
	if e != nil {
		return -1, "", e
	}
	relative, e := filepath.Rel(rootAbs, candidate)
	if e != nil || relative == "." || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return -1, "", errors.New("physical path invalid")
	}
	parts := strings.Split(filepath.ToSlash(relative), "/")
	for _, p := range parts {
		if p == "" || p == "." || p == ".." || strings.ContainsAny(p, `\`) {
			return -1, "", errors.New("physical component invalid")
		}
	}
	current, _, _, e := s.capacityRoot()
	if e != nil {
		return -1, "", e
	}
	for _, p := range parts[:len(parts)-1] {
		next, e := capacityDirectory(current, p, create)
		_ = unix.Close(current) // Read-only traversal descriptor; no pending writes.
		if e != nil {
			return -1, "", e
		}
		current = next
	}
	return current, parts[len(parts)-1], nil
}

func (s *localFileService) ensurePhysicalDirectory(path string) error {
	parent, _, e := s.physicalParent(filepath.Join(path, ".directory-witness"), true)
	if e != nil {
		return e
	}
	return unix.Close(parent)
}

func (s *localFileService) createPhysicalFile(path string) (*os.File, error) {
	parent, name, e := s.physicalParent(path, false)
	if e != nil {
		return nil, e
	}
	defer func() { _ = unix.Close(parent) }() // Read-only parent descriptor.
	fd, e := unix.Openat(parent, name, unix.O_WRONLY|unix.O_CREAT|unix.O_EXCL|unix.O_NOFOLLOW, 0o600)
	if e != nil {
		return nil, e
	}
	return os.NewFile(uintptr(fd), path), nil
}
