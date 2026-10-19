//go:build linux || darwin

package file

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"

	"golang.org/x/sys/unix"
)

type localGCMoveReceipt struct {
	Path          string
	Device, Inode uint64
}

func gcResumeHolding(ctx context.Context, root int, path string) (bool, bool, error) {
	holding, e := gcQuarantineDirectory(root)
	if e != nil {
		return false, false, e
	}
	defer func() { _ = unix.Close(holding) }() // Directory durability is checked with Fsync before returning.
	receiptName, objectName := gcHoldingNames(path)
	receipt, e := gcReadMoveReceipt(holding, receiptName)
	if errors.Is(e, unix.ENOENT) {
		return false, false, nil
	}
	if e != nil {
		return false, false, e
	}
	if receipt.Path != path {
		return false, false, errors.New("GC holding receipt scope mismatch")
	}
	var held unix.Stat_t
	if e = unix.Fstatat(holding, objectName, &held, unix.AT_SYMLINK_NOFOLLOW); errors.Is(e, unix.ENOENT) {
		return false, false, nil
	}
	if e != nil {
		return false, false, e
	}
	if uint64(held.Dev) != receipt.Device || uint64(held.Ino) != receipt.Inode || held.Nlink != 1 ||
		held.Mode&unix.S_IFMT != unix.S_IFREG {
		return false, false, errors.New("GC holding inode changed")
	}
	if e = ctx.Err(); e != nil {
		return false, false, e
	}
	if e = unix.Unlinkat(holding, objectName, 0); e != nil {
		return false, false, e
	}
	if e = unix.Fsync(holding); e != nil {
		return false, false, e
	}
	if e = unix.Unlinkat(holding, receiptName, 0); e != nil {
		return false, false, e
	}
	if e = unix.Fsync(holding); e != nil {
		return false, false, e
	}
	return true, true, nil
}

func gcQuarantineDirectory(root int) (int, error) {
	fd, e := capacityDirectory(root, ".gc-exact-holding", true)
	if e != nil {
		return -1, e
	}
	var st unix.Stat_t
	if e = unix.Fstat(fd, &st); e != nil || int(st.Uid) != os.Geteuid() || st.Mode&0o777 != 0o700 {
		_ = unix.Close(fd) // Rejected directory descriptor; no pending writes.
		return -1, errors.New("GC holding directory not private")
	}
	return fd, nil
}

func gcHoldingNames(path string) (string, string) {
	hash := sha256.Sum256([]byte(path))
	name := hex.EncodeToString(hash[:])
	return name + ".receipt", name + ".object"
}

func gcReadMoveReceipt(dir int, name string) (localGCMoveReceipt, error) {
	var r localGCMoveReceipt
	fd, e := unix.Openat(dir, name, unix.O_RDONLY|unix.O_NOFOLLOW, 0)
	if e != nil {
		return r, e
	}
	f := os.NewFile(uintptr(fd), name)
	defer func() { _ = f.Close() }() // Read-only receipt, authenticated before use.
	var st unix.Stat_t
	if e = unix.Fstat(
		fd,
		&st,
	); e != nil || st.Nlink != 1 || st.Mode&unix.S_IFMT != unix.S_IFREG || st.Mode&0o777 != 0o600 || int(st.Uid) != os.Geteuid() ||
		st.Size > 4096 {
		return r, errors.New("GC holding receipt unavailable")
	}
	b := make([]byte, st.Size)
	n, e := f.Read(b)
	if e != nil || int64(n) != st.Size {
		return r, errors.New("GC holding receipt incomplete")
	}
	e = json.Unmarshal(b, &r)
	return r, e
}

func gcPersistMoveReceipt(dir int, name string, r localGCMoveReceipt) (resultErr error) {
	fd, e := unix.Openat(dir, name, unix.O_WRONLY|unix.O_CREAT|unix.O_EXCL|unix.O_NOFOLLOW, 0o600)
	if e != nil {
		return e
	}
	f := os.NewFile(uintptr(fd), name)
	closed := false
	defer func() {
		if !closed {
			resultErr = errors.Join(resultErr, f.Close())
		}
	}()
	b, e := json.Marshal(r)
	if e != nil {
		return e
	}
	if _, e = f.Write(b); e != nil {
		return e
	}
	if e = f.Sync(); e != nil {
		return e
	}
	e = f.Close()
	closed = true
	if e != nil {
		return e
	}
	return unix.Fsync(dir)
}

// A rename isolates the pathname atomically before any unlink. Only the exact
// pinned inode in the owned private holding directory can be destroyed. A
// replacement is restored without overwrite or retained there for review.
func gcQuarantineUnlink(
	ctx context.Context,
	root, parent int,
	filename, path string,
	before unix.Stat_t,
) (bool, error) {
	holding, e := gcQuarantineDirectory(root)
	if e != nil {
		return false, e
	}
	defer func() { _ = unix.Close(holding) }() // Directory mutations are synchronized explicitly.
	receiptName, objectName := gcHoldingNames(path)
	receipt, e := gcReadMoveReceipt(holding, receiptName)
	if errors.Is(e, unix.ENOENT) {
		receipt = localGCMoveReceipt{path, uint64(before.Dev), uint64(before.Ino)}
		if e = gcPersistMoveReceipt(holding, receiptName, receipt); e != nil {
			return false, e
		}
	} else if e != nil {
		return false, e
	}
	if receipt.Path != path || receipt.Device == 0 || receipt.Inode == 0 {
		return false, errors.New("GC holding receipt scope mismatch")
	}
	var held unix.Stat_t
	if e = unix.Fstatat(holding, objectName, &held, unix.AT_SYMLINK_NOFOLLOW); errors.Is(e, unix.ENOENT) {
		if e = gcRenameExclusive(parent, filename, holding, objectName); e != nil {
			return false, e
		}
		if e = unix.Fsync(parent); e != nil {
			return false, e
		}
		if e = unix.Fsync(holding); e != nil {
			return false, e
		}
		if e = unix.Fstatat(holding, objectName, &held, unix.AT_SYMLINK_NOFOLLOW); e != nil {
			return false, e
		}
	} else if e != nil {
		return false, e
	}
	if uint64(held.Dev) != receipt.Device || uint64(held.Ino) != receipt.Inode || held.Nlink != 1 ||
		held.Mode&unix.S_IFMT != unix.S_IFREG {
		_ = gcRenameExclusive(holding, objectName, parent, filename)
		return false, errors.New("GC same-path replacement refused; bytes retained")
	}
	if e = ctx.Err(); e != nil {
		return false, e
	}
	if e = unix.Unlinkat(holding, objectName, 0); e != nil {
		return false, e
	}
	if e = unix.Fsync(holding); e != nil {
		return false, e
	}
	if e = unix.Unlinkat(holding, receiptName, 0); e != nil {
		return false, e
	}
	if e = unix.Fsync(holding); e != nil {
		return false, e
	}
	return true, nil
}
