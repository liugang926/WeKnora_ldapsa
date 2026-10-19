//go:build linux || darwin

package file

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	"github.com/Tencent/WeKnora/internal/types"
	"golang.org/x/sys/unix"
)

var (
	capacitySegment = regexp.MustCompile(`^[A-Za-z0-9_-]{1,128}$`)
	capacityToken   = regexp.MustCompile(`^[a-f0-9]{8}-(?:[a-f0-9]{4}-){3}[a-f0-9]{12}$`)
)

func (s *localFileService) SupportsLocalCapacityIntents() bool { return true }
func (s *localFileService) capacityRoot() (int, string, string, error) {
	root, e := filepath.Abs(s.baseDir)
	if e != nil {
		return -1, "", "", e
	}
	actual, e := filepath.EvalSymlinks(root)
	if e != nil || actual != root {
		return -1, "", "", errors.New("capacity storage root is linked or unavailable")
	}
	fd, e := unix.Open(root, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW, 0)
	if e != nil {
		return -1, "", "", e
	}
	var st unix.Stat_t
	if e = unix.Fstat(fd, &st); e != nil {
		_ = unix.Close(fd) // Read-only directory cleanup; no buffered writes.
		return -1, "", "", e
	}
	rootSHA := sha256.Sum256(
		[]byte(root + "\n" + strconv.FormatUint(uint64(st.Dev), 10) + "\n" + strconv.FormatUint(uint64(st.Ino), 10)),
	)
	scopeSHA := sha256.Sum256([]byte("local-filesystem:" + strconv.FormatUint(uint64(st.Dev), 10)))
	return fd, hex.EncodeToString(rootSHA[:]), hex.EncodeToString(scopeSHA[:]), nil
}

func capacityDirectory(parent int, name string, create bool) (int, error) {
	if create {
		if e := unix.Mkdirat(parent, name, 0o700); e != nil && !errors.Is(e, unix.EEXIST) {
			return -1, e
		}
	}
	return unix.Openat(parent, name, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW, 0)
}

func capacityWriterLock(root int, token string) (*os.File, error) {
	if !capacityToken.MatchString(token) {
		return nil, errors.New("invalid capacity token")
	}
	locks, e := capacityDirectory(root, ".capacity-intents", true)
	if e != nil {
		return nil, e
	}
	defer func() { _ = unix.Close(locks) }() // Directory descriptor; durability uses explicit Fsync.
	var directory unix.Stat_t
	if e = unix.Fstat(
		locks,
		&directory,
	); e != nil || int(directory.Uid) != os.Geteuid() ||
		directory.Mode&0o777 != 0o700 {
		return nil, errors.New("capacity lock directory is not private and owned")
	}
	fd, e := unix.Openat(locks, token+".lock", unix.O_RDWR|unix.O_CREAT|unix.O_NOFOLLOW, 0o600)
	if e != nil {
		return nil, e
	}
	var st unix.Stat_t
	if e = unix.Fstat(
		fd,
		&st,
	); e != nil || st.Nlink != 1 || st.Mode&unix.S_IFMT != unix.S_IFREG || int(st.Uid) != os.Geteuid() ||
		st.Mode&0o777 != 0o600 {
		_ = unix.Close(fd) // Rejected lock descriptor has no pending writes.
		return nil, errors.New("unsafe capacity writer lock")
	}
	if e = unix.Flock(fd, unix.LOCK_EX|unix.LOCK_NB); e != nil {
		_ = unix.Close(fd) // Unacquired lock descriptor has no pending writes.
		return nil, fmt.Errorf("capacity writer still active: %w", e)
	}
	return os.NewFile(uintptr(fd), "capacity writer fence"), nil
}

func capacityParts(path, backend, knowledge, token string) ([]string, error) {
	id, inner, ok := types.ParseStorageBackendPath(path)
	if !ok || id != backend || !strings.HasPrefix(inner, "local://") {
		return nil, errors.New("capacity provider/backend mismatch")
	}
	parts := strings.Split(strings.TrimPrefix(inner, "local://"), "/")
	if len(parts) != 3 || !capacitySegment.MatchString(parts[0]) || !capacitySegment.MatchString(parts[1]) ||
		parts[1] != knowledge ||
		!strings.HasPrefix(parts[2], "capacity-"+token) ||
		filepath.Base(parts[2]) != parts[2] ||
		strings.ContainsAny(parts[2], `\`) {
		return nil, errors.New("capacity object scope mismatch")
	}
	return parts, nil
}

func (s *localFileService) saveCapacityFile(
	ctx context.Context,
	header *multipart.FileHeader,
	tenant uint64,
	knowledge string,
) (storedPath string, resultErr error) {
	session := types.CapacityWriteSessionFromContext(ctx)
	if session == nil || tenant == 0 || !capacitySegment.MatchString(knowledge) || header == nil || header.Size <= 0 {
		return "", errors.New("invalid capacity file scope")
	}
	ext := filepath.Ext(header.Filename)
	if strings.ContainsAny(ext, `/\`) || len(ext) > 32 {
		return "", errors.New("invalid capacity file extension")
	}
	root, identity, scope, e := s.capacityRoot()
	if e != nil {
		return "", e
	}
	defer func() { _ = unix.Close(root) }() // Read-only directory; explicit Fsync below is checked.
	lock, e := capacityWriterLock(root, session.Token())
	if e != nil {
		return "", e
	}
	if e = session.RetainWriterLock(lock); e != nil {
		return "", errors.Join(e, lock.Close())
	}
	src, e := header.Open()
	if e != nil {
		return "", e
	}
	defer func() { _ = src.Close() }() // Read-only upload source.
	filename := "capacity-" + session.Token() + ext
	inner := fmt.Sprintf("local://%d/%s/%s", tenant, knowledge, filename)
	physical := types.BuildStorageBackendPath(session.Backend(), inner)
	if e = session.PrepareLocal(ctx, knowledge, physical, identity, scope, header.Size); e != nil {
		return "", e
	}
	tenantFD, e := capacityDirectory(root, strconv.FormatUint(tenant, 10), true)
	if e != nil {
		return "", e
	}
	defer func() { _ = unix.Close(tenantFD) }() // Directory durability is explicitly synchronized below.
	knowledgeFD, e := capacityDirectory(tenantFD, knowledge, true)
	if e != nil {
		return "", e
	}
	defer func() { _ = unix.Close(knowledgeFD) }() // Directory durability is explicitly synchronized below.
	if e = session.ValidateCatalog(ctx, physical, knowledge); e != nil {
		return "", e
	}
	pathLock, lockErr := s.physicalPathFence(inner)
	if lockErr != nil {
		return "", lockErr
	}
	defer closeLocalWrite(pathLock, &storedPath, &resultErr)
	fd, e := unix.Openat(knowledgeFD, filename, unix.O_WRONLY|unix.O_CREAT|unix.O_EXCL|unix.O_NOFOLLOW, 0o600)
	if e != nil {
		return "", e
	}
	dst := os.NewFile(uintptr(fd), filename)
	closed := false
	defer func() {
		if !closed {
			closeLocalWrite(dst, &storedPath, &resultErr)
		}
	}()
	var created unix.Stat_t
	if e = unix.Fstat(fd, &created); e != nil {
		return "", e
	}
	objectIdentity := sha256.Sum256(
		[]byte(strconv.FormatUint(uint64(created.Dev), 10) + "\n" + strconv.FormatUint(uint64(created.Ino), 10)),
	)
	if e = session.LocalCreated(ctx, physical, hex.EncodeToString(objectIdentity[:])); e != nil {
		return "", e
	}
	hash := sha256.New()
	var written int64
	reader := io.LimitReader(src, header.Size)
	buffer := make([]byte, 32<<10)
	for {
		if e = ctx.Err(); e != nil {
			return "", e
		}
		if e = session.ValidateCatalog(ctx, physical, knowledge); e != nil {
			return "", e
		}
		n, readErr := reader.Read(buffer)
		if n > 0 {
			w, writeErr := dst.Write(buffer[:n])
			written += int64(w)
			hash.Write(buffer[:w])
			if writeErr != nil {
				return "", writeErr
			}
			if w != n {
				return "", io.ErrShortWrite
			}
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return "", readErr
		}
	}
	var extra [1]byte
	extraN, readErr := src.Read(extra[:])
	if written != header.Size || extraN != 0 || (readErr != nil && readErr != io.EOF) {
		return "", errors.New("capacity upload size changed")
	}
	if e = dst.Sync(); e != nil {
		return "", e
	}
	for _, dir := range []int{knowledgeFD, tenantFD, root} {
		if e = unix.Fsync(dir); e != nil {
			return "", e
		}
	}
	e = dst.Close()
	closed = true
	if e != nil {
		return "", e
	}
	if e = session.PhysicalWritten(ctx, physical, written, hex.EncodeToString(hash.Sum(nil))); e != nil {
		return "", e
	}
	return inner, nil
}

func (s *localFileService) InspectCapacityIntent(
	ctx context.Context,
	o types.CapacityLocalObject,
	callback func(types.CapacityLocalObject) error,
) (resultErr error) {
	if callback == nil {
		return errors.New("capacity reconciliation callback required")
	}
	parts, e := capacityParts(o.PhysicalPath, o.BackendID, o.TenantKnowledgeID, o.Token)
	if e != nil {
		return e
	}
	root, identity, scope, e := s.capacityRoot()
	if e != nil {
		return e
	}
	defer func() { _ = unix.Close(root) }() // Read-only directory descriptor.
	if identity != o.RootIdentity || scope != o.ScopeID {
		return errors.New("capacity root/filesystem identity changed")
	}
	lock, e := capacityWriterLock(root, o.Token)
	if e != nil {
		return e
	}
	defer func() { resultErr = errors.Join(resultErr, lock.Close()) }()
	// NOFOLLOW applies to every path component, including missing/linked
	// directories. An unavailable path is not classified as an absent object.
	tenantFD, e := capacityDirectory(root, parts[0], false)
	if errors.Is(e, unix.ENOENT) {
		o.Exists = false
		o.Size = 0
		return callback(o)
	}
	if e != nil {
		return e
	}
	defer func() { _ = unix.Close(tenantFD) }() // Read-only directory descriptor.
	knowledgeFD, e := capacityDirectory(tenantFD, parts[1], false)
	if errors.Is(e, unix.ENOENT) {
		o.Exists = false
		o.Size = 0
		return callback(o)
	}
	if e != nil {
		return e
	}
	defer func() { _ = unix.Close(knowledgeFD) }() // Read-only directory descriptor.
	fd, e := unix.Openat(knowledgeFD, parts[2], unix.O_RDONLY|unix.O_NOFOLLOW, 0)
	if errors.Is(e, unix.ENOENT) {
		o.Exists = false
		o.Size = 0
		return callback(o)
	}
	if e != nil {
		return e
	}
	file := os.NewFile(uintptr(fd), parts[2])
	defer func() { _ = file.Close() }() // Read-only observation; no buffered writes.
	var before, after unix.Stat_t
	if e = unix.Fstat(fd, &before); e != nil {
		return e
	}
	if before.Nlink != 1 || before.Mode&unix.S_IFMT != unix.S_IFREG {
		return errors.New("capacity object is linked or nonregular")
	}
	hash := sha256.New()
	bytes, e := io.Copy(hash, file)
	if e != nil {
		return e
	}
	if e = unix.Fstat(fd, &after); e != nil {
		return e
	}
	var current unix.Stat_t
	if e = unix.Fstatat(knowledgeFD, parts[2], &current, unix.AT_SYMLINK_NOFOLLOW); e != nil {
		return e
	}
	if before.Dev != after.Dev || before.Ino != after.Ino || before.Size != after.Size || before.Size != bytes ||
		current.Ino != before.Ino ||
		current.Dev != before.Dev ||
		current.Nlink != 1 {
		return errors.New("capacity object changed during observation")
	}
	if e = ctx.Err(); e != nil {
		return e
	}
	o.Exists = true
	objectIdentity := sha256.Sum256(
		[]byte(strconv.FormatUint(uint64(before.Dev), 10) + "\n" + strconv.FormatUint(uint64(before.Ino), 10)),
	)
	o.ObjectIdentity = hex.EncodeToString(objectIdentity[:])
	o.Size = bytes
	o.SHA256 = hex.EncodeToString(hash.Sum(nil))
	return callback(o)
}
