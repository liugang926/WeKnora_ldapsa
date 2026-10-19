//go:build linux || darwin

package file

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"golang.org/x/sys/unix"
)

func (s *localFileService) MeasureStorageCapacity(ctx context.Context) (types.StorageCapacity, error) {
	if err := ctx.Err(); err != nil {
		return types.StorageCapacity{}, err
	}
	root, err := filepath.Abs(s.baseDir)
	if err != nil {
		return types.StorageCapacity{}, err
	}
	resolved, err := filepath.EvalSymlinks(root)
	if err != nil || resolved != root {
		return types.StorageCapacity{}, fmt.Errorf("local capacity root is unavailable or linked")
	}
	info, err := os.Stat(root)
	if err != nil || !info.IsDir() {
		return types.StorageCapacity{}, fmt.Errorf("local capacity root is not a directory")
	}
	fd, err := unix.Open(root, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW, 0)
	if err != nil {
		return types.StorageCapacity{}, err
	}
	defer unix.Close(fd)
	var fs unix.Statfs_t
	if err = unix.Fstatfs(fd, &fs); err != nil {
		return types.StorageCapacity{}, err
	}
	var st unix.Stat_t
	if err = unix.Fstat(fd, &st); err != nil {
		return types.StorageCapacity{}, err
	}
	if fs.Bsize <= 0 || uint64(fs.Blocks) > uint64(math.MaxInt64)/uint64(fs.Bsize) || uint64(fs.Bavail) > uint64(math.MaxInt64)/uint64(fs.Bsize) {
		return types.StorageCapacity{}, fmt.Errorf("capacity measurement overflow")
	}
	total := int64(fs.Blocks) * int64(fs.Bsize)
	available := int64(fs.Bavail) * int64(fs.Bsize)
	if total <= 0 || available < 0 || available > total {
		return types.StorageCapacity{}, fmt.Errorf("invalid filesystem capacity")
	}
	digest := sha256.Sum256([]byte("local-filesystem:" + strconv.FormatUint(uint64(st.Dev), 10)))
	return types.StorageCapacity{ScopeID: hex.EncodeToString(digest[:]), TotalBytes: total, AvailableBytes: available, MeasuredAt: time.Now().UTC()}, nil
}
