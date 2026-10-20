//go:build linux || darwin

package file

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

func TestLocalGCReplacementIsMovedAndRestoredWithoutUnlink(t *testing.T) {
	base := t.TempDir()
	s := &localFileService{baseDir: base}
	path := filepath.Join(base, "7", "old", "1790000000000000000.txt")
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o700))
	require.NoError(t, os.WriteFile(path, []byte("old source"), 0o600))
	root, _, _, e := s.capacityRoot()
	require.NoError(t, e)
	defer func() { require.NoError(t, unix.Close(root)) }()
	parent, e := unix.Open(filepath.Dir(path), unix.O_RDONLY|unix.O_DIRECTORY, 0)
	require.NoError(t, e)
	defer func() { require.NoError(t, unix.Close(parent)) }()
	var original unix.Stat_t
	require.NoError(t, unix.Fstatat(parent, filepath.Base(path), &original, unix.AT_SYMLINK_NOFOLLOW))
	replacement := path + ".new"
	require.NoError(t, os.WriteFile(replacement, []byte("current replacement survives"), 0o600))
	require.NoError(t, os.Rename(replacement, path))
	unlinked, e := gcQuarantineUnlink(
		context.Background(),
		root,
		parent,
		filepath.Base(path),
		"local://7/old/1790000000000000000.txt",
		original,
	)
	require.Error(t, e)
	require.False(t, unlinked)
	bytes, e := os.ReadFile(path)
	require.NoError(t, e)
	require.Equal(t, []byte("current replacement survives"), bytes)
}

func TestLocalGCPathFenceCoversLegacyActualWriter(t *testing.T) {
	base := t.TempDir()
	s := &localFileService{baseDir: base}
	path := filepath.Join(base, "7", "old", "1790000000000000000.txt")
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o700))
	require.NoError(t, os.WriteFile(path, []byte("held original"), 0o600))
	fence, e := s.physicalPathFence(path)
	require.NoError(t, e)
	unlinked, e := s.DeleteNextcloudObject(context.Background(), "local://7/old/1790000000000000000.txt")
	require.Error(t, e)
	require.False(t, unlinked)
	require.NoError(t, fence.Close())
	unlinked, e = s.DeleteNextcloudObject(context.Background(), "local://7/old/1790000000000000000.txt")
	require.NoError(t, e)
	require.True(t, unlinked)
	_, e = os.Stat(path)
	require.ErrorIs(t, e, os.ErrNotExist)
}

func TestLocalGCHoldingReceiptResumesExactInode(t *testing.T) {
	base := t.TempDir()
	s := &localFileService{baseDir: base}
	path := filepath.Join(base, "7", "old", "legacy.txt")
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o700))
	require.NoError(t, os.WriteFile(path, []byte("held bytes"), 0o600))
	root, _, _, e := s.capacityRoot()
	require.NoError(t, e)
	defer func() { require.NoError(t, unix.Close(root)) }()
	parent, e := unix.Open(filepath.Dir(path), unix.O_RDONLY|unix.O_DIRECTORY, 0)
	require.NoError(t, e)
	defer func() { require.NoError(t, unix.Close(parent)) }()
	var st unix.Stat_t
	require.NoError(t, unix.Fstatat(parent, "legacy.txt", &st, unix.AT_SYMLINK_NOFOLLOW))
	holding, e := gcQuarantineDirectory(root)
	require.NoError(t, e)
	defer func() { require.NoError(t, unix.Close(holding)) }()
	receipt, obj := gcHoldingNames("local://7/old/legacy.txt")
	require.NoError(
		t,
		gcPersistMoveReceipt(
			holding,
			receipt,
			localGCMoveReceipt{"local://7/old/legacy.txt", uint64(st.Dev), uint64(st.Ino)},
		),
	)
	require.NoError(t, gcRenameExclusive(parent, "legacy.txt", holding, obj))
	// A crash at this point loses no object identity and does not require an
	// operator to certify a newly observed inode at the original pathname.
	unlinked, e := s.DeleteNextcloudObject(context.Background(), "local://7/old/legacy.txt")
	require.NoError(t, e)
	require.True(t, unlinked)
	var after unix.Stat_t
	require.ErrorIs(t, unix.Fstatat(holding, obj, &after, unix.AT_SYMLINK_NOFOLLOW), unix.ENOENT)
}

func TestLocalGCHoldingSubprocessCrash(t *testing.T) {
	if base := os.Getenv("WEKNORA_GC_HOLDING_CHILD"); base != "" {
		s := &localFileService{baseDir: base}
		root, _, _, e := s.capacityRoot()
		require.NoError(t, e)
		parent, e := unix.Open(filepath.Join(base, "7", "old"), unix.O_RDONLY|unix.O_DIRECTORY, 0)
		require.NoError(t, e)
		holding, e := gcQuarantineDirectory(root)
		require.NoError(t, e)
		var st unix.Stat_t
		require.NoError(t, unix.Fstatat(parent, "crash.txt", &st, unix.AT_SYMLINK_NOFOLLOW))
		receipt, obj := gcHoldingNames("local://7/old/crash.txt")
		require.NoError(
			t,
			gcPersistMoveReceipt(
				holding,
				receipt,
				localGCMoveReceipt{"local://7/old/crash.txt", uint64(st.Dev), uint64(st.Ino)},
			),
		)
		require.NoError(t, gcRenameExclusive(parent, "crash.txt", holding, obj))
		require.NoError(t, unix.Fsync(parent))
		require.NoError(t, unix.Fsync(holding))
		os.Exit(79)
	}
	base := t.TempDir()
	path := filepath.Join(base, "7", "old", "crash.txt")
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o700))
	require.NoError(t, os.WriteFile(path, []byte("owned crash bytes"), 0o600))
	child := exec.Command(os.Args[0], "-test.run=^TestLocalGCHoldingSubprocessCrash$")
	child.Env = append(os.Environ(), "WEKNORA_GC_HOLDING_CHILD="+base)
	err := child.Run()
	var exit *exec.ExitError
	require.ErrorAs(t, err, &exit)
	require.Equal(t, 79, exit.ExitCode())
	s := &localFileService{baseDir: base}
	unlinked, err := s.DeleteNextcloudObject(context.Background(), "local://7/old/crash.txt")
	require.NoError(t, err)
	require.True(t, unlinked)
	unlinked, err = s.DeleteNextcloudObject(context.Background(), "local://7/old/crash.txt")
	require.NoError(t, err)
	require.False(t, unlinked)
	entries, err := os.ReadDir(filepath.Join(base, ".gc-exact-holding"))
	require.NoError(t, err)
	require.Empty(t, entries)
}
