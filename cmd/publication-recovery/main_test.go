package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRecoveryPrivateInputAndExclusiveReceipt(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.Chmod(dir, 0o700))
	key := filepath.Join(dir, "key")
	require.NoError(t, os.WriteFile(key, []byte("operator evidence"), 0o600))
	b, e := privateFile(key)
	require.NoError(t, e)
	require.Equal(t, "operator evidence", string(b))
	link := filepath.Join(dir, "alias")
	require.NoError(t, os.Symlink(key, link))
	_, e = privateFile(link)
	require.Error(t, e)
	require.NoError(t, os.Chmod(key, 0o644))
	_, e = privateFile(key)
	require.Error(t, e)
	require.NoError(t, os.Chmod(key, 0o600))
	hard := filepath.Join(dir, "hardlink")
	require.NoError(t, os.Link(key, hard))
	_, e = privateFile(key)
	require.Error(t, e)
	require.NoError(t, os.Remove(hard))
	output := filepath.Join(dir, "receipt")
	require.NoError(t, writePrivate(output, []byte("receipt")))
	require.Error(t, writePrivate(output, []byte("replacement")))
	b, e = privateFile(output)
	require.NoError(t, e)
	require.Equal(t, "receipt", string(b))
}
