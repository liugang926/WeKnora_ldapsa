package file

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

type failingLocalClose struct {
	err   error
	calls int
}

func (f *failingLocalClose) Close() error { f.calls++; return f.err }

func TestLocalWriteCloseFailureCannotPublishPath(t *testing.T) {
	writeErr, closeErr := errors.New("write failed"), errors.New("flush failed during close")
	for _, earlier := range []error{nil, writeErr} {
		closer := &failingLocalClose{err: closeErr}
		path, err := func() (path string, resultErr error) {
			defer closeLocalWrite(closer, &path, &resultErr)
			return "local://uncommitted-object", earlier
		}()
		require.Empty(t, path, "the caller must not receive a successful storage reference after close fails")
		require.ErrorIs(t, err, closeErr)
		if earlier != nil {
			require.ErrorIs(t, err, earlier)
		}
		require.Equal(t, 1, closer.calls)
	}
}

func TestLocalWriteSuccessfulClosePreservesOriginalResult(t *testing.T) {
	closer := &failingLocalClose{}
	path, err := func() (path string, resultErr error) {
		defer closeLocalWrite(closer, &path, &resultErr)
		return "local://complete-object", nil
	}()
	require.NoError(t, err)
	require.Equal(t, "local://complete-object", path)
	require.Equal(t, 1, closer.calls)
}
