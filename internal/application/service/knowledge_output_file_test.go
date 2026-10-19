package service

import (
	"bytes"
	"context"
	"io"
	"os"
	"testing"

	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
)

type outputArtifactReader struct {
	reader *bytes.Reader
	closed bool
	onRead func()
}

func (f *outputArtifactReader) Read(p []byte) (int, error) {
	if f.onRead != nil {
		f.onRead()
		f.onRead = nil
	}
	return f.reader.Read(p)
}
func (f *outputArtifactReader) Close() error { f.closed = true; return nil }

func TestKnowledgeOutputFileActualArtifactSnapshotAndRange(t *testing.T) {
	for _, body := range [][]byte{{}, {'a'}, {'a', 'b'}, {'a', 'b', 'c'}, {0, 255, 1, 2, 3}, bytes.Repeat([]byte{17, 33, 64, 255}, 20000)} {
		t.Run(string(rune(len(body)+65)), func(t *testing.T) {
			t.Setenv("TMPDIR", t.TempDir())
			a, reader, row := outputAuthorizerFixture(t)
			row.Type = "file"
			digest, err := repository.KnowledgeInputDigest(body)
			require.NoError(t, err)
			reader.snapshot.Proof.InputDigest = digest
			guard, err := a.Begin(context.Background(), []*types.Knowledge{row})
			require.NoError(t, err)
			input := &outputArtifactReader{reader: bytes.NewReader(body)}
			file, err := guard.BindFile(context.Background(), row, input)
			require.NoError(t, err)
			require.True(t, input.closed)
			actual, err := io.ReadAll(file)
			require.NoError(t, err)
			require.Equal(t, body, actual)
			if len(body) > 0 {
				_, err := file.Seek(int64(len(body)-1), io.SeekStart)
				require.NoError(t, err)
				tail, err := io.ReadAll(file)
				require.NoError(t, err)
				require.Equal(t, body[len(body)-1:], tail)
			}
			snapshot := file.(*knowledgeOutputFileSnapshot)
			info, err := os.Stat(snapshot.Name())
			require.NoError(t, err)
			require.Equal(t, os.FileMode(0o600), info.Mode().Perm())
			require.NoError(t, file.Close())
			require.NoError(t, file.Close())
			_, err = os.Stat(snapshot.directory)
			require.True(t, os.IsNotExist(err))
		})
	}
}

func TestKnowledgeOutputFileManualCanonicalUTF8AndTamperDenied(t *testing.T) {
	for _, tc := range []struct {
		name     string
		original string
		actual   []byte
		allowed  bool
	}{
		{"unicode_quotes_html", "中文\n<>&\"\u2028", []byte("中文\n<>&\"\u2028"), true},
		{"different_body", "original body", []byte("unaccounted body"), false},
		{"invalid_utf8_cannot_match_replacement", "\ufffd", []byte{255}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("TMPDIR", t.TempDir())
			a, reader, row := outputAuthorizerFixture(t)
			row.Type = types.KnowledgeTypeManual
			digest, err := repository.KnowledgeInputDigest(tc.original)
			require.NoError(t, err)
			reader.snapshot.Proof.InputDigest = digest
			guard, err := a.Begin(context.Background(), []*types.Knowledge{row})
			require.NoError(t, err)
			input := &outputArtifactReader{reader: bytes.NewReader(tc.actual)}
			file, err := guard.BindFile(context.Background(), row, input)
			if tc.allowed {
				require.NoError(t, err)
				require.NoError(t, file.Close())
			} else {
				require.Error(t, err)
				require.Nil(t, file)
			}
			require.True(t, input.closed)
			remaining, err := os.ReadDir(os.Getenv("TMPDIR"))
			require.NoError(t, err)
			require.Empty(t, remaining)
		})
	}
}

func TestKnowledgeOutputFileWithdrawalDuringCopyNeverReleasesSnapshot(t *testing.T) {
	t.Setenv("TMPDIR", t.TempDir())
	a, reader, row := outputAuthorizerFixture(t)
	row.Type = "file"
	body := []byte("exact original bytes")
	digest, err := repository.KnowledgeInputDigest(body)
	require.NoError(t, err)
	reader.snapshot.Proof.InputDigest = digest
	revoked := false
	a.checker = outputCheckerFunc(func(context.Context, *types.SourceLineage, access.LineageReadScope) error {
		if revoked {
			return access.ErrNextcloudPublicationDenied
		}
		return nil
	})
	guard, err := a.Begin(context.Background(), []*types.Knowledge{row})
	require.NoError(t, err)
	input := &outputArtifactReader{reader: bytes.NewReader(body), onRead: func() { revoked = true }}
	file, err := guard.BindFile(context.Background(), row, input)
	require.ErrorIs(t, err, access.ErrNextcloudPublicationDenied)
	require.Nil(t, file)
	require.True(t, input.closed)
	remaining, err := os.ReadDir(os.Getenv("TMPDIR"))
	require.NoError(t, err)
	require.Empty(t, remaining)
}
