package service

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"sync"
	"unicode/utf8"

	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/types"
)

type knowledgeOutputFileSnapshot struct {
	*os.File
	directory string
	once      sync.Once
	err       error
}

func (f *knowledgeOutputFileSnapshot) Close() error {
	f.once.Do(func() {
		f.err = errors.Join(f.File.Close(), os.Remove(f.Name()), os.Remove(f.directory))
	})
	return f.err
}

// BindFile snapshots the actual opened artifact before any public byte leaves
// the process. Its digest must match the retained original input, not current
// filename/hash metadata. The returned private file supports HTTP ranges and
// is removed on Close; permission/proof guards still apply to its delivery.
func (g *KnowledgeOutputGuard) BindFile(
	ctx context.Context,
	row *types.Knowledge,
	input io.ReadCloser,
) (io.ReadSeekCloser, error) {
	if input == nil {
		return nil, types.ErrSourceLineageUnknown
	}
	defer func() {
		// Only the consumed source reader is closed; no artifact write is committed here.
		_ = input.Close()
	}()
	if err := g.Check(ctx, []*types.Knowledge{row}); err != nil {
		return nil, err
	}
	entry, exists := g.entries[knowledgeOutputKey(row)]
	if !exists {
		return nil, repository.ErrKnowledgeInputChanged
	}
	if row.Type != "file" && row.Type != types.KnowledgeTypeManual {
		// URL/passage/other physical artifacts need a typed original artifact
		// receipt. A URL string or generated passage cannot certify bytes.
		return nil, types.ErrSourceLineageUnknown
	}
	directory, err := os.MkdirTemp("", "weknora-output-")
	if err != nil {
		return nil, err
	}
	file, err := os.OpenFile(directory+"/artifact", os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
	if err != nil {
		_ = os.Remove(directory)
		return nil, err
	}
	snapshot := &knowledgeOutputFileSnapshot{File: file, directory: directory}
	keep := false
	defer func() {
		if !keep {
			_ = snapshot.Close()
		}
	}()
	var digest string
	if row.Type == types.KnowledgeTypeManual {
		// Manual authoring is capped at 200,000 Unicode runes. This bound is
		// larger than its maximal UTF-8 byte representation, without allowing
		// an unrelated unbounded reader to masquerade as manual input.
		body, err := io.ReadAll(io.LimitReader(input, 1024*1024+1))
		if err != nil {
			return nil, err
		}
		if len(body) > 1024*1024 {
			return nil, types.ErrSourceLineageLimit
		}
		if !utf8.Valid(body) {
			return nil, repository.ErrKnowledgeInputChanged
		}
		digest, err = repository.KnowledgeInputDigest(string(body))
		if err != nil {
			return nil, err
		}
		if _, err := file.Write(body); err != nil {
			return nil, err
		}
	} else {
		// KnowledgeInputDigest([]byte) is SHA-256 of the canonical JSON
		// base64 string. Encode the exact copied bytes incrementally so large
		// uploads do not require an equally large heap allocation.
		hash := sha256.New()
		_, _ = hash.Write([]byte{'"'})
		encoder := base64.NewEncoder(base64.StdEncoding, hash)
		_, copyErr := io.Copy(io.MultiWriter(file, encoder), input)
		encodeErr := encoder.Close()
		if err := errors.Join(copyErr, encodeErr); err != nil {
			return nil, err
		}
		_, _ = hash.Write([]byte{'"'})
		digest = hex.EncodeToString(hash.Sum(nil))
	}
	if digest != entry.proof.InputDigest {
		return nil, repository.ErrKnowledgeInputChanged
	}
	if err := g.Check(ctx, []*types.Knowledge{row}); err != nil {
		return nil, err
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	keep = true
	return snapshot, nil
}
