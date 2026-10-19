// Package logger tests request-local restrictions on content telemetry.
package logger

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/Tencent/WeKnora/internal/logprivacy"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
)

func TestSourceContentDebugLoggingAndDetachedContextsAreSuppressed(t *testing.T) {
	dir := t.TempDir()
	previousEnabled, previousDir := llmDebug.enabled, llmDebug.dir
	llmDebug.enabled, llmDebug.dir = true, dir
	t.Cleanup(func() { llmDebug.enabled, llmDebug.dir = previousEnabled, previousDir })
	private := &LLMCallRecord{
		CallType: "source",
		Model:    "private-model",
		Sections: []RecordSection{{Title: "Input", Content: "PRIVATE_SOURCE_BODY_AND_ENTITY"}},
		Error:    "PRIVATE_SOURCE_ERROR_AND_CREDENTIAL",
	}
	ctx := logprivacy.SuppressContent(context.Background())
	rag := types.WithRAGSourceLineage(context.Background(), types.NewRAGSourceLineage(nil))
	for _, current := range []context.Context{
		ctx,
		rag,
		CloneContext(ctx),
		CloneContextWithoutTrace(ctx),
		CloneContext(rag),
		CloneContextWithoutTrace(rag),
	} {
		require.True(t, logprivacy.ContentSuppressed(current))
		LLMDebugLog(current, private)
	}
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Empty(t, entries, "private source calls must not create a debug file")
	LLMDebugLog(
		context.Background(),
		&LLMCallRecord{
			CallType: "ordinary",
			Sections: []RecordSection{{Title: "Input", Content: "PUBLIC_ORDINARY_INPUT"}},
		},
	)
	entries, err = os.ReadDir(dir)
	require.NoError(t, err)
	require.Len(t, entries, 1, "ordinary debug logging must remain enabled")
	data, err := os.ReadFile(filepath.Join(dir, entries[0].Name()))
	require.NoError(t, err)
	require.Contains(t, string(data), "PUBLIC_ORDINARY_INPUT")
	require.NotContains(t, string(data), "PRIVATE_SOURCE")
}
