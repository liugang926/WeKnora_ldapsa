package api

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/Tencent/WeKnora/internal/logprivacy"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

func contentPrivacyLogContext(out *bytes.Buffer) context.Context {
	log := logrus.New()
	log.SetOutput(out)
	log.SetLevel(logrus.DebugLevel)
	return context.WithValue(context.Background(), types.LoggerContextKey, logrus.NewEntry(log))
}

func TestSourceContentAPIRequestAndStreamLogsRemainPrivate(t *testing.T) {
	var logs bytes.Buffer
	ctx := contentPrivacyLogContext(&logs)
	private := logprivacy.SuppressContent(ctx)
	LogRequest(private, "https://PRIVATE_SOURCE_ENDPOINT", "PRIVATE_SOURCE_MODEL", []byte(`{"content":"PRIVATE_SOURCE_REQUEST"}`), true)
	a := NewStreamAssembler(private, "PRIVATE_SOURCE_MODEL")
	ch := make(chan types.StreamResponse, 32)
	a.Process(ch, Delta{Reasoning: "PRIVATE_SOURCE_REASONING"})
	a.Process(ch, Delta{Content: "PRIVATE_SOURCE_OUTPUT"})
	a.Process(ch, Delta{ToolCalls: []ToolCallDelta{{Index: 0, ID: "PRIVATE_SOURCE_TOOL_ID", Name: "PRIVATE_SOURCE_TOOL_NAME", Arguments: `{}`}}})
	a.Fail(ch, errors.New("PRIVATE_SOURCE_PROVIDER_ERROR"))
	var final types.StreamResponse
	for len(ch) > 0 {
		final = <-ch
	}
	require.Equal(t, types.ResponseTypeError, final.ResponseType)
	require.Equal(t, "PRIVATE_SOURCE_PROVIDER_ERROR", final.Content, "the caller must still receive the original error")
	require.True(t, final.Done)
	require.NotContains(t, logs.String(), "PRIVATE_SOURCE")
	require.Contains(t, logs.String(), "content logging suppressed")
	logs.Reset()
	LogRequest(ctx, "https://ordinary.example.test", "ordinary", []byte(`{"content":"PUBLIC_ORDINARY_REQUEST"}`), false)
	ordinary := NewStreamAssembler(ctx, "ordinary")
	ordinary.Process(ch, Delta{Reasoning: "PUBLIC_ORDINARY_REASONING", Content: "PUBLIC_ORDINARY_OUTPUT"})
	ordinary.Fail(ch, errors.New("PUBLIC_ORDINARY_ERROR"))
	for _, marker := range []string{"PUBLIC_ORDINARY_REQUEST", "PUBLIC_ORDINARY_REASONING", "PUBLIC_ORDINARY_OUTPUT", "PUBLIC_ORDINARY_ERROR"} {
		require.Contains(t, logs.String(), marker)
	}
	logs.Reset()
	cancelled, cancel := context.WithCancel(private)
	cancel()
	abandoned := NewStreamAssembler(cancelled, "ordinary")
	abandoned.Fail(make(chan types.StreamResponse), errors.New("PRIVATE_SOURCE_CANCELLED"))
	require.True(t, abandoned.Aborted(), "private logging must preserve cancellation-aware emission")
	require.NotContains(t, logs.String(), "PRIVATE_SOURCE")
}

func TestSourceContentRawDumpRequiresContextAndPreservesOrdinary(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "not-created")
	t.Setenv("WEKNORA_LLM_STREAM_RAW_DUMP_DIR", dir)
	ctx := logprivacy.SuppressContent(context.Background())
	require.Nil(t, NewStreamPacketDumper(ctx, "PRIVATE_SOURCE_MODEL", map[string]string{"content": "PRIVATE_SOURCE_REQUEST"}))
	require.Nil(t, NewStreamPacketDumper(nil, "missing-context", nil))
	require.NoDirExists(t, dir, "suppression must precede mkdir and file creation")
	ordinary := NewStreamPacketDumper(context.Background(), "ordinary", map[string]string{"content": "PUBLIC_ORDINARY_REQUEST"})
	require.NotNil(t, ordinary)
	ordinary.WritePacketRaw([]byte(`{"content":"PUBLIC_ORDINARY_RESPONSE"}`))
	ordinary.Close()
	data, err := os.ReadFile(ordinary.Path())
	require.NoError(t, err)
	require.Contains(t, string(data), "PUBLIC_ORDINARY_REQUEST")
	require.Contains(t, string(data), "PUBLIC_ORDINARY_RESPONSE")
	require.NotContains(t, string(data), "PRIVATE_SOURCE")
}
