package vlm

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/logprivacy"
	"github.com/Tencent/WeKnora/internal/tracing/langfuse"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"
)

func privacyTelemetryManager(t *testing.T, payload *strings.Builder, mu *sync.Mutex, requests *int) *langfuse.Manager {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		data, err := io.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
		}
		mu.Lock()
		*requests = *requests + 1
		payload.Write(data)
		mu.Unlock()
		w.Header().Set("Content-Type", "application/x-protobuf")
		w.WriteHeader(http.StatusOK)
	}))
	cfg := langfuse.Config{
		Enabled:        true,
		Host:           server.URL,
		PublicKey:      "owned-public",
		SecretKey:      "owned-secret",
		FlushAt:        100,
		QueueSize:      128,
		FlushInterval:  time.Hour,
		RequestTimeout: time.Second,
		SampleRate:     1,
	}
	mgr, err := langfuse.Init(cfg)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, mgr.Shutdown(context.Background()))
		_, err := langfuse.Init(langfuse.Config{})
		require.NoError(t, err)
		server.Close()
	})
	return mgr
}

func privacyContexts() []context.Context {
	return []context.Context{
		logprivacy.SuppressContent(context.Background()),
		types.WithRAGSourceLineage(context.Background(), types.NewRAGSourceLineage(nil)),
	}
}

type privacyVLMSpy struct {
	ctx       context.Context
	recording bool
}

func (s *privacyVLMSpy) GetModelName() string { return "controlled" }
func (s *privacyVLMSpy) GetModelID() string   { return "controlled" }
func (s *privacyVLMSpy) Predict(ctx context.Context, images [][]byte, prompt string) (string, error) {
	s.ctx = ctx
	s.recording = trace.SpanFromContext(ctx).IsRecording()
	return prompt, nil
}

func TestSourceContentVLMTelemetryBypassPreservesOrdinary(t *testing.T) {
	var payload strings.Builder
	var mu sync.Mutex
	var requests int
	mgr := privacyTelemetryManager(t, &payload, &mu, &requests)
	spy := &privacyVLMSpy{}
	wrapper := &langfuseVLM{inner: spy}
	for _, ctx := range privacyContexts() {
		response, err := wrapper.Predict(ctx, [][]byte{[]byte("PRIVATE_SOURCE_IMAGE")}, "PRIVATE_SOURCE_PROMPT")
		require.NoError(t, err)
		require.Equal(t, "PRIVATE_SOURCE_PROMPT", response)
		require.True(t, spy.ctx == ctx)
		require.False(t, spy.recording)
	}
	require.NoError(t, mgr.Shutdown(context.Background()))
	require.Zero(t, requests, "private model calls must emit no Langfuse HTTP request")
	require.Zero(t, payload.Len())
	mgr = privacyTelemetryManager(t, &payload, &mu, &requests)
	_, err := wrapper.Predict(context.Background(), [][]byte{{1}}, "PUBLIC_ORDINARY_PROMPT")
	require.NoError(t, err)
	require.True(t, spy.recording)
	require.NoError(t, mgr.Shutdown(context.Background()))
	require.Positive(t, requests)
	require.Contains(t, payload.String(), "PUBLIC_ORDINARY_PROMPT")
	require.NotContains(t, payload.String(), "PRIVATE_SOURCE")
}
