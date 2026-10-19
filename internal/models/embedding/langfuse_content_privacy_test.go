package embedding

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

type privacyEmbeddingSpy struct {
	ctx       context.Context
	recording bool
}

func (s *privacyEmbeddingSpy) GetModelName() string { return "controlled" }
func (s *privacyEmbeddingSpy) GetModelID() string   { return "controlled" }
func (s *privacyEmbeddingSpy) GetDimensions() int   { return 1 }
func (s *privacyEmbeddingSpy) Embed(ctx context.Context, _ string) ([]float32, error) {
	s.ctx = ctx
	s.recording = trace.SpanFromContext(ctx).IsRecording()
	return []float32{1}, nil
}

func (s *privacyEmbeddingSpy) BatchEmbed(ctx context.Context, _ []string) ([][]float32, error) {
	s.ctx = ctx
	s.recording = trace.SpanFromContext(ctx).IsRecording()
	return [][]float32{{1}}, nil
}

func (s *privacyEmbeddingSpy) BatchEmbedWithPool(
	ctx context.Context,
	model Embedder,
	texts []string,
) ([][]float32, error) {
	return model.BatchEmbed(ctx, texts)
}

func TestSourceContentEmbeddingTelemetryBypassPreservesOrdinary(t *testing.T) {
	var payload strings.Builder
	var mu sync.Mutex
	var requests int
	mgr := privacyTelemetryManager(t, &payload, &mu, &requests)
	spy := &privacyEmbeddingSpy{}
	wrapper := &langfuseEmbedder{inner: spy}
	for _, ctx := range privacyContexts() {
		_, err := wrapper.Embed(ctx, "PRIVATE_SOURCE_EMBED")
		require.NoError(t, err)
		require.True(t, spy.ctx == ctx)
		require.False(t, spy.recording)
		_, err = wrapper.BatchEmbed(ctx, []string{"PRIVATE_SOURCE_BATCH"})
		require.NoError(t, err)
		require.True(t, spy.ctx == ctx)
		_, err = wrapper.BatchEmbedWithPool(ctx, wrapper, []string{"PRIVATE_SOURCE_POOL"})
		require.NoError(t, err)
		require.True(t, spy.ctx == ctx)
		require.False(t, spy.recording)
	}
	require.NoError(t, mgr.Shutdown(context.Background()))
	require.Zero(t, requests, "private model calls must emit no Langfuse HTTP request")
	require.Zero(t, payload.Len())
	mgr = privacyTelemetryManager(t, &payload, &mu, &requests)
	_, err := wrapper.Embed(context.Background(), "PUBLIC_ORDINARY_EMBED")
	require.NoError(t, err)
	require.True(t, spy.recording)
	_, err = wrapper.BatchEmbedWithPool(context.Background(), wrapper, []string{"PUBLIC_ORDINARY_BATCH"})
	require.NoError(t, err)
	require.NoError(t, mgr.Shutdown(context.Background()))
	require.Positive(t, requests)
	require.Contains(t, payload.String(), "PUBLIC_ORDINARY_EMBED")
	require.Contains(t, payload.String(), "PUBLIC_ORDINARY_BATCH")
	require.NotContains(t, payload.String(), "PRIVATE_SOURCE")
}
