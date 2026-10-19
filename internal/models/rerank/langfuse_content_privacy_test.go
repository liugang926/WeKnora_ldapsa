package rerank

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

type privacyRerankSpy struct {
	ctx       context.Context
	recording bool
}

func (s *privacyRerankSpy) GetModelName() string { return "controlled" }
func (s *privacyRerankSpy) GetModelID() string   { return "controlled" }
func (s *privacyRerankSpy) Rerank(ctx context.Context, _ string, documents []string) ([]RankResult, error) {
	s.ctx = ctx
	s.recording = trace.SpanFromContext(ctx).IsRecording()
	return []RankResult{{Index: 0, Document: DocumentInfo{Text: documents[0]}, RelevanceScore: 1}}, nil
}

func TestSourceContentRerankTelemetryBypassPreservesOrdinary(t *testing.T) {
	var payload strings.Builder
	var mu sync.Mutex
	var requests int
	mgr := privacyTelemetryManager(t, &payload, &mu, &requests)
	spy := &privacyRerankSpy{}
	wrapper := &langfuseReranker{inner: spy}
	for _, ctx := range privacyContexts() {
		response, err := wrapper.Rerank(ctx, "PRIVATE_SOURCE_QUERY", []string{"PRIVATE_SOURCE_DOCUMENT"})
		require.NoError(t, err)
		require.Equal(t, "PRIVATE_SOURCE_DOCUMENT", response[0].Document.Text)
		require.True(t, spy.ctx == ctx)
		require.False(t, spy.recording)
	}
	require.NoError(t, mgr.Shutdown(context.Background()))
	require.Zero(t, requests, "private model calls must emit no Langfuse HTTP request")
	require.Zero(t, payload.Len())
	mgr = privacyTelemetryManager(t, &payload, &mu, &requests)
	_, err := wrapper.Rerank(context.Background(), "PUBLIC_ORDINARY_QUERY", []string{"PUBLIC_ORDINARY_DOCUMENT"})
	require.NoError(t, err)
	require.True(t, spy.recording)
	require.NoError(t, mgr.Shutdown(context.Background()))
	require.Positive(t, requests)
	require.Contains(t, payload.String(), "PUBLIC_ORDINARY_QUERY")
	require.Contains(t, payload.String(), "PUBLIC_ORDINARY_DOCUMENT")
	require.NotContains(t, payload.String(), "PRIVATE_SOURCE")
}
