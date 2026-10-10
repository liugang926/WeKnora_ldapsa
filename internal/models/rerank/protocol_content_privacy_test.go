package rerank

import (
	"bytes"
	"context"
	"testing"

	"github.com/Tencent/WeKnora/internal/logprivacy"
	"github.com/Tencent/WeKnora/internal/models/catalog"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

func TestSourceContentProtocolRerankDebugPreservesOrdinary(t *testing.T) {
	var logs bytes.Buffer
	log := logrus.New()
	log.SetOutput(&logs)
	log.SetLevel(logrus.DebugLevel)
	ctx := context.WithValue(context.Background(), types.LoggerContextKey, logrus.NewEntry(log))
	r := newWrapped(&fakeProtocol{}, catalog.RerankSettings{})
	for _, private := range []context.Context{logprivacy.SuppressContent(ctx), types.WithRAGSourceLineage(ctx, types.NewRAGSourceLineage(nil))} {
		result, err := r.Rerank(private, "PRIVATE_SOURCE_QUERY", []string{"PRIVATE_SOURCE_DOCUMENT"})
		require.NoError(t, err)
		require.Equal(t, "PRIVATE_SOURCE_DOCUMENT", result[0].Document.Text)
	}
	require.Zero(t, logs.Len(), "private query/document previews must not enter ordinary debug logs")
	_, err := r.Rerank(ctx, "PUBLIC_ORDINARY_QUERY", []string{"PUBLIC_ORDINARY_DOCUMENT"})
	require.NoError(t, err)
	require.Contains(t, logs.String(), "PUBLIC_ORDINARY_QUERY")
	require.Contains(t, logs.String(), "PUBLIC_ORDINARY_DOCUMENT")
	require.NotContains(t, logs.String(), "PRIVATE_SOURCE")
}
