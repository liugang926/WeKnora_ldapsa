package types

import (
	"context"
	"testing"

	"github.com/Tencent/WeKnora/internal/logprivacy"
)

func TestSourceContentRAGLineageMarksAndPreservesPrivacy(t *testing.T) {
	ordinary := WithRAGSourceLineage(context.Background(), nil)
	if logprivacy.ContentSuppressed(ordinary) {
		t.Fatal("nil lineage must preserve ordinary telemetry")
	}
	turn := NewRAGSourceLineage(nil)
	ctx := WithRAGSourceLineage(context.Background(), turn)
	if RAGSourceLineageFromContext(ctx) != turn || !logprivacy.ContentSuppressed(ctx) {
		t.Fatal("typed source context must suppress content telemetry without changing lineage")
	}
	if !logprivacy.ContentSuppressed(WithRAGSourceLineage(ctx, nil)) {
		t.Fatal("clearing a lineage handle must not clear inherited privacy")
	}
}
