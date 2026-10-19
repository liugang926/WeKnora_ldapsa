package logprivacy

import (
	"context"
	"testing"
)

func TestSourceContentSuppressionIsInherited(t *testing.T) {
	if ContentSuppressed(nil) || ContentSuppressed(context.Background()) {
		t.Fatal("ordinary contexts must retain content logging")
	}
	ctx := SuppressContent(context.Background())
	child, cancel := context.WithCancel(context.WithValue(ctx, struct{}{}, "ordinary value"))
	cancel()
	if !ContentSuppressed(ctx) || !ContentSuppressed(child) {
		t.Fatal("context derivation must not drop the content restriction")
	}
}
