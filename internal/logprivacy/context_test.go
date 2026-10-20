package logprivacy

import (
	"context"
	"testing"
)

type ordinaryContextValueKey struct{}

func TestSourceContentSuppressionIsInherited(t *testing.T) {
	if ContentSuppressed(context.Background()) {
		t.Fatal("ordinary contexts must retain content logging")
	}
	ctx := SuppressContent(context.Background())
	child, cancel := context.WithCancel(context.WithValue(ctx, ordinaryContextValueKey{}, "ordinary value"))
	cancel()
	if !ContentSuppressed(ctx) || !ContentSuppressed(child) {
		t.Fatal("context derivation must not drop the content restriction")
	}
}

func TestOrdinaryOrAbsentContextDoesNotClaimContentSuppression(t *testing.T) {
	for _, ordinary := range []struct {
		name string
		ctx  context.Context
	}{
		{name: "absent"},
		{name: "ordinary", ctx: context.Background()},
	} {
		t.Run(ordinary.name, func(t *testing.T) {
			if ContentSuppressed(ordinary.ctx) {
				t.Fatal("an absent or ordinary context must not claim a source restriction")
			}
		})
	}
}
