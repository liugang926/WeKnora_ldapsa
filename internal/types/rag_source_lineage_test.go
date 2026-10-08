package types

import (
	"context"
	"errors"
	"sync"
	"testing"
)

func TestRAGSourceLineageConcurrentInputsPreserveUnknown(t *testing.T) {
	ctx := context.Background()
	turn := NewRAGSourceLineage(func(context.Context, *SourceLineage) error { return nil })
	var wg sync.WaitGroup
	for _, etag := range []string{"old", "new"} {
		wg.Add(1)
		go func(etag string) {
			defer wg.Done()
			proof, _ := NewCompleteSourceLineage(lineageTestIdentity("1", etag))
			_ = turn.Add(proof)
		}(etag)
	}
	wg.Wait()
	if len(turn.Snapshot().Sources) != 2 || turn.Check(ctx) != nil {
		t.Fatal("lost a real input generation")
	}
	snapshot := turn.Snapshot()
	snapshot.Sources[0].ETag = "mutated"
	if turn.Snapshot().Sources[0].ETag == "mutated" {
		t.Fatal("prefix proof aliases the accumulator")
	}
	_ = turn.Add(nil)
	if !errors.Is(turn.Check(ctx), ErrSourceLineageUnknown) {
		t.Fatal("uncovered input was certified")
	}
	proof, _ := NewCompleteSourceLineage()
	_ = turn.Add(proof)
	if !errors.Is(turn.Check(ctx), ErrSourceLineageUnknown) {
		t.Fatal("empty later input removed unknown")
	}
}

func TestRAGSourceLineageUsesCurrentAuthorizationAtEveryBoundary(t *testing.T) {
	revoked := false
	denied := errors.New("revoked")
	turn := NewRAGSourceLineage(func(context.Context, *SourceLineage) error {
		if revoked {
			return denied
		}
		return nil
	})
	ctx := WithRAGSourceLineage(context.Background(), turn)
	if RAGSourceLineageFromContext(ctx) != turn || turn.Check(ctx) != nil {
		t.Fatal("fresh accounted text failed")
	}
	revoked = true
	if !errors.Is(turn.Check(ctx), denied) {
		t.Fatal("prefix reused a previous grant")
	}
	if NewRAGSourceLineage(nil).Check(ctx) == nil {
		t.Fatal("missing authorizer allowed a turn")
	}
}

func TestRAGSourceLineageCheckedSnapshotReauthorizesConcurrentSourceAddition(t *testing.T) {
	var turn *RAGSourceLineage
	calls := 0
	turn = NewRAGSourceLineage(func(_ context.Context, proof *SourceLineage) error {
		calls++
		if calls == 1 {
			input, _ := NewCompleteSourceLineage(lineageTestIdentity("2", "etag"))
			_ = turn.Add(input) // Commit a second input while the first check runs.
			return nil
		}
		if len(proof.Sources) != 1 {
			t.Fatal("new actual input was not checked")
		}
		return errors.New("second source revoked")
	})
	proof, err := turn.CheckedSnapshot(context.Background())
	if err == nil || proof.RequireComplete() == nil || calls != 2 {
		t.Fatal("published an unchecked added source")
	}
}
