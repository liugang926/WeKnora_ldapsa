package types

import (
	"context"
	"sync"
)

type ragLineageContextKey struct{}

// RAGSourceLineage is a request-local accumulator for accounted ordinary RAG
// adapters. It is shared by parallel readers and never serialized or supplied
// to a model. A model response inherits the union of all earlier inputs.
type RAGSourceLineage struct {
	mu        sync.Mutex
	lineage   *SourceLineage
	revision  uint64
	authorize func(context.Context, *SourceLineage) error
}

func NewRAGSourceLineage(authorize func(context.Context, *SourceLineage) error) *RAGSourceLineage {
	lineage, _ := NewCompleteSourceLineage()
	return &RAGSourceLineage{lineage: lineage, authorize: authorize}
}

func WithRAGSourceLineage(ctx context.Context, turn *RAGSourceLineage) context.Context {
	return context.WithValue(ctx, ragLineageContextKey{}, turn)
}

func RAGSourceLineageFromContext(ctx context.Context) *RAGSourceLineage {
	if ctx == nil {
		return nil
	}
	turn, _ := ctx.Value(ragLineageContextKey{}).(*RAGSourceLineage)
	return turn
}

func (t *RAGSourceLineage) Add(inputs ...*SourceLineage) error {
	if t == nil {
		return ErrSourceLineageUnknown
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	var err error
	before := t.lineage
	t.lineage, err = UnionSourceLineage(append([]*SourceLineage{t.lineage}, inputs...)...)
	if !sameRAGSourceLineage(before, t.lineage) {
		t.revision++
	}
	return err
}

func sameRAGSourceLineage(a, b *SourceLineage) bool {
	if a == nil || b == nil {
		return a == b
	}
	if a.Version != b.Version || a.State != b.State || len(a.Sources) != len(b.Sources) {
		return false
	}
	for i := range a.Sources {
		if a.Sources[i] != b.Sources[i] {
			return false
		}
	}
	return true
}

func (t *RAGSourceLineage) Snapshot() *SourceLineage {
	if t == nil {
		return nil
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	copy, _ := UnionSourceLineage(t.lineage)
	return copy
}

func (t *RAGSourceLineage) Check(ctx context.Context) error {
	_, err := t.CheckedSnapshot(ctx)
	return err
}

// CheckedSnapshot returns the exact envelope authorized by the callback. A
// concurrent reader cannot add an unchecked source between check and emission.
// Bounded retries fail closed rather than prolong an operation indefinitely.
func (t *RAGSourceLineage) CheckedSnapshot(ctx context.Context) (*SourceLineage, error) {
	if t == nil || t.authorize == nil {
		return UnknownSourceLineage(), ErrSourceLineageUnknown
	}
	for attempt := 0; attempt < 32; attempt++ {
		t.mu.Lock()
		lineage, _ := UnionSourceLineage(t.lineage)
		revision := t.revision
		t.mu.Unlock()
		if err := lineage.RequireComplete(); err != nil {
			return UnknownSourceLineage(), err
		}
		if err := t.authorize(ctx, lineage); err != nil {
			return UnknownSourceLineage(), err
		}
		t.mu.Lock()
		stable := revision == t.revision
		t.mu.Unlock()
		if stable {
			return lineage, nil
		}
	}
	return UnknownSourceLineage(), ErrSourceLineageUnknown
}

// SourceIndependentDerivativesAllowed applies only to committed, accounted
// source-independent answers. Source-aware passage/memory/suggestion adapters
// are required before controlled or unknown answers may be copied there.
func SourceIndependentDerivativesAllowed(message *Message) bool {
	return message != nil && message.IsCompleted && message.SourceLineage.RequireComplete() == nil && len(message.SourceLineage.Sources) == 0
}
