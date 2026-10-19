package types

import (
	"context"
	"sync"

	"github.com/Tencent/WeKnora/internal/logprivacy"
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

// NewRAGSourceLineage creates a turn accumulator with a current authorization checker.
func NewRAGSourceLineage(authorize func(context.Context, *SourceLineage) error) *RAGSourceLineage {
	lineage, _ := NewCompleteSourceLineage()
	return &RAGSourceLineage{lineage: lineage, authorize: authorize}
}

// WithRAGSourceLineage attaches the turn accumulator and suppresses content telemetry.
func WithRAGSourceLineage(ctx context.Context, turn *RAGSourceLineage) context.Context {
	if turn != nil {
		ctx = logprivacy.SuppressContent(ctx)
	}
	return context.WithValue(ctx, ragLineageContextKey{}, turn)
}

// RAGSourceLineageFromContext returns the attached turn accumulator.
func RAGSourceLineageFromContext(ctx context.Context) *RAGSourceLineage {
	if ctx == nil {
		return nil
	}
	turn, _ := ctx.Value(ragLineageContextKey{}).(*RAGSourceLineage)
	return turn
}

// Add unions original input lineages into the shared turn accumulator.
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

// Snapshot copies the accumulated lineage without granting current authorization.
func (t *RAGSourceLineage) Snapshot() *SourceLineage {
	if t == nil {
		return nil
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	snapshot, _ := UnionSourceLineage(t.lineage)
	return snapshot
}

// Check authorizes a stable current snapshot of the accumulated lineage.
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
	return message != nil && message.IsCompleted && message.SourceLineage.RequireComplete() == nil &&
		len(message.SourceLineage.Sources) == 0
}
