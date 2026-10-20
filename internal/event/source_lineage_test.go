package event

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/Tencent/WeKnora/internal/types"
)

func TestRAGSourceLineageAdapterPreservesInternalProof(t *testing.T) {
	proof, _ := types.NewCompleteSourceLineage()
	bus := NewEventBus()
	adapter := bus.AsEventBusInterface()
	called := false
	adapter.On(types.EventType(EventAgentFinalAnswer), func(_ context.Context, evt types.Event) error {
		called = true
		if evt.SourceLineage.RequireComplete() != nil {
			t.Fatal("adapter lost producer proof")
		}
		return nil
	})
	if err := adapter.Emit(
		context.Background(),
		types.Event{Type: types.EventType(EventAgentFinalAnswer), SourceLineage: proof},
	); err != nil {
		t.Fatal(err)
	}
	if !called {
		t.Fatal("actual event handler was not reached")
	}
	for _, value := range []any{Event{SourceLineage: proof}, types.Event{SourceLineage: proof}} {
		payload, err := json.Marshal(value)
		if err != nil || strings.Contains(strings.ToLower(string(payload)), "lineage") {
			t.Fatal("public event exposed internal lineage")
		}
	}
}
