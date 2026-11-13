package session

import (
	"slices"
	"strings"
	"unicode/utf8"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
)

// Limit the amount of newly grouped text per authorized write. A producer
// event already larger than the limit passes through as a single event.
const maxLiveDeltaBytes = 1024

// nextLiveFrame reads from an already authorized batch. It never waits for
// more deltas or modifies stored events (their offsets still drive reconnect).
func nextLiveFrame(events []interfaces.StreamEvent) (interfaces.StreamEvent, int) {
	first := events[0]
	if len(events) == 1 || !plainLiveDelta(first) {
		return first, 1
	}
	size, count := len(first.Content), 1
	for _, next := range events[1:] {
		if next.ID != first.ID || next.Type != first.Type || len(next.Content) > maxLiveDeltaBytes-size ||
			!plainLiveDelta(next) || !slices.Equal(next.SourceLineage.Sources, first.SourceLineage.Sources) {
			break
		}
		size += len(next.Content)
		count++
	}
	if count > 1 {
		var content strings.Builder
		content.Grow(size)
		for _, evt := range events[:count] {
			content.WriteString(evt.Content)
		}
		first.Content = content.String()
		first.Timestamp = events[count-1].Timestamp
	}
	return first, count
}

func plainLiveDelta(evt interfaces.StreamEvent) bool {
	if (evt.Type != types.ResponseTypeAnswer && evt.Type != types.ResponseTypeThinking) ||
		evt.Done || evt.ID == "" || evt.Usage != nil || len(evt.Data) != 1 ||
		evt.Content == "" || len(evt.Content) > maxLiveDeltaBytes || !utf8.ValidString(evt.Content) {
		return false
	}
	id, ok := evt.Data["event_id"].(string)
	return ok && id == evt.ID && evt.SourceLineage.RequireComplete() == nil
}
