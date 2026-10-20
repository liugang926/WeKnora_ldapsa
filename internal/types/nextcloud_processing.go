package types

import "context"

// NextcloudProcessingLedger is installed by the actual task wiring. Counters
// represent a frozen task manifest, never a pending-subtask approximation.
type NextcloudProcessingLedger interface {
	PlanGraph(context.Context, *Knowledge, int, []*Chunk, bool, bool) error
	StartGraph(context.Context, uint64, string, int, string) (bool, error)
	FailGraph(context.Context, uint64, string, int, string, string, bool) error
}

type (
	graphWriteOperationKey struct{}
	// GraphWriteOperation identifies the graph writer operation recorded by a producer.
	GraphWriteOperation struct {
		ID      string
		Attempt int
		ChunkID string
	}
)

// WithGraphWriteOperation attaches the exact server-owned graph write operation.
func WithGraphWriteOperation(ctx context.Context, operation GraphWriteOperation) context.Context {
	return context.WithValue(ctx, graphWriteOperationKey{}, operation)
}

// GraphWriteOperationFromContext returns a valid attached graph write operation.
func GraphWriteOperationFromContext(ctx context.Context) (GraphWriteOperation, bool) {
	value, ok := ctx.Value(graphWriteOperationKey{}).(GraphWriteOperation)
	return value, ok && value.ID != "" && value.Attempt > 0 && value.ChunkID != ""
}

// NextcloudGraphQueueLedger records the frozen graph payload and its queue state.
type NextcloudGraphQueueLedger interface {
	RecordGraphPayload(context.Context, uint64, string, int, string, []byte) error
	MarkGraphQueued(context.Context, uint64, string, int, string, bool) error
}
type graphQueueLedgerKey struct{}

// WithNextcloudGraphQueueLedger attaches the actual graph processing ledger.
func WithNextcloudGraphQueueLedger(ctx context.Context, ledger NextcloudProcessingLedger) context.Context {
	return context.WithValue(ctx, graphQueueLedgerKey{}, ledger)
}

// NextcloudGraphQueueLedgerFromContext returns the attached graph queue ledger.
func NextcloudGraphQueueLedgerFromContext(ctx context.Context) NextcloudGraphQueueLedger {
	ledger, _ := ctx.Value(graphQueueLedgerKey{}).(NextcloudGraphQueueLedger)
	return ledger
}
