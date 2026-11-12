package service

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
)

type sweepSnapshotInspector struct {
	fakeTaskInspector
	scans, probes int
}

func (f *sweepSnapshotInspector) QueuedKnowledgeIDs(ctx context.Context) (map[string]struct{}, error) {
	f.scans++
	return f.fakeTaskInspector.QueuedKnowledgeIDs(ctx)
}

func (f *sweepSnapshotInspector) HasQueuedTasksForKnowledge(context.Context, string) (bool, error) {
	f.probes++
	panic("housekeeping must not rescan each document")
}

func TestHousekeeping_SnapshotOncePerSweep(t *testing.T) {
	db := setupHousekeepingDB(t)
	inspector := &sweepSnapshotInspector{fakeTaskInspector: fakeTaskInspector{queued: map[string]bool{}}}
	svc := newHousekeepingSvcWithInspector(db, inspector)
	stale := time.Now().Add(-3 * time.Hour)
	for i := 0; i < 12; i++ {
		id := fmt.Sprintf("queued-%d", i)
		insertKnowledge(t, db, id, types.ParseStatusFinalizing, stale)
		inspector.queued[id] = true
	}
	insertKnowledge(t, db, "durable", types.ParseStatusFinalizing, stale)
	insertWikiPendingOp(t, db, "kb-1", "durable")
	insertKnowledge(t, db, "orphan", types.ParseStatusProcessing, stale)
	svc.runSweep(context.Background())
	require.Equal(t, 1, inspector.scans)
	require.Zero(t, inspector.probes)
	for i := 0; i < 12; i++ {
		status, _ := readKnowledgeStatus(t, db, fmt.Sprintf("queued-%d", i))
		require.Equal(t, types.ParseStatusFinalizing, status)
	}
	status, _ := readKnowledgeStatus(t, db, "durable")
	require.Equal(t, types.ParseStatusFinalizing, status)
	status, _ = readKnowledgeStatus(t, db, "orphan")
	require.Equal(t, types.ParseStatusFailed, status)
}
