package repository

import (
	"context"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

type graphCommitPauseKey struct{}

func TestPostgresNextcloudActualGraphProviderCommitSerializesRetirement(t *testing.T) {
	state, db, ctx, k, chunk, inputs := processingStateFixtureBackend(t, true, true)
	_, err := state.StartGraph(ctx, 7, k.ID, 1, chunk.ID)
	require.NoError(t, err)
	ctx, graph := boundProcessingGraph(ctx, t, k, chunk, inputs)
	admitted := make(chan struct{})
	release := make(chan struct{})
	name := "pause_graph_provider_actual_commit"
	require.NoError(t, db.Callback().Create().Before("gorm:create").Register(name, func(tx *gorm.DB) {
		if tx.Statement.Table == "nextcloud_graph_records" &&
			tx.Statement.Context.Value(graphCommitPauseKey{}) == true {
			close(admitted)
			<-release
		}
	}))
	t.Cleanup(func() { _ = db.Callback().Create().Remove(name) })
	written := make(chan error, 1)
	go func() {
		written <- NewScopedSQLGraphRepository(
			db,
			nil,
		).AddGraph(context.WithValue(ctx, graphCommitPauseKey{}, true), types.NameSpace{
			KnowledgeBase: "kb",
			Knowledge:     k.ID,
		}, []*types.GraphData{
			graph,
		})
	}()
	select {
	case <-admitted:
	case <-time.After(10 * time.Second):
		t.Fatal("actual writer admission did not arrive")
	}
	retired := make(chan error, 1)
	go func() {
		retired <- NewNextcloudContentLeaseStore(db).RetireKnowledge(context.Background(), NextcloudContentScope{
			TenantID:        7,
			KnowledgeBaseID: "kb",
			KnowledgeID:     k.ID,
			DataSourceID:    "ds",
			ExternalID:      "nextcloud:instance:77",
		})
	}()
	select {
	case err := <-retired:
		t.Fatalf("retirement passed actual provider transaction: %v", err)
	case <-time.After(150 * time.Millisecond):
	}
	close(release)
	require.NoError(t, <-written)
	require.NoError(t, <-retired)
	require.Error(
		t,
		NewScopedSQLGraphRepository(
			db,
			nil,
		).AddGraph(ctx, types.NameSpace{KnowledgeBase: "kb", Knowledge: k.ID}, []*types.GraphData{graph}),
		"released generation cannot commit again",
	)
	require.NoError(t, state.Capture(context.Background(), 7, "kb", "ds", "nextcloud:instance:77", true))
	status := loadProcessingState(t, db)
	require.Equal(t, "cancelled", status.Graph.State)
	require.False(t, status.Graph.Persisted)
	require.Equal(t, "unverified", status.PublicationState)
}
