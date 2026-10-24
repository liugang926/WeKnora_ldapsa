package handler

import (
	"context"
	"errors"
	"testing"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
)

type nextcloudHealthInspector struct {
	runtimeTestInspector
	stats     []types.WorkerServerStat
	supported bool
	err       error
}

func (i nextcloudHealthInspector) WorkerServerStats(
	context.Context,
) ([]types.WorkerServerStat, bool, error) {
	return i.stats, i.supported, i.err
}

func TestNextcloudWorkerObservationRequiresLiveDatasourceSyncWorker(t *testing.T) {
	tests := []struct {
		name       string
		inspector  nextcloudHealthInspector
		wantWorker bool
	}{
		{
			name: "active maintenance worker",
			inspector: nextcloudHealthInspector{
				stats: []types.WorkerServerStat{{
					Status: "active",
					Queues: types.QueueWeightsForPool(types.WorkerPoolMaintenance),
				}},
				supported: true,
			},
			wantWorker: true,
		},
		{
			name: "quiet maintenance worker",
			inspector: nextcloudHealthInspector{
				stats: []types.WorkerServerStat{{
					Status: "quiet",
					Queues: types.QueueWeightsForPool(types.WorkerPoolMaintenance),
				}},
				supported: true,
			},
			wantWorker: true,
		},
		{
			name: "unrelated worker pool",
			inspector: nextcloudHealthInspector{
				stats: []types.WorkerServerStat{{
					Status: "active",
					Queues: types.QueueWeightsForPool(types.WorkerPoolCore),
				}},
				supported: true,
			},
		},
		{
			name: "stopped maintenance worker",
			inspector: nextcloudHealthInspector{
				stats: []types.WorkerServerStat{{
					Status: "stopped",
					Queues: types.QueueWeightsForPool(types.WorkerPoolMaintenance),
				}},
				supported: true,
			},
		},
		{
			name: "unsupported worker inspection",
			inspector: nextcloudHealthInspector{
				stats: []types.WorkerServerStat{{
					Status: "active",
					Queues: types.QueueWeightsForPool(types.WorkerPoolMaintenance),
				}},
			},
		},
		{
			name: "worker inspection error",
			inspector: nextcloudHealthInspector{
				supported: true,
				err:       errors.New("redis unavailable"),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			observation := nextcloudWorkerObservation(context.Background(), tt.inspector)
			if !tt.wantWorker {
				require.Nil(t, observation)
				return
			}
			require.NotNil(t, observation)
			require.Equal(t, "running", observation.Status)
			require.False(t, observation.ObservedAt.IsZero())
		})
	}

	require.Nil(t, nextcloudWorkerObservation(context.Background(), nil))
}
