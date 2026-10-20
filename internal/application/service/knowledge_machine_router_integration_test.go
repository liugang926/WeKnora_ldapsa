package service_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/application/service"
	"github.com/Tencent/WeKnora/internal/router"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/hibiken/asynq"
	"github.com/stretchr/testify/require"
)

func machineQueueTaskID(t *testing.T, task *asynq.Task) string {
	t.Helper()
	var p struct {
		Receipt struct {
			ID     string `json:"id"`
			TaskID string `json:"task_id"`
		} `json:"_nextcloud_machine_task"`
	}
	require.NoError(t, json.Unmarshal(task.Payload(), &p))
	require.NotEmpty(t, p.Receipt.ID)
	require.NotEmpty(t, p.Receipt.TaskID)
	return p.Receipt.TaskID
}

func runMachineQueue(t *testing.T, f *service.MachineQueueFixture, task *asynq.Task) error {
	t.Helper()
	executor := router.NewSyncTaskExecutor()
	results := make(chan error, 1)
	handler := router.WrapKnowledgeBuildTask(repository.NewNextcloudContentLeaseStore(f.Database()),
		f.KnowledgeRepository(), f.ChunkRepository(), f.KnowledgeBases(), f.Groups(), f.Tracker(),
		f.Inputs(), f.Handler(task.Type()))
	executor.RegisterHandler(task.Type(), func(ctx context.Context, actual *asynq.Task) error {
		err := handler(ctx, actual)
		results <- err
		return err
	})
	info, err := executor.Enqueue(task, asynq.MaxRetry(0))
	require.NoError(t, err)
	require.Equal(t, machineQueueTaskID(t, task), info.ID,
		"the actual Lite execution must preserve the emitted queue identity")
	select {
	case err := <-results:
		return err
	case <-time.After(30 * time.Second):
		t.Fatal("machine worker timed out")
		return context.DeadlineExceeded
	}
}

func TestNextcloudMachineProducerQueueWorkerRestrictedPipeline(t *testing.T) {
	f := service.NewMachineQueueFixture(t)
	k := f.Ingest(t)
	stages := map[string]int{}
	for i := 0; i < len(f.Tasks()); i++ {
		task := f.Tasks()[i]
		require.NoError(t, runMachineQueue(t, f, task), task.Type())
		stages[task.Type()]++
	}
	require.Positive(t, stages[types.TypeDocumentProcess])
	var diagnostic struct {
		ParseStatus  string
		ErrorMessage string
	}
	require.NoError(t, f.Database().Table("knowledges").Select("parse_status,error_message").Where("id = ?",
		k.ID).Take(&diagnostic).Error)
	require.Positive(t, stages[types.TypeKnowledgePostProcess], "controlled fixture state=%+v", diagnostic)
	require.Positive(t, stages[types.TypeSummaryGeneration])
	require.Positive(t, f.ModelCalls())
	var state struct{ ParseStatus string }
	require.NoError(t, f.Database().Table("knowledges").Select("parse_status").Where("id = ?", k.ID).Take(&state).Error)
	require.Equal(t, types.ParseStatusCompleted, state.ParseStatus)
	require.Error(t, runMachineQueue(t, f, f.Tasks()[0]), "a completed machine receipt must not replay")
}

func TestNextcloudMachineQueueRejectsUnissuedAndChangedWork(t *testing.T) {
	for _, scenario := range []string{
		"payload", "type", "execution ID", "attempt", "tenant", "KB",
		"forged initiator", "missing receipt", "manager revoked",
	} {
		t.Run(scenario, func(t *testing.T) {
			f := service.NewMachineQueueFixture(t)
			f.Ingest(t)
			task := f.Tasks()[0]
			var fields map[string]any
			require.NoError(t, json.Unmarshal(task.Payload(), &fields))
			kind := task.Type()
			switch scenario {
			case "payload":
				fields["file_name"] = "changed.md"
			case "type":
				kind = types.TypeKnowledgePostProcess
			case "execution ID":
				fields["_nextcloud_machine_task"].(map[string]any)["task_id"] = "forged-execution"
			case "attempt":
				fields["attempt"] = float64(99)
			case "tenant":
				fields["tenant_id"] = float64(8)
			case "KB":
				fields["knowledge_base_id"] = "unrelated"
			case "forged initiator":
				fields["initiator"] = map[string]any{"user_id": "enrich-human", "tenant_id": float64(7)}
			case "missing receipt":
				delete(fields, "_nextcloud_machine_task")
			case "manager revoked":
				f.RevokeManager()
			}
			raw, err := json.Marshal(fields)
			require.NoError(t, err)
			changed := asynq.NewTask(kind, raw)
			if scenario == "missing receipt" {
				handler := router.WrapKnowledgeBuildTask(
					repository.NewNextcloudContentLeaseStore(f.Database()),
					f.KnowledgeRepository(),
					f.ChunkRepository(),
					f.KnowledgeBases(),
					f.Groups(),
					f.Tracker(),
					f.Inputs(),
					f.Handler(kind),
				)
				err = handler(types.WithBackgroundTask(context.Background()), changed)
			} else {
				err = runMachineQueue(t, f, changed)
			}
			require.Error(t, err)
			require.Zero(t, f.ModelCalls())
		})
	}
}
