//go:build sqlite_fts5

package service_test

import (
	"encoding/json"
	"testing"

	"github.com/Tencent/WeKnora/internal/application/service"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/hibiken/asynq"
	"github.com/stretchr/testify/require"
)

func TestNextcloudMachineTableUsesOriginalFileAndLineage(t *testing.T) {
	f := service.NewMachineTableQueueFixture(t)
	k := f.Ingest(t)
	stages := map[string]int{}
	for i := 0; i < len(f.Tasks()); i++ {
		task := f.Tasks()[i]
		require.NoError(t, runMachineQueue(t, f, task), task.Type())
		stages[task.Type()]++
	}
	var diagnostic struct{ ParseStatus, ErrorMessage string }
	require.NoError(t, f.Database().Table("knowledges").Select("parse_status,error_message").Where("id = ?", k.ID).
		Take(&diagnostic).Error)
	require.Equal(t, 1, stages[types.TypeDataTableSummary], "controlled fixture state=%+v", diagnostic)
	require.GreaterOrEqual(t, f.ModelCalls(), 2)
	var chunks []types.Chunk
	require.NoError(t, f.Database().Where("knowledge_id = ? AND chunk_type IN ?", k.ID,
		[]types.ChunkType{types.ChunkTypeTableSummary, types.ChunkTypeTableColumn}).Find(&chunks).Error)
	require.Len(t, chunks, 2)
	for _, chunk := range chunks {
		var lineage struct{ SourceLineage string }
		require.NoError(t, f.Database().Table("knowledge_input_material_receipts r").Select("a.source_lineage").
			Joins("JOIN knowledge_input_admissions a ON a.admission_id = r.admission_id").
			Where("r.row_kind = 'chunk' AND r.row_id = ?", chunk.ID).Take(&lineage).Error)
		var source types.SourceLineage
		require.NoError(t, json.Unmarshal([]byte(lineage.SourceLineage), &source))
		require.NoError(t, source.RequireComplete())
		require.NotEmpty(t, source.Sources, "table model output cannot become source-free vector material")
		require.Equal(t, k.ID, source.Sources[0].KnowledgeID)
		var vectors int64
		require.NoError(t, f.Database().Table("lite_embeddings").Where("chunk_id = ? AND dimension = 2", chunk.ID).
			Count(&vectors).Error)
		require.EqualValues(t, 1, vectors, "table output must reach the actual SQLite vector transaction")
	}
	require.Positive(t, f.EmbeddingCalls())
}

func TestNextcloudMachineTableRejectsChangedFileModelsAndAuthority(t *testing.T) {
	for _, scenario := range []string{"file bytes", "summary model", "embedding model", "manager during model"} {
		t.Run(scenario, func(t *testing.T) {
			f := service.NewMachineTableQueueFixture(t)
			k := f.Ingest(t)
			require.NoError(t, runMachineQueue(t, f, f.Tasks()[0]))
			var task *asynq.Task
			for _, candidate := range f.Tasks() {
				if candidate.Type() == types.TypeDataTableSummary {
					task = candidate
					break
				}
			}
			require.NotNil(t, task)
			switch scenario {
			case "file bytes":
				f.TamperTableArtifact(t)
			case "summary model", "embedding model":
				id := "summary"
				if scenario == "embedding model" {
					id = "embed"
				}
				require.NoError(
					t,
					f.Database().
						Model(&types.Model{}).
						Where("id = ?", id).
						Update("name", "changed current model").
						Error,
				)
			case "manager during model":
				f.BeforeSummaryModelReturn(f.RevokeManager)
			}
			require.Error(t, runMachineQueue(t, f, task))
			var count int64
			require.NoError(t, f.Database().Model(&types.Chunk{}).Where("knowledge_id = ? AND chunk_type IN ?", k.ID,
				[]types.ChunkType{types.ChunkTypeTableSummary, types.ChunkTypeTableColumn}).Count(&count).Error)
			require.Zero(t, count, "refused file/model/manager input must not publish table chunks or vectors")
			if scenario == "manager during model" {
				require.Equal(t, 1, f.ModelCalls())
			} else {
				require.Zero(t, f.ModelCalls())
			}
		})
	}
}

func TestNextcloudMachineTableRejectsEmbeddingConfigurationChangeDuringProvider(t *testing.T) {
	f := service.NewMachineTableQueueFixture(t)
	k := f.Ingest(t)
	require.NoError(t, runMachineQueue(t, f, f.Tasks()[0]))
	var task *asynq.Task
	for _, candidate := range f.Tasks() {
		if candidate.Type() == types.TypeDataTableSummary {
			task = candidate
			break
		}
	}
	require.NotNil(t, task)
	priorCalls := f.EmbeddingCalls()
	f.BeforeEmbeddingReturn(func() {
		require.NoError(t, f.Database().Model(&types.Model{}).Where("id = ?", "embed").
			Update("name", "changed during provider").Error)
	})
	require.Error(t, runMachineQueue(t, f, task))
	require.Equal(
		t,
		priorCalls+1,
		f.EmbeddingCalls(),
		"a failed post-provider checkpoint must prevent further provider IO",
	)
	var vectors int64
	require.NoError(t, f.Database().Table("lite_embeddings e").
		Joins("JOIN chunks c ON c.id = e.chunk_id").Where("c.knowledge_id = ? AND c.chunk_type IN ?", k.ID,
		[]types.ChunkType{types.ChunkTypeTableSummary, types.ChunkTypeTableColumn}).Count(&vectors).Error)
	require.Zero(t, vectors, "changed embedding controls must never commit table vectors")
}

func TestNextcloudMachineTableProducerReentryBindsNewProofAndRejectsTampering(t *testing.T) {
	f := service.NewMachineTableQueueFixture(t)
	f.RepeatTableProducerAfterDocument()
	f.Ingest(t)
	require.NoError(t, runMachineQueue(t, f, f.Tasks()[0]))
	var tables []*asynq.Task
	for _, candidate := range f.Tasks() {
		if candidate.Type() == types.TypeDataTableSummary {
			tables = append(tables, candidate)
		}
	}
	require.Len(t, tables, 2, "the real producer may issue a fresh proof during parent reentry")
	var proofs [2]service.DataTableSummaryPayload
	for i, task := range tables {
		require.NoError(t, json.Unmarshal(task.Payload(), &proofs[i]))
	}
	require.NotEqual(t, proofs[0].ProducerInput.ID, proofs[1].ProducerInput.ID)
	require.Equal(t, proofs[0].OriginalInput.AdmissionID, proofs[1].OriginalInput.AdmissionID)
	require.Equal(t, proofs[0].Attempt, proofs[1].Attempt)
	require.NotEqual(t, machineQueueTaskID(t, tables[0]), machineQueueTaskID(t, tables[1]))
	var altered map[string]any
	require.NoError(t, json.Unmarshal(tables[1].Payload(), &altered))
	altered["artifact_sha256"] = "changed after producer admission"
	raw, err := json.Marshal(altered)
	require.NoError(t, err)
	require.Error(t, runMachineQueue(t, f, asynq.NewTask(types.TypeDataTableSummary, raw)))
	require.NoError(t, runMachineQueue(t, f, tables[1]),
		"a forged delivery must not invalidate the original newly issued immutable task")
}
