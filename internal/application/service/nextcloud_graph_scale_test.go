package service

import (
	"context"
	"encoding/json"
	"fmt"
	gormlogger "gorm.io/gorm/logger"
	"os"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
)

// This opt-in workload uses small actual plaintext artifacts, a controlled
// source responder and deterministic model. All 10k file heads, worker input
// proofs, chunks, graph writes, task finalizers and outbox states are real SQL.
// Original 100GB transfer and live NC/LDAP costs are measured separately.
func TestNextcloudControlledTenThousandFileProcessingPipeline(t *testing.T) {
	value := os.Getenv("WEKNORA_GRAPH_PIPELINE_SCALE_FILES")
	if value == "" {
		t.Skip("explicit graph pipeline workload required")
	}
	count, err := strconv.Atoi(value)
	require.NoError(t, err)
	require.True(t, count > 0 && count <= 10000)
	db, inputs, state, provider, _, _, initial := scopedGraphPipelineFixture(t, true)
	db.Logger = db.Logger.LogMode(gormlogger.Silent)
	repo := repository.NewKnowledgeRepository(db)
	leases := repository.NewNextcloudContentLeaseStore(db)
	model := &graphEntryModel{}
	started := time.Now()
	var latencies []time.Duration
	for i := 0; i < count; i++ {
		one := time.Now()
		id := fmt.Sprintf("scale-file-%d", i)
		file := strconv.Itoa(1000 + i)
		external := "nextcloud:instance:" + file
		body := []byte(fmt.Sprintf("Synthetic source file %d: Alice knows Bob. This plaintext is actually admitted, chunked and processed.", i))
		metadata, _ := json.Marshal(map[string]string{"datasource_id": "ds-77", "external_id": external, "nextcloud_instance_id": "instance", "nextcloud_binding_id": "binding-77", "nextcloud_file_id": file, "source_resource_id": "binding-77", "nextcloud_path": id + ".md", "nextcloud_etag": ""})
		doc := &types.Knowledge{ID: id, TenantID: 7, KnowledgeBaseID: initial.KnowledgeBaseID, Title: id, FileName: id + ".md", Type: "file", Channel: types.ConnectorTypeNextcloud, Metadata: metadata, ParseStatus: types.ParseStatusFinalizing, EnableStatus: "enabled", PendingSubtasksCount: 1}
		require.NoError(t, db.Create(doc).Error)
		require.NoError(t, repo.(interface {
			StageNextcloudVersion(context.Context, uint64, string, string, string, string, string) error
		}).StageNextcloudVersion(context.Background(), 7, doc.KnowledgeBaseID, "ds-77", external, "scale-etag-"+file, id))
		require.NoError(t, db.Where("id=?", id).Take(doc).Error)
		scope := repository.NextcloudContentScope{TenantID: 7, KnowledgeBaseID: doc.KnowledgeBaseID, KnowledgeID: id, DataSourceID: "ds-77", ExternalID: external}
		lease, err := leases.AcquireKnowledge(context.Background(), scope, repository.NextcloudContentBuildLease, "scale-parser", time.Minute)
		require.NoError(t, err)
		ctx := repository.WithNextcloudBuildLease(context.Background(), leases, lease, scope)
		proof, err := inputs.AdmitNextcloudBuildInputs(ctx, body)
		require.NoError(t, err)
		ctx = repository.WithKnowledgeBuildInputProof(ctx, inputs, proof)
		require.NoError(t, repository.RecordNextcloudDerivedBackendSelection(ctx, nil))
		chunk := &types.Chunk{ID: id + "-chunk", TenantID: 7, KnowledgeBaseID: doc.KnowledgeBaseID, KnowledgeID: id, Content: string(body), ChunkType: types.ChunkTypeText, IsEnabled: true, IndexStatus: "ready", Metadata: types.JSON(`{}`)}
		require.NoError(t, repository.NewChunkRepository(db).CreateChunks(ctx, []*types.Chunk{chunk}))
		tracker := NewSpanTracker(repository.NewKnowledgeSpanRepository(db), db)
		_, attempt, openErr := tracker.OpenAttempt(ctx, id, "controlled-text-parser")
		require.NoError(t, openErr)
		require.Equal(t, 1, attempt)
		for _, stage := range []string{types.StageDocReader, types.StageChunking, types.StageEmbedding} {
			span := tracker.BeginStage(ctx, id, 1, stage, nil)
			if stage == types.StageEmbedding {
				tracker.SkipSpan(ctx, span, "keyword-only controlled fixture")
			} else {
				tracker.EndSpan(ctx, span, nil)
			}
		}
		require.NoError(t, state.PlanGraph(ctx, doc, 1, []*types.Chunk{chunk}, true, true))
		snapshot, err := inputs.CaptureKnowledgeBuildInputs(ctx, types.KnowledgeBuildInputCaptureRequest{Projection: types.KnowledgeBuildProjectionGraph, TenantID: 7, KnowledgeBaseID: doc.KnowledgeBaseID, KnowledgeID: id, ChunkIDs: []string{chunk.ID}})
		require.NoError(t, err)
		require.NoError(t, pipelineExtractTask(t, db, inputs, state, provider, ctx, snapshot.Proof, doc, model))
		require.NoError(t, leases.ReleaseLease(context.Background(), lease.ID))
		latencies = append(latencies, time.Since(one))
		if (i+1)%1000 == 0 {
			t.Logf("processed=%d elapsed_ms=%d", i+1, time.Since(started).Milliseconds())
		}
	}
	elapsed := time.Since(started)
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	t.Logf("p95_file_processing_ms=%d", latencies[(count*95+99)/100-1].Milliseconds())
	require.Equal(t, count, model.calls)
	var records, acks, published, states, resources int64
	require.NoError(t, db.Table("nextcloud_graph_records").Where("knowledge_id LIKE 'scale-file-%'").Count(&records).Error)
	require.EqualValues(t, count, records)
	require.NoError(t, db.Table("nextcloud_graph_tasks").Where("knowledge_id LIKE 'scale-file-%' AND state='succeeded' AND persisted=TRUE AND finalized=TRUE").Count(&acks).Error)
	require.EqualValues(t, count, acks)
	require.NoError(t, db.Table("nextcloud_source_versions").Where("candidate_knowledge_id LIKE 'scale-file-%' AND state='published'").Count(&published).Error)
	require.EqualValues(t, count, published)
	require.NoError(t, db.Table("nextcloud_derived_write_artifacts").Where("knowledge_id LIKE 'scale-file-%' AND item_kind='scoped_sql_graph'").Count(&resources).Error)
	require.EqualValues(t, count, resources)
	require.NoError(t, db.Table("nextcloud_file_states").Where("connection_id=? AND CAST(file_id AS INTEGER)>=1000 AND CAST(file_id AS INTEGER)<? AND json_extract(payload_json,'$.publication_state')='published' AND json_extract(payload_json,'$.graph.persisted')=1", "pipeline-connection-0001", 1000+count).Count(&states).Error)
	require.EqualValues(t, count, states)
	t.Logf("real_file_heads=%d real_chunk_extract_calls=%d real_graph_records=%d durable_task_acks=%d exact_graph_resources=%d published=%d elapsed_ms=%d files_per_second=%.2f; controlled small plaintext/source/model; embedding explicitly skipped; excludes liveNC/LDAP/remoteLLM/100GB source transfer", count, model.calls, records, acks, resources, published, elapsed.Milliseconds(), float64(count)/elapsed.Seconds())
}
