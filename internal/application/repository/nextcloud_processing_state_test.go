package repository

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	nextcloud "github.com/Tencent/WeKnora/internal/datasource/connector/nextcloud"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/utils"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

func processingStateFixture(
	t *testing.T,
	required bool,
) (
	*NextcloudProcessingStateStore,
	*gorm.DB,
	context.Context,
	*types.Knowledge,
	*types.Chunk,
	*KnowledgeInputProvenanceStore,
) {
	t.Helper()
	return processingStateFixtureBackend(t, required, false)
}

func processingStateFixtureBackend(
	t *testing.T,
	required, pg bool,
) (
	*NextcloudProcessingStateStore,
	*gorm.DB,
	context.Context,
	*types.Knowledge,
	*types.Chunk,
	*KnowledgeInputProvenanceStore,
) {
	t.Helper()
	var inputs *KnowledgeInputProvenanceStore
	var db *gorm.DB
	if pg {
		inputs, db = knowledgeInputPostgresFixture(t)
	} else {
		inputs, db = knowledgeInputFixture(t)
	}
	candidateID := "candidate"
	if pg {
		candidateID = "doc-a"
	}

	base := &types.KnowledgeBase{
		ID:               "kb",
		TenantID:         7,
		Name:             "source",
		EmbeddingModelID: "embed",
		SummaryModelID:   "summary",
		IndexingStrategy: types.IndexingStrategy{GraphEnabled: true, GraphRequired: required},
		ExtractConfig:    &types.ExtractConfig{Enabled: true, Tags: []string{"KNOWS"}},
	}
	require.NoError(
		t,
		inputs.ReplaceKnowledgeBaseConfigurationWithInput(WithIndependentKnowledgeInput(context.Background()), base),
	)
	// The shared input fixture already installs the actual current control schema.
	require.NoError(t, db.AutoMigrate(&types.AuthToken{}))
	var ds types.DataSource
	require.NoError(t, db.Where("id='ds'").Take(&ds).Error)
	baseURL, sha, binding, err := NextcloudEventDataSourceIdentity(ds.Config)
	require.NoError(t, err)
	encrypted, err := utils.EncryptAESGCM("0123456789abcdef0123456789abcdef", utils.GetAESKey())
	require.NoError(t, err)
	require.NoError(
		t,
		db.Table("nextcloud_event_connections").
			Create(map[string]any{
				"connection_id":             "connection-0000001",
				"tenant_id":                 7,
				"knowledge_base_id":         "kb",
				"datasource_id":             "ds",
				"nextcloud_instance_id":     "instance",
				"binding_id":                binding,
				"datasource_base_url":       baseURL,
				"datasource_config_sha256":  sha,
				"status":                    "active",
				"current_key_id":            "current",
				"current_secret_ciphertext": encrypted,
			}).
			Error,
	)
	repo := NewKnowledgeRepository(db).(*knowledgeRepository)
	require.NoError(
		t,
		repo.StageNextcloudVersion(
			context.Background(),
			7,
			"kb",
			"ds",
			"nextcloud:instance:77",
			"original-etag",
			candidateID,
		),
	)
	var k types.Knowledge
	require.NoError(t, db.Where("id=?", candidateID).Take(&k).Error)
	require.NoError(
		t,
		db.Model(&k).
			Updates(map[string]any{
				"parse_status":           types.ParseStatusFinalizing,
				"enable_status":          "disabled",
				"pending_subtasks_count": 1,
				"file_name":              "file.md",
				"title":                  "file.md",
				"file_hash":              "test-file-hash",
				"file_type":              "md",
				"file_size":              20,
			}).
			Error,
	)
	require.NoError(t, db.Where("id=?", k.ID).Take(&k).Error)
	scope := NextcloudContentScope{
		TenantID:        7,
		KnowledgeBaseID: "kb",
		KnowledgeID:     k.ID,
		DataSourceID:    "ds",
		ExternalID:      "nextcloud:instance:77",
	}
	leases := NewNextcloudContentLeaseStore(db)
	lease, err := leases.AcquireKnowledge(
		context.Background(),
		scope,
		NextcloudContentBuildLease,
		"actual-test-worker",
		time.Minute,
	)
	require.NoError(t, err)
	ctx := WithNextcloudBuildLease(context.Background(), leases, lease, scope)
	proof, err := inputs.AdmitNextcloudBuildInputs(ctx, []byte("real source artifact"))
	require.NoError(t, err)
	ctx = WithKnowledgeBuildInputProof(ctx, inputs, proof)
	chunk := &types.Chunk{
		ID:              "graph-source-chunk",
		TenantID:        7,
		KnowledgeBaseID: "kb",
		KnowledgeID:     k.ID,
		Content:         "Alice knows Bob",
		ChunkType:       types.ChunkTypeText,
		IsEnabled:       true,
		IndexStatus:     "ready",
		Metadata:        types.JSON(`{}`),
	}
	require.NoError(t, NewChunkRepository(db).CreateChunks(ctx, []*types.Chunk{chunk}))
	now := time.Now().UTC()
	require.NoError(
		t,
		db.Create(
			&types.KnowledgeProcessingSpan{
				KnowledgeID: k.ID,
				Attempt:     1,
				SpanID:      "root",
				Name:        "root",
				Kind:        types.SpanKindRoot,
				Status:      types.SpanStatusRunning,
				StartedAt:   &now,
			},
		).Error,
	)
	state := NewNextcloudProcessingStateStore(db)
	require.NoError(t, state.PlanGraph(ctx, &k, 1, []*types.Chunk{chunk}, true, required))
	return state, db, ctx, &k, chunk, inputs
}

func boundProcessingGraph(
	ctx context.Context,
	t *testing.T,
	k *types.Knowledge,
	c *types.Chunk,
	inputs *KnowledgeInputProvenanceStore,
) (context.Context, *types.GraphData) {
	t.Helper()
	snap, err := inputs.CaptureKnowledgeBuildInputs(
		ctx,
		types.KnowledgeBuildInputCaptureRequest{
			Projection:      types.KnowledgeBuildProjectionGraph,
			TenantID:        7,
			KnowledgeBaseID: "kb",
			KnowledgeID:     k.ID,
			ChunkIDs:        []string{c.ID},
		},
	)
	require.NoError(t, err)
	ctx = WithKnowledgeBuildInputProof(ctx, inputs, snap.Proof)
	namespace := types.NameSpace{KnowledgeBase: "kb", Knowledge: k.ID}
	graph := &types.GraphData{
		WireDigest:    types.GraphDigest("actual model wire"),
		SourceLineage: snap.Proof.SourceLineage,
		Node: []*types.GraphNode{
			{Name: "Alice", Chunks: []string{c.ID}, Attributes: []string{"person"}},
			{Name: "Bob", Chunks: []string{c.ID}, Attributes: []string{"person"}},
		},
		Relation: []*types.GraphRelation{{Node1: "Alice", Node2: "Bob", Type: "KNOWS"}},
	}
	receipt := &types.GraphSourceReceipt{
		OriginalInput:   snap.Proof,
		Version:         1,
		TenantID:        7,
		KnowledgeBaseID: "kb",
		KnowledgeID:     k.ID,
		BuildGeneration: snap.Proof.BuildGeneration,
		InputDigest:     snap.Proof.InputDigest,
		ConfigDigest:    types.GraphDigest("model configuration"),
		WireDigest:      graph.WireDigest,
		SourceLineage:   graph.SourceLineage,
	}
	for _, n := range graph.Node {
		n.SourceReceipt, err = types.BindGraphSourceReceipt(receipt, types.GraphNodeBodyDigest(namespace, n))
		require.NoError(t, err)
	}
	for _, e := range graph.Relation {
		e.SourceReceipt, err = types.BindGraphSourceReceipt(receipt, types.GraphRelationBodyDigest(namespace, e))
		require.NoError(t, err)
	}
	graph.SourceReceipt, err = types.BindGraphSourceReceipt(receipt, types.GraphDataBodyDigest(namespace, graph))
	require.NoError(t, err)
	ctx = types.WithGraphWriteOperation(
		ctx,
		types.GraphWriteOperation{ID: NextcloudGraphOperationID(7, k.ID, 1, c.ID), Attempt: 1, ChunkID: c.ID},
	)
	ctx = types.WithGraphWriteAuthorization(ctx, func(ctx context.Context, receipt *types.GraphSourceReceipt) error {
		return inputs.RecheckKnowledgeBuildInputs(ctx, receipt.OriginalInput)
	})
	return ctx, graph
}

func loadProcessingState(t *testing.T, db *gorm.DB) NextcloudFileStateV2 {
	t.Helper()
	var row nextcloudFileStateRow
	require.NoError(t, db.Where("connection_id='connection-0000001'").Order("publication_epoch DESC").Take(&row).Error)
	var out NextcloudFileStateV2
	require.NoError(t, json.Unmarshal([]byte(row.PayloadJSON), &out))
	return out
}

func TestNextcloudGraphActualSQLProviderDurableStateAndIdempotentTask(t *testing.T) {
	state, db, ctx, k, chunk, inputs := processingStateFixture(t, true)
	done, err := state.StartGraph(ctx, 7, k.ID, 1, chunk.ID)
	require.NoError(t, err)
	require.False(t, done)
	ctx, graph := boundProcessingGraph(ctx, t, k, chunk, inputs)
	provider := NewScopedSQLGraphRepository(db, nil)
	require.NoError(
		t,
		provider.AddGraph(ctx, types.NameSpace{KnowledgeBase: "kb", Knowledge: k.ID}, []*types.GraphData{graph}),
	)
	status := loadProcessingState(t, db)
	require.Equal(t, "succeeded", status.Graph.State)
	require.Equal(t, 1, status.Graph.Expected)
	require.Equal(t, 1, status.Graph.Succeeded)
	require.True(t, status.Graph.Persisted)
	require.NotNil(t, status.LastVerifiedAt)
	graphRead, err := provider.SearchNode(ctx, types.NameSpace{KnowledgeBase: "kb", Knowledge: k.ID}, []string{"Alice"})
	require.NoError(t, err)
	require.Len(t, graphRead.Node, 2)
	require.Len(t, graphRead.Relation, 1)
	require.NoError(t, graphRead.SourceLineage.RequireComplete())
	require.NoError(
		t,
		provider.AddGraph(ctx, types.NameSpace{KnowledgeBase: "kb", Knowledge: k.ID}, []*types.GraphData{graph}),
	)
	done, err = state.StartGraph(ctx, 7, k.ID, 1, chunk.ID)
	require.NoError(t, err)
	require.True(t, done, "ACK lost must reuse persisted op rather than regenerate nodes")
	var n int64
	require.NoError(t, db.Model(&nextcloudGraphRecord{}).Count(&n).Error)
	require.EqualValues(t, 1, n)
	require.Error(
		t,
		db.Model(&nextcloudGraphRecord{}).
			Where("operation_id=?", NextcloudGraphOperationID(7, k.ID, 1, chunk.ID)).
			Update("body_json", "{}").
			Error,
		"target records are immutable",
	)
	require.Error(
		t,
		provider.DelGraph(ctx, []types.NameSpace{{KnowledgeBase: "kb", Knowledge: k.ID}}),
		"unscoped deletion cannot bypass actual GC claim",
	)
	ready, err := nextcloudRequiredGraphReady(db, k)
	require.NoError(t, err)
	require.True(t, ready)
}

func TestNextcloudGraphActualSQLRetirementAndAttemptFence(t *testing.T) {
	for _, change := range []string{"retire", "attempt", "config", "lease"} {
		t.Run(change, func(t *testing.T) {
			state, db, ctx, k, chunk, inputs := processingStateFixture(t, false)
			_, err := state.StartGraph(ctx, 7, k.ID, 1, chunk.ID)
			require.NoError(t, err)
			ctx, graph := boundProcessingGraph(ctx, t, k, chunk, inputs)
			switch change {
			case "retire":
				require.NoError(
					t,
					NewNextcloudContentLeaseStore(
						db,
					).RetireKnowledge(context.Background(), NextcloudContentScope{
						TenantID:        7,
						KnowledgeBaseID: "kb",
						KnowledgeID:     k.ID,
						DataSourceID:    "ds",
						ExternalID:      "nextcloud:instance:77",
					}),
				)
			case "attempt":
				require.NoError(
					t,
					db.Create(
						&types.KnowledgeProcessingSpan{
							KnowledgeID: k.ID,
							Attempt:     2,
							SpanID:      "root-new",
							Name:        "root",
							Kind:        types.SpanKindRoot,
							Status:      types.SpanStatusRunning,
						},
					).Error,
				)
			case "config":
				require.NoError(
					t,
					db.Exec("UPDATE knowledge_bases SET summary_model_id='new-model' WHERE id='kb'").Error,
				)
			case "lease":
				require.NoError(
					t,
					db.Exec("UPDATE nextcloud_content_leases SET expires_at_ms=1 WHERE kind='build'").Error,
				)
			}
			require.Error(
				t,
				NewScopedSQLGraphRepository(
					db,
					nil,
				).AddGraph(ctx, types.NameSpace{KnowledgeBase: "kb", Knowledge: k.ID}, []*types.GraphData{graph}),
			)
			var n int64
			require.NoError(t, db.Model(&nextcloudGraphRecord{}).Count(&n).Error)
			require.Zero(t, n)
		})
	}
}

func TestNextcloudStateOutboxRevisionFreshnessAndSupersededDelivery(t *testing.T) {
	state, db, ctx, k, chunk, _ := processingStateFixture(t, false)
	before := loadProcessingState(t, db)
	require.NoError(t, state.Capture(ctx, 7, "kb", "ds", "nextcloud:instance:77", true))
	first := loadProcessingState(t, db)
	require.Equal(t, "nextcloud:instance:77", first.ExternalID)
	require.NotNil(t, first.CandidateKnowledgeID)
	require.Equal(t, k.ID, *first.CandidateKnowledgeID)
	require.NotEmpty(t, first.EffectiveConfigDigest)
	require.NoError(t, state.Capture(ctx, 7, "kb", "ds", "nextcloud:instance:77", true))
	second := loadProcessingState(t, db)
	require.Equal(t, first.EffectiveConfigDigest, second.EffectiveConfigDigest)
	require.Equal(t, first.StatusRevision, second.StatusRevision)
	require.Equal(t, first.HeadRevision, second.HeadRevision)
	require.NotEqual(t, first.EventID, second.EventID)
	require.NoError(t, state.FailGraph(ctx, 7, k.ID, 1, chunk.ID, "graph_enqueue_failed", true))
	failure := loadProcessingState(t, db)
	require.Equal(t, "failed", failure.Graph.State)
	require.Greater(t, failure.StatusRevision, before.StatusRevision)
	delivery, err := state.PrepareDelivery(ctx, first.EventID)
	require.NoError(t, err)
	require.Empty(t, delivery.EventID)
	var out nextcloudStatusOutboxRow
	require.NoError(t, db.Where("event_id=?", first.EventID).Take(&out).Error)
	require.Equal(t, "superseded", out.State)
	ready, err := nextcloudRequiredGraphReady(db, k)
	require.NoError(t, err)
	require.True(t, ready, "optional graph failure keeps document publication independent")
	require.NoError(
		t,
		db.Exec("UPDATE nextcloud_graph_tasks SET state='succeeded',persisted=TRUE WHERE knowledge_id=?", k.ID).Error,
	)
	fake := loadProcessingState(t, db)
	require.Equal(t, "failed", fake.Graph.State, "unsent generic row mutation does not mint state")
	require.NoError(t, state.Capture(ctx, 7, "kb", "ds", "nextcloud:instance:77", true))
	verified := loadProcessingState(t, db)
	require.Equal(
		t,
		"unverified",
		verified.Graph.State,
		"counter success without actual provider ACK cannot report success",
	)
	require.False(t, verified.Graph.Persisted)
}

func TestNextcloudStateV2CrossLanguageSigningFixture(t *testing.T) {
	raw, err := os.ReadFile("testdata/nextcloud-stage-status-v2-signatures.json")
	require.NoError(t, err)
	var v map[string]string
	require.NoError(t, json.Unmarshal(raw, &v))
	key := NextcloudStateSigningKey([]byte(v["secret"]))
	require.Equal(t, v["derived_key_hex"], hex.EncodeToString(key))
	sum := sha256.Sum256([]byte(v["body"]))
	signature := func(s string) string {
		mac := hmac.New(sha256.New, key)
		mac.Write([]byte(s))
		return hex.EncodeToString(mac.Sum(nil))
	}
	require.Equal(
		t,
		v["response_signature"],
		signature(
			string(
				NextcloudFileStateResponseCanonical(
					SignedNextcloudFileStatusRequest{
						SignedNextcloudEventStatusRequest: SignedNextcloudEventStatusRequest{
							ConnectionID: "connection-0000001",
							KeyID:        v["key_id"],
							Nonce:        v["nonce"],
						},
						RequestSignature: v["request_signature"],
					},
					[]byte(v["body"]),
				),
			),
		),
	)
	require.Equal(
		t,
		v["push_signature"],
		signature(
			string(
				nextcloud.StatePushCanonical(
					v["push_path"],
					hex.EncodeToString(sum[:]),
					v["timestamp"],
					v["nonce"],
					"connection-0000001",
					v["key_id"],
				),
			),
		),
	)
}

func TestNextcloudSameETagHigherContentRevisionCannotReuseCandidate(t *testing.T) {
	_, db, ctx, k, _, _ := processingStateFixture(t, false)
	params := &types.KnowledgeCheckParams{
		Type:                     "file",
		FileHash:                 "test-file-hash",
		FileType:                 "md",
		DataSourceID:             "ds",
		ExternalID:               "nextcloud:instance:77",
		NextcloudTargetETag:      "original-etag",
		NextcloudContentRevision: "0",
	}
	exists, _, err := NewKnowledgeRepository(db).CheckKnowledgeExists(ctx, 7, "kb", params)
	require.NoError(t, err)
	require.True(t, exists)
	params.NextcloudContentRevision = "1"
	exists, duplicate, err := NewKnowledgeRepository(db).CheckKnowledgeExists(ctx, 7, "kb", params)
	require.NoError(t, err)
	require.False(t, exists)
	require.Nil(t, duplicate, "content write revision advances even for same ETag and identical byte hash")
	var old nextcloudSourceVersion
	require.NoError(t, db.Where("candidate_knowledge_id=?", k.ID).Take(&old).Error)
	require.Equal(t, "0", old.DesiredContentRevision)
}

func TestNextcloudBatchDeliveryFinalCurrentHeadAndLostACK(t *testing.T) {
	state, db, ctx, k, chunk, _ := processingStateFixture(t, false)
	require.NoError(t, state.Capture(ctx, 7, "kb", "ds", "nextcloud:instance:77", true))
	first := loadProcessingState(t, db)
	batch, err := state.PrepareDeliveryBatch(ctx, []string{first.EventID})
	require.NoError(t, err)
	require.Len(t, batch, 1)
	current, err := state.DeliveryCurrent(ctx, batch[0])
	require.NoError(t, err)
	require.True(t, current)
	// A persisted receiver ACK lost before our local commit leaves the exact
	// event/body pending and reusable. No replacement ID is minted by retries.
	again, err := state.PrepareDeliveryBatch(ctx, []string{first.EventID})
	require.NoError(t, err)
	require.Len(t, again, 1)
	require.Equal(t, batch[0].Body, again[0].Body)
	require.Equal(t, first.EventID, again[0].EventID)
	require.NoError(t, state.FailGraph(ctx, 7, k.ID, 1, chunk.ID, "graph_enqueue_failed", true))
	current, err = state.DeliveryCurrent(ctx, batch[0])
	require.NoError(t, err)
	require.False(t, current)
	require.NoError(t, state.AcknowledgeCurrentDeliveries(ctx, []string{first.EventID}, time.Now()))
	var old nextcloudStatusOutboxRow
	require.NoError(t, db.Where("event_id=?", first.EventID).Take(&old).Error)
	require.Equal(t, "superseded", old.State)
	latest := loadProcessingState(t, db)
	require.NoError(t, state.AcknowledgeCurrentDeliveries(ctx, []string{latest.EventID}, time.Now()))
	old = nextcloudStatusOutboxRow{}
	require.NoError(t, db.Where("event_id=?", latest.EventID).Take(&old).Error)
	require.Equal(t, "delivered", old.State)
}

func TestNextcloudPlannedGraphPayloadCrashRecoveryIsExactAndFenced(t *testing.T) {
	state, db, ctx, k, chunk, inputs := processingStateFixture(t, true)
	missing, err := state.UnpreparedGraphChunks(ctx, 7, k.ID, 1)
	require.NoError(t, err)
	require.Equal(t, []string{chunk.ID}, missing)
	snapshot, err := inputs.CaptureKnowledgeBuildInputs(
		ctx,
		types.KnowledgeBuildInputCaptureRequest{
			Projection:      types.KnowledgeBuildProjectionGraph,
			TenantID:        7,
			KnowledgeBaseID: "kb",
			KnowledgeID:     k.ID,
			ChunkIDs:        []string{chunk.ID},
		},
	)
	require.NoError(t, err)
	wire, err := json.Marshal(
		types.ExtractChunkPayload{
			TenantID:      7,
			KnowledgeID:   k.ID,
			Attempt:       1,
			ChunkID:       chunk.ID,
			ModelID:       "controlled",
			OriginalInput: snapshot.Proof,
		},
	)
	require.NoError(t, err)
	require.NoError(t, state.RecordGraphPayload(ctx, 7, k.ID, 1, chunk.ID, wire))
	// Restored stale rows precede this valid intent. The bounded page must
	// exclude their closed generations rather than repeatedly selecting them.
	var current nextcloudGraphTask
	require.NoError(t, db.Where("operation_id=?", NextcloudGraphOperationID(7, k.ID, 1, chunk.ID)).Take(&current).Error)
	for ordinal := 0; ordinal < 200; ordinal++ {
		old := current
		old.OperationID = "stale-op-" + strconv.Itoa(ordinal)
		old.ChunkID = "stale-chunk-" + strconv.Itoa(ordinal)
		old.BuildEpoch = current.BuildEpoch + 99
		old.CreatedAt = current.CreatedAt - 1
		require.NoError(t, db.Create(&old).Error)
	}
	missing, err = state.UnpreparedGraphChunks(ctx, 7, k.ID, 1)
	require.NoError(t, err)
	require.Empty(t, missing, "existing durable intent must not be rebuilt")
	intents, err := state.PendingGraphQueueIntents(ctx, time.Now())
	require.NoError(t, err)
	require.Len(t, intents, 1)
	require.Equal(t, wire, intents[0].Payload)
	require.NoError(
		t,
		NewNextcloudContentLeaseStore(
			db,
		).RetireKnowledge(context.Background(), NextcloudContentScope{
			TenantID:        7,
			KnowledgeBaseID: "kb",
			KnowledgeID:     k.ID,
			DataSourceID:    "ds",
			ExternalID:      "nextcloud:instance:77",
		}),
	)
	intents, err = state.PendingGraphQueueIntents(ctx, time.Now())
	require.NoError(t, err)
	require.Empty(t, intents, "old-generation queue replay stays closed")
}

func TestNextcloudActualGraphExactGCReceiptAndUnknownCoverage(t *testing.T) {
	state, db, ctx, k, chunk, inputs := processingStateFixture(t, true)
	_, err := state.StartGraph(ctx, 7, k.ID, 1, chunk.ID)
	require.NoError(t, err)
	ctx, graph := boundProcessingGraph(ctx, t, k, chunk, inputs)
	require.NoError(
		t,
		NewScopedSQLGraphRepository(
			db,
			nil,
		).AddGraph(ctx, types.NameSpace{KnowledgeBase: "kb", Knowledge: k.ID}, []*types.GraphData{graph}),
	)
	migration, err := os.ReadFile("../../../migrations/sqlite/000068_nextcloud_local_derived_gc.up.sql")
	require.NoError(t, err)
	require.NoError(t, db.Exec(string(migration)).Error)
	scope := NextcloudContentScope{
		TenantID:        7,
		KnowledgeBaseID: "kb",
		KnowledgeID:     k.ID,
		DataSourceID:    "ds",
		ExternalID:      "nextcloud:instance:77",
	}
	leases := NewNextcloudContentLeaseStore(db)
	require.NoError(
		t,
		NewKnowledgeRepository(db).(*knowledgeRepository).TombstoneNextcloudVersion(
			context.Background(),
			7,
			"kb",
			"ds",
			scope.ExternalID,
		),
	)
	notBefore := leaseTestNotBefore(t, db, scope)
	_, err = leases.ClaimKnowledgeGC(context.Background(), scope, notBefore, time.Minute)
	require.ErrorIs(
		t,
		err,
		ErrNextcloudContentGCUncovered,
		"recovery/unobserved coverage cannot authorize graph deletion",
	)
	// The test explicitly establishes covered/drained synthetic participants;
	// production never installs this marker as part of state or GC automation.
	leaseTestCoverKB(t, db, scope)
	_, err = leases.ClaimKnowledgeGC(context.Background(), scope, notBefore, time.Minute)
	require.ErrorIs(t, err, ErrNextcloudContentGCBusy, "the original build lease remains protected")
	guard := ctx.Value(nextcloudBuildContextKey{}).(nextcloudBuildContext)
	require.NoError(t, leases.ReleaseLease(context.Background(), guard.lease.ID))
	claim, err := leases.ClaimKnowledgeGC(context.Background(), scope, notBefore, time.Minute)
	require.NoError(t, err)
	job := nextcloudGCJob{
		ID:                "actual-graph-gc",
		Reason:            "tombstone",
		TenantID:          7,
		KnowledgeBaseID:   "kb",
		KnowledgeID:       k.ID,
		DataSourceID:      "ds",
		ExternalID:        scope.ExternalID,
		State:             "pending",
		NotBefore:         notBefore,
		OriginalNotBefore: notBefore,
	}
	require.NoError(t, db.Create(&job).Error)
	op := NextcloudGraphOperationID(7, k.ID, 1, chunk.ID)
	require.NoError(
		t,
		db.Create(&nextcloudGCItem{JobID: job.ID, Kind: "scoped_sql_graph", ObjectRef: op, State: "pending"}).Error,
	)
	gc := NewNextcloudGCStore(db)
	bad := job
	bad.KnowledgeID = "other-candidate"
	require.Error(t, db.Transaction(func(tx *gorm.DB) error {
		now, _ := nextcloudLeaseNowMS(tx)
		return gc.deleteScopedGraphInTx(tx, claim, bad, op, now)
	}))
	require.NoError(t, db.Transaction(func(tx *gorm.DB) error {
		now, _ := nextcloudLeaseNowMS(tx)
		return gc.deleteScopedGraphInTx(tx, claim, job, op, now)
	}))
	var records int64
	require.NoError(t, db.Table("nextcloud_graph_records").Count(&records).Error)
	require.Zero(t, records)
	var receipt nextcloudDerivedGCReceipt
	require.NoError(
		t,
		db.Where("job_id=? AND item_kind='scoped_sql_graph' AND object_ref=?", job.ID, op).Take(&receipt).Error,
	)
	require.EqualValues(t, 1, receipt.DeletedRows)
	require.Equal(t, claim.Token, receipt.ClaimToken)
	require.Equal(t, claim.Epoch, receipt.ClaimEpoch)
	require.NotEmpty(t, receipt.BeforeSHA256)
	require.NoError(t, db.Transaction(func(tx *gorm.DB) error {
		now, _ := nextcloudLeaseNowMS(tx)
		return gc.deleteScopedGraphInTx(tx, claim, job, op, now)
	}))
	var receipts int64
	require.NoError(t, db.Model(&nextcloudDerivedGCReceipt{}).Count(&receipts).Error)
	require.EqualValues(t, 1, receipts, "retry cannot fabricate a second physical deletion")
}

func TestNextcloudTombstoneStatusPreservesSourceIdentityAndAggregatesCleanup(t *testing.T) {
	state, db, ctx, k, _, _ := processingStateFixture(t, false)
	require.NoError(t, NewKnowledgeRepository(db).(*knowledgeRepository).TombstoneNextcloudVersion(
		context.Background(), 7, "kb", "ds", "nextcloud:instance:77",
	))
	now := time.Now().UTC()
	for _, job := range []nextcloudGCJob{
		{
			ID: "tombstone-old-candidate", Reason: "tombstone", TenantID: 7,
			KnowledgeBaseID: "kb", KnowledgeID: "candidate-old", DataSourceID: "ds",
			ExternalID: "nextcloud:instance:77", State: "collected",
			NotBefore: now, OriginalNotBefore: now, NextAttemptAt: now,
			CompletedAt: &now, CreatedAt: now, UpdatedAt: now,
		},
		{
			ID: "tombstone-current-candidate", Reason: "tombstone", TenantID: 7,
			KnowledgeBaseID: "kb", KnowledgeID: k.ID, DataSourceID: "ds",
			ExternalID: "nextcloud:instance:77", State: "pending",
			NotBefore: now, OriginalNotBefore: now, NextAttemptAt: now,
			CreatedAt: now, UpdatedAt: now,
		},
	} {
		require.NoError(t, db.Create(&job).Error)
	}

	require.NoError(t, state.Capture(ctx, 7, "kb", "ds", "nextcloud:instance:77", true))
	status := loadProcessingState(t, db)
	require.Equal(t, "withdrawn", status.SyncState)
	require.Equal(t, "withdrawn", status.PublicationState)
	require.Equal(t, "original-etag", status.SourceETag,
		"tombstone wire identity must remain eligible for Nextcloud cleanup-only status")
	require.Equal(t, "cleanup_partial", status.GCState)

	require.NoError(t, db.Model(&nextcloudGCJob{}).
		Where("id=?", "tombstone-current-candidate").
		Update("state", "collected").Error)
	require.NoError(t, state.Capture(ctx, 7, "kb", "ds", "nextcloud:instance:77", true))
	status = loadProcessingState(t, db)
	require.Equal(t, "cleanup_verified", status.GCState)
}

func TestNextcloudClosedRecoveryControlsCannotReportPublishedOrDeliverOldReady(t *testing.T) {
	state, db, ctx, k, chunk, inputs := processingStateFixture(t, true)
	_, err := state.StartGraph(ctx, 7, k.ID, 1, chunk.ID)
	require.NoError(t, err)
	ctx, graph := boundProcessingGraph(ctx, t, k, chunk, inputs)
	require.NoError(
		t,
		NewScopedSQLGraphRepository(
			db,
			nil,
		).AddGraph(ctx, types.NameSpace{KnowledgeBase: "kb", Knowledge: k.ID}, []*types.GraphData{graph}),
	)
	directoryDDL, err := os.ReadFile("../../../migrations/sqlite/000076_nextcloud_directory_migrations.up.sql")
	require.NoError(t, err)
	if !db.Migrator().HasTable("nextcloud_root_migrations") {
		require.NoError(t, db.Exec(string(directoryDDL)).Error)
	}
	script, err := os.ReadFile("../../../migrations/sqlite/000078_nextcloud_control_recovery.up.sql")
	require.NoError(t, err)
	if !db.Migrator().HasTable("nextcloud_control_recovery_gates") {
		require.NoError(t, db.Exec(string(script)).Error)
	}
	require.NoError(
		t,
		db.Create(
			&nextcloudControlRecoveryGate{
				InstanceID:      "instance",
				PlanSHA256:      strings.Repeat("a", 64),
				InventorySHA256: strings.Repeat("b", 64),
				Closed:          true,
				ClosedAt:        time.Now(),
			},
		).Error,
	)
	require.NoError(t, state.Capture(ctx, 7, "kb", "ds", "nextcloud:instance:77", true))
	status := loadProcessingState(t, db)
	require.Equal(t, "unverified", status.PublicationState)
	require.Nil(t, status.PublishedSourceETag)
	require.Equal(t, "publication_recovery_closed", status.ErrorCode)
	require.Equal(t, "succeeded", status.Graph.State)
	require.True(t, status.Graph.Persisted, "a stage result is independent of human publication")
	current, err := state.DeliveryCurrent(
		ctx,
		NextcloudStatusDelivery{State: NextcloudFileStateV2{InstanceID: "instance", PublicationState: "published"}},
	)
	require.NoError(t, err)
	require.False(t, current, "old ready event is fenced even before compensator mints new blocked metadata")
	var originalTask nextcloudGraphTask
	require.NoError(
		t,
		db.Where("operation_id=?", NextcloudGraphOperationID(7, k.ID, 1, chunk.ID)).Take(&originalTask).Error,
	)
	rawTask := originalTask
	rawTask.ChunkID = "raw-target-probe"
	rawTask.OperationID = NextcloudGraphOperationID(7, k.ID, 1, rawTask.ChunkID)
	rawTask.State = "running"
	rawTask.Persisted = false
	rawTask.Finalized = false
	require.NoError(t, db.Create(&rawTask).Error)
	lease, _, err := NextcloudBuildInputLease(ctx)
	require.NoError(t, err)
	rawRecord := nextcloudGraphRecord{
		OperationID:     rawTask.OperationID,
		TenantID:        7,
		KnowledgeBaseID: "kb",
		KnowledgeID:     k.ID,
		Backend:         NextcloudGraphBackend,
		Generation:      lease.Epoch,
		BuildLeaseID:    lease.ID,
		BodyJSON:        "{}",
		BodySHA256:      recoverySHA([]byte("{}")),
		NodeCount:       0,
		EdgeCount:       0,
		CommittedAt:     time.Now().Unix(),
	}
	require.ErrorContains(
		t,
		db.Create(&rawRecord).Error,
		"nextcloud_graph_recovery_permit_required",
		"actual provider target SQL rejects a previously valid lease without a bounded maintenance permit",
	)
}
