package service

import (
	"context"
	"encoding/json"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/application/repository"
	nextcloud "github.com/Tencent/WeKnora/internal/datasource/connector/nextcloud"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/utils"
	"github.com/stretchr/testify/require"
)

// The original bytes, source generation and durable HTTP ACK come from the
// owned NC/PHP/PG/LDAP fixture. Plain-text parsing and the model are controlled
// here; no remote embedding/vector service or real LLM load is claimed.
func TestNextcloudLiveOriginalExtractionProviderAndStatusInbox(t *testing.T) {
	path := os.Getenv("WEKNORA_NEXTCLOUD_GRAPH_LIVE_FIXTURE")
	if path == "" {
		t.Skip("owned two-system graph fixture required")
	}
	var f struct {
		Binding    string `json:"binding_id"`
		Key        string `json:"key_id"`
		Token      string `json:"token"`
		Instance   string `json:"instance_id"`
		File       string `json:"file_id"`
		Revision   string `json:"source_content_revision"`
		ETag       string `json:"source_etag"`
		Epoch      string `json:"publication_epoch"`
		Connection string `json:"connection_id"`
		Secret     string `json:"state_secret"`
		Checkpoint struct {
			Head   string `json:"head_revision"`
			Status string `json:"status_revision"`
		} `json:"sender_checkpoint"`
		Pair struct {
			Operation string `json:"operation_id"`
			Tenant    string `json:"tenant_id"`
			KB        string `json:"knowledge_base_id"`
			DS        string `json:"data_source_id"`
		} `json:"pair"`
	}
	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(raw, &f))
	require.Equal(t, "7", f.Pair.Tenant)
	t.Setenv("SYSTEM_AES_KEY", "0123456789abcdef0123456789abcdef")
	t.Setenv("WEKNORA_NEXTCLOUD_DEV_HTTP", "1")
	t.Setenv("WEKNORA_NEXTCLOUD_ALLOWED_ORIGINS", "http://nextcloud")
	cfg := &types.DataSourceConfig{
		Type:        types.ConnectorTypeNextcloud,
		ResourceIDs: []string{f.Binding},
		Settings:    map[string]any{"base_url": "http://nextcloud"},
		Credentials: map[string]any{"key_id": f.Key, "token": f.Token},
	}
	fetched, _, err := nextcloud.NewConnector().FetchIncremental(context.Background(), cfg, nil)
	require.NoError(t, err)
	var item *types.FetchedItem
	for i := range fetched {
		if fetched[i].ExternalID == "nextcloud:"+f.Instance+":"+f.File {
			item = &fetched[i]
			break
		}
	}
	require.NotNil(t, item)
	require.Equal(t, f.Revision, item.Metadata["nextcloud_content_revision"])
	require.Equal(t, f.ETag, item.Metadata["nextcloud_etag"])
	db := scopedGraphFixtureDatabase(t)
	inputs := repository.NewKnowledgeInputProvenanceStore(db)
	kb := &types.KnowledgeBase{
		ID:               f.Pair.KB,
		TenantID:         7,
		Name:             "owned actual source",
		SummaryModelID:   "local-graph-model",
		IndexingStrategy: types.IndexingStrategy{GraphEnabled: true, GraphRequired: true},
		ExtractConfig:    &types.ExtractConfig{Enabled: true, Tags: []string{"KNOWS"}},
	}
	require.NoError(
		t,
		inputs.CreateKnowledgeBaseWithInput(repository.WithIndependentKnowledgeInput(context.Background()), kb),
	)
	blob, err := cfg.ToJSON()
	require.NoError(t, err)
	require.NoError(
		t,
		db.Create(
			&types.DataSource{
				ID:              f.Pair.DS,
				TenantID:        7,
				KnowledgeBaseID: kb.ID,
				Type:            types.ConnectorTypeNextcloud,
				Status:          types.DataSourceStatusActive,
				Config:          blob,
			},
		).Error,
	)
	base, configSHA, binding, err := repository.NextcloudEventDataSourceIdentity(blob)
	require.NoError(t, err)
	epoch, err := strconv.ParseInt(f.Epoch, 10, 64)
	require.NoError(t, err)
	require.NoError(
		t,
		db.Create(
			&repository.NextcloudSourcePairing{
				OperationID:      f.Pair.Operation,
				TenantID:         7,
				KnowledgeBaseID:  kb.ID,
				DataSourceID:     f.Pair.DS,
				InstanceID:       f.Instance,
				BindingID:        binding,
				BaseURL:          base,
				ConfigSHA:        configSHA,
				State:            "active",
				PublicationEpoch: epoch,
			},
		).Error,
	)
	item.Metadata["source_resource_id"] = item.SourceResourceID
	item.Metadata["datasource_id"] = f.Pair.DS
	item.Metadata["external_id"] = item.ExternalID
	item.Metadata["nextcloud_publication_epoch"] = f.Epoch
	item.Metadata["nextcloud_etag"] = ""
	metadata, err := json.Marshal(item.Metadata)
	require.NoError(t, err)
	doc := &types.Knowledge{
		ID:                   "actual-root-graph-document-" + strconv.FormatInt(time.Now().UnixMilli(), 10),
		TenantID:             7,
		KnowledgeBaseID:      kb.ID,
		Title:                item.FileName,
		FileName:             item.FileName,
		Type:                 "file",
		Channel:              types.ConnectorTypeNextcloud,
		Metadata:             metadata,
		ParseStatus:          types.ParseStatusFinalizing,
		EnableStatus:         "enabled",
		PendingSubtasksCount: 1,
	}
	require.NoError(t, db.Create(doc).Error)
	repo := repository.NewKnowledgeRepository(db)
	require.NoError(t, repo.(interface {
		StageNextcloudVersion(context.Context, uint64, string, string, string, string, string) error
	}).StageNextcloudVersion(context.Background(), 7, kb.ID, f.Pair.DS, item.ExternalID, f.ETag, doc.ID))
	require.NoError(t, db.Where("id=?", doc.ID).Take(doc).Error)
	leases := repository.NewNextcloudContentLeaseStore(db)
	scope := repository.NextcloudContentScope{
		TenantID:        7,
		KnowledgeBaseID: kb.ID,
		KnowledgeID:     doc.ID,
		DataSourceID:    f.Pair.DS,
		ExternalID:      item.ExternalID,
	}
	lease, err := leases.AcquireKnowledge(
		context.Background(),
		scope,
		repository.NextcloudContentBuildLease,
		"actual-live-graph-build",
		time.Minute,
	)
	require.NoError(t, err)
	defer func() { require.NoError(t, leases.ReleaseLease(context.Background(), lease.ID)) }()
	ctx := repository.WithNextcloudBuildLease(context.Background(), leases, lease, scope)
	proof, err := inputs.AdmitNextcloudBuildInputs(ctx, item.Content)
	require.NoError(t, err)
	ctx = repository.WithKnowledgeBuildInputProof(ctx, inputs, proof)
	require.NoError(t, repository.RecordNextcloudDerivedBackendSelection(ctx, nil))
	// Plain text is the controlled parser's exact chunk. It is actually stored
	// through the protected provider; embedding is explicitly skipped.
	chunk := &types.Chunk{
		ID:              doc.ID + "-chunk",
		TenantID:        7,
		KnowledgeBaseID: kb.ID,
		KnowledgeID:     doc.ID,
		Content:         string(item.Content),
		ChunkType:       types.ChunkTypeText,
		IsEnabled:       true,
		IndexStatus:     "ready",
		Metadata:        types.JSON(`{}`),
	}
	require.NoError(t, repository.NewChunkRepository(db).CreateChunks(ctx, []*types.Chunk{chunk}))
	tracker := NewSpanTracker(repository.NewKnowledgeSpanRepository(db), db)
	_, attempt, openErr := tracker.OpenAttempt(ctx, doc.ID, "controlled-text-parser")
	require.NoError(t, openErr)
	require.Equal(t, 1, attempt)
	for _, stage := range []string{types.StageDocReader, types.StageChunking, types.StageEmbedding} {
		span := tracker.BeginStage(ctx, doc.ID, 1, stage, nil)
		if stage == types.StageEmbedding {
			tracker.SkipSpan(ctx, span, "keyword-only controlled fixture")
		} else {
			tracker.EndSpan(ctx, span, nil)
		}
	}
	encrypted, err := utils.EncryptAESGCM(f.Secret, utils.GetAESKey())
	require.NoError(t, err)
	require.NoError(
		t,
		db.Table("nextcloud_event_connections").
			Create(map[string]any{"connection_id": f.Connection, "tenant_id": 7, "knowledge_base_id": kb.ID, "datasource_id": f.Pair.DS, "nextcloud_instance_id": f.Instance, "binding_id": binding, "datasource_base_url": base, "datasource_config_sha256": configSHA, "status": "active", "current_key_id": "current", "current_secret_ciphertext": encrypted}).
			Error,
	)
	state := repository.NewNextcloudProcessingStateStore(db)
	require.NoError(t, state.RefreshSourceEpoch(ctx, f.Connection))
	require.NoError(t, state.PlanGraph(ctx, doc, 1, []*types.Chunk{chunk}, true, true))
	// Restore the exact durable owned-inbox checkpoint into the fresh sender
	// fixture. The distinct new candidate advances its head exactly once; real
	// worker/provider transactions still create every success transition.
	headSeed, err := strconv.ParseInt(f.Checkpoint.Head, 10, 64)
	require.NoError(t, err)
	statusSeed, err := strconv.ParseInt(f.Checkpoint.Status, 10, 64)
	require.NoError(t, err)
	headSeed++
	var initial struct{ PayloadJSON string }
	require.NoError(
		t,
		db.Table("nextcloud_file_states").
			Where("connection_id=? AND file_id=?", f.Connection, f.File).
			Take(&initial).
			Error,
	)
	var initialState repository.NextcloudFileStateV2
	require.NoError(t, json.Unmarshal([]byte(initial.PayloadJSON), &initialState))
	initialState.HeadRevision = strconv.FormatInt(headSeed, 10)
	initialState.StatusRevision = strconv.FormatInt(statusSeed, 10)
	initialBody, err := json.Marshal(initialState)
	require.NoError(t, err)
	require.NoError(
		t,
		db.Table("nextcloud_file_states").
			Where("connection_id=? AND file_id=?", f.Connection, f.File).
			Updates(map[string]any{"head_revision": headSeed, "status_revision": statusSeed, "payload_json": string(initialBody)}).
			Error,
	)

	snapshot, err := inputs.CaptureKnowledgeBuildInputs(
		ctx,
		types.KnowledgeBuildInputCaptureRequest{
			Projection:      types.KnowledgeBuildProjectionGraph,
			TenantID:        7,
			KnowledgeBaseID: kb.ID,
			KnowledgeID:     doc.ID,
			ChunkIDs:        []string{chunk.ID},
		},
	)
	require.NoError(t, err)
	model := &graphEntryModel{}
	require.NoError(
		t,
		pipelineExtractTask(

			ctx, t,
			db,
			inputs,
			state,
			repository.NewScopedSQLGraphRepository(db, nil),

			snapshot.Proof,
			doc,
			model,
		),
	)
	require.Equal(t, 1, model.calls)
	require.NoError(t, state.Capture(ctx, 7, kb.ID, f.Pair.DS, item.ExternalID, true))
	var row struct{ PayloadJSON string }
	require.NoError(
		t,
		db.Table("nextcloud_file_states").Where("connection_id=? AND file_id=?", f.Connection, f.File).Take(&row).Error,
	)
	var result repository.NextcloudFileStateV2
	require.NoError(t, json.Unmarshal([]byte(row.PayloadJSON), &result))
	require.Equal(t, "succeeded", result.Graph.State)
	require.True(t, result.Graph.Persisted)
	require.Equal(t, "published", result.PublicationState)
	// Real dispatcher verifies current head and actual NC returns exact durable
	// inbox ACK. Re-delivery uses the same event/body and exercises duplicate ACK.
	dispatcher := NewNextcloudStateDispatcher(state, nil)
	dispatcher.deliver(ctx, time.Now().UTC())
	var delivered int64
	require.NoError(
		t,
		db.Table("nextcloud_status_outbox").
			Where("event_id=? AND state='delivered'", result.EventID).
			Count(&delivered).
			Error,
	)
	require.EqualValues(t, 1, delivered)
	require.NoError(
		t,
		nextcloud.PushProcessingState(
			ctx,
			cfg,
			f.Connection,
			"current",
			result.EventID,
			repository.NextcloudStateSigningKey([]byte(f.Secret)),
			[]byte(row.PayloadJSON),
		),
	)
	out := os.Getenv("WEKNORA_NEXTCLOUD_GRAPH_LIVE_RESULT")
	if out != "" {
		require.NoError(t, os.WriteFile(out, []byte(row.PayloadJSON), 0o600))
	}
	t.Log(
		("fixture sender DB rebuilt from exact durable owned checkpoint; actual NC o" +
			"riginal bytes -> controlled text parse/model -> real scoped SQL graph comm" +
			"it+ledger -> durable signed NC inbox ACK; exact duplicate event ACK passed"),
	)
}
