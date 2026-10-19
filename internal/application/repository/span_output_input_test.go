package repository

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

func installSpanInputs(t *testing.T, db *gorm.DB) {
	t.Helper()
	if db.Name() == "sqlite" {
		require.NoError(t, db.Exec(spansTestDDL).Error)
	} else {
		require.NoError(t, db.AutoMigrate(&types.KnowledgeProcessingSpan{}))
		require.NoError(
			t,
			db.Exec(
				("CREATE UNIQUE INDEX IF NOT EXISTS span_test_unique_tuple ON knowledge_proc" +
					"essing_spans(knowledge_id,attempt,span_id)"),
			).Error,
		)
	}
	path := "../../../migrations/sqlite/000057_processing_span_original_inputs.up.sql"
	if db.Name() == "postgres" {
		path = "../../../migrations/versioned/000138_processing_span_original_inputs.up.sql"
	}
	script, err := os.ReadFile(path)
	require.NoError(t, err)
	require.NoError(t, db.Exec(string(script)).Error)
}

func spanFixture(
	t *testing.T,
) (*KnowledgeInputProvenanceStore, *gorm.DB, context.Context, *types.Knowledge, *types.Chunk, KnowledgeSpanRepository) {
	t.Helper()
	store, db := knowledgeInputFixture(t)
	ctx, k, c, _ := createAdmittedManual(t, store, db)
	installSpanInputs(t, db)
	return store, db, ctx, k, c, NewKnowledgeSpanRepository(db)
}

func startOriginalSpan(
	ctx context.Context,
	t *testing.T,
	repo KnowledgeSpanRepository,
	kid string,
	attempt int,
	id,
	parent,
	kind string,
) *types.KnowledgeProcessingSpan {
	t.Helper()
	now := time.Now()
	row := &types.KnowledgeProcessingSpan{
		KnowledgeID:  kid,
		Attempt:      attempt,
		SpanID:       id,
		ParentSpanID: parent,
		Name:         "actual span",
		Kind:         kind,
		Status:       types.SpanStatusRunning,
		Input:        types.JSONMap{"actual": "original input body"},
		Metadata:     types.JSONMap{"control": "actual Start control"},
		StartedAt:    &now,
	}
	require.NoError(t, repo.Upsert(ctx, row))
	require.NotEmpty(t, row.WriteToken)
	return row
}

func finishOriginalSpan(
	ctx context.Context,
	t *testing.T,
	repo KnowledgeSpanRepository,
	start *types.KnowledgeProcessingSpan,
) {
	t.Helper()
	now := time.Now()
	end := *start
	end.Input = nil
	end.Metadata = nil
	end.Status = types.SpanStatusFailed
	end.FinishedAt = &now
	end.Output = types.JSONMap{"actual": "original output body"}
	end.ErrorCode = "ACTUAL_FAILURE"
	end.ErrorMessage = "original error body"
	end.ErrorDetail = "private original error detail"
	require.NoError(t, repo.Upsert(ctx, &end))
}

func captureOriginalSpans(
	t *testing.T,
	store *KnowledgeInputProvenanceStore,
	k *types.Knowledge,
	attempt int,
) *types.SpanOutputInputSnapshot {
	t.Helper()
	snapshot, err := store.CaptureSpanOutputInputs(
		context.Background(),
		types.SpanOutputReadRequest{
			TenantID:        k.TenantID,
			KnowledgeBaseID: k.KnowledgeBaseID,
			KnowledgeID:     k.ID,
			Attempt:         attempt,
		},
	)
	require.NoError(t, err)
	return snapshot
}

func TestSpanOriginalStartTerminalAtomicReceiptAndManifest(t *testing.T) {
	store, db, ctx, k, c, repo := spanFixture(t)
	root := startOriginalSpan(ctx, t, repo, k.ID, 1, "root", "", types.SpanKindRoot)
	child := startOriginalSpan(ctx, t, repo, k.ID, 1, "child", root.SpanID, types.SpanKindGeneration)
	child.Input["actual"] = "caller later mutates input"
	finishOriginalSpan(ctx, t, repo, child)
	snapshot := captureOriginalSpans(t, store, k, 1)
	require.Len(t, snapshot.Rows, 2)
	actual := snapshot.Rows[1]
	require.Equal(t, "complete", actual.ContentProvenance)
	require.Equal(t, "original input body", actual.Input["actual"])
	require.Equal(t, "actual Start control", actual.Metadata["control"])
	require.Equal(t, "original output body", actual.Output["actual"])
	require.Equal(t, "original error body", actual.ErrorMessage)
	require.NoError(t, store.RecheckSpanOutputInputs(context.Background(), snapshot))
	var manifest spanStartManifest
	require.NoError(t, db.Where("run_token = ?", child.WriteToken).Take(&manifest).Error)
	var material spanMaterialInput
	require.NoError(t, json.Unmarshal([]byte(manifest.ActualInputs), &material))
	require.Equal(t, c.Content, material.Chunks[0].Content)
	require.Equal(t, "summary", material.KnowledgeBase.SummaryModelID)
	require.Error(
		t,
		db.Model(&spanStartManifest{}).
			Where("run_token = ?", child.WriteToken).
			Update("inputs_digest", manifest.InputsDigest).
			Error,
	)
	// A later generated title in the SAME admitted origin does not stamp the
	// old input. The Begin snapshot continues to carry its own original bytes.
	k.Title = "later generated title"
	require.NoError(t, NewKnowledgeRepository(db).UpdateKnowledge(ctx, k))
	require.NoError(t, store.RecheckSpanOutputInputs(context.Background(), snapshot))
}

func TestSpanUnknownStartCannotBeCertifiedByEndAndLegacyStatusOnly(t *testing.T) {
	store, _, ctx, k, _, repo := spanFixture(t)
	now := time.Now()
	row := &types.KnowledgeProcessingSpan{
		KnowledgeID: k.ID,
		Attempt:     1,
		SpanID:      "orphan",
		Name:        "SECRET dynamic name",
		Kind:        types.SpanKindGeneration,
		Status:      types.SpanStatusRunning,
		Input:       types.JSONMap{"secret": "SECRET input"},
		Metadata:    types.JSONMap{"secret": "SECRET controls"},
		StartedAt:   &now,
	}
	require.NoError(t, repo.Upsert(context.Background(), row))
	row.Status = types.SpanStatusFailed
	row.FinishedAt = &now
	row.Output = types.JSONMap{"secret": "SECRET output"}
	row.ErrorMessage = "SECRET error"
	require.NoError(t, repo.Upsert(ctx, row))
	snapshot := captureOriginalSpans(t, store, k, 1)
	body, err := json.Marshal(snapshot)
	require.NoError(t, err)
	require.NotContains(t, string(body), "SECRET")
	require.Equal(t, "unknown", snapshot.Rows[0].ContentProvenance)
	require.Equal(t, types.SpanStatusFailed, snapshot.Rows[0].Status)
}

func TestSpanOriginalNewActualBeginReplacesUnknownRunAndClearsOldContent(t *testing.T) {
	store, _, ctx, k, _, repo := spanFixture(t)
	now := time.Now()
	root := &types.KnowledgeProcessingSpan{
		KnowledgeID: k.ID,
		Attempt:     1,
		SpanID:      "root",
		Name:        "knowledge_processing",
		Kind:        types.SpanKindRoot,
		Status:      types.SpanStatusRunning,
		StartedAt:   &now,
	}
	require.NoError(t, repo.Upsert(context.Background(), root))
	unknown := &types.KnowledgeProcessingSpan{
		KnowledgeID:  k.ID,
		Attempt:      1,
		SpanID:       "reader",
		ParentSpanID: root.SpanID,
		Name:         types.StageDocReader,
		Kind:         types.SpanKindStage,
		Status:       types.SpanStatusDone,
		Input:        types.JSONMap{"old": "OLD unproved input"},
		Output:       types.JSONMap{"old": "OLD unproved output"},
		Metadata:     types.JSONMap{"old": "OLD unproved controls"},
		ErrorMessage: "OLD unproved error",
		StartedAt:    &now,
	}
	require.NoError(t, repo.Upsert(context.Background(), unknown))
	actual := *unknown
	actual.WriteToken = ""
	actual.Status = types.SpanStatusRunning
	actual.Input = types.JSONMap{"new": "actual original Start input"}
	actual.Output = nil
	actual.Metadata = nil
	actual.ErrorMessage = ""
	require.NoError(t, repo.Upsert(ctx, &actual))
	require.NotEmpty(t, actual.WriteToken)
	snapshot := captureOriginalSpans(t, store, k, 1)
	require.Equal(t, "unknown", snapshot.Rows[0].ContentProvenance)
	require.Equal(t, "complete", snapshot.Rows[1].ContentProvenance)
	encoded, err := json.Marshal(snapshot)
	require.NoError(t, err)
	require.NotContains(t, string(encoded), "OLD")
	require.Contains(t, string(encoded), "actual original Start input")
	finishOriginalSpan(ctx, t, repo, &actual)
	require.Equal(t, "complete", captureOriginalSpans(t, store, k, 1).Rows[1].ContentProvenance)
}

func TestSpanOriginalLateRunTokenAndCrossAttemptRejected(t *testing.T) {
	store, db, ctx, k, _, repo := spanFixture(t)
	root := startOriginalSpan(ctx, t, repo, k.ID, 1, "root", "", types.SpanKindRoot)
	first := startOriginalSpan(ctx, t, repo, k.ID, 1, "stage", root.SpanID, types.SpanKindStage)
	second := startOriginalSpan(ctx, t, repo, k.ID, 1, "stage", root.SpanID, types.SpanKindStage)
	require.NotEqual(t, first.WriteToken, second.WriteToken)
	changed := *second
	changed.Input = types.JSONMap{"body": "replacement input at End"}
	changed.Status = types.SpanStatusDone
	finished := time.Now()
	changed.FinishedAt = &finished
	require.ErrorIs(t, repo.Upsert(ctx, &changed), ErrKnowledgeInputChanged, "End cannot change the original input")
	late := *first
	late.Status = types.SpanStatusDone
	now := time.Now()
	late.FinishedAt = &now
	late.Output = types.JSONMap{"body": "late worker body"}
	require.ErrorIs(t, repo.Upsert(ctx, &late), ErrKnowledgeInputChanged)
	late = *second
	late.Attempt = 2
	late.Status = types.SpanStatusDone
	late.FinishedAt = &now
	require.ErrorIs(t, repo.Upsert(ctx, &late), ErrKnowledgeInputChanged)
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	late = *second
	late.Status = types.SpanStatusDone
	late.FinishedAt = &now
	require.ErrorIs(t, repo.Upsert(canceled, &late), context.Canceled)
	snapshot := captureOriginalSpans(t, store, k, 1)
	require.Empty(t, snapshot.Rows[1].Output)
	require.NoError(
		t,
		db.Model(&types.KnowledgeProcessingSpan{}).
			Where("span_id='stage'").
			Update("status", types.SpanStatusCancelled).
			Error,
	)
	require.ErrorIs(t, repo.Upsert(ctx, &late), ErrKnowledgeInputChanged)
}

func TestSpanOriginalBodyAndInputControlABAClosed(t *testing.T) {
	for _, mode := range []string{
		"span body",
		"chunk input",
		"chunk metadata",
		"parser controls",
		"input artifact",
		"tuple",
	} {
		t.Run(mode, func(t *testing.T) {
			store, db, ctx, k, c, repo := spanFixture(t)
			root := startOriginalSpan(ctx, t, repo, k.ID, 1, "root", "", types.SpanKindRoot)
			child := startOriginalSpan(ctx, t, repo, k.ID, 1, "child", root.SpanID, types.SpanKindGeneration)
			finishOriginalSpan(ctx, t, repo, child)
			saved := captureOriginalSpans(t, store, k, 1)
			switch mode {
			case "span body":
				require.NoError(
					t,
					db.Exec(
						"UPDATE knowledge_processing_spans SET error_message='changed' WHERE span_id='child'",
					).Error,
				)
				require.NoError(
					t,
					db.Exec(
						("UPDATE knowledge_processing_spans SET error_message='original error body' " +
							"WHERE span_id='child'"),
					).Error,
				)
			case "chunk input":
				require.NoError(t, db.Model(&types.Chunk{}).Where("id = ?", c.ID).Update("content", "changed").Error)
				require.NoError(t, db.Model(&types.Chunk{}).Where("id = ?", c.ID).Update("content", c.Content).Error)
			case "chunk metadata":
				require.NoError(
					t,
					db.Model(&types.Chunk{}).
						Where("id = ?", c.ID).
						Update("metadata", types.JSON(`{"source":"changed"}`)).
						Error,
				)
				require.NoError(t, db.Model(&types.Chunk{}).Where("id = ?", c.ID).Update("metadata", c.Metadata).Error)
			case "parser controls":
				require.NoError(t, db.Exec("UPDATE knowledge_bases SET summary_model_id='changed' WHERE id='kb'").Error)
				require.NoError(t, db.Exec("UPDATE knowledge_bases SET summary_model_id='summary' WHERE id='kb'").Error)
			case "input artifact":
				require.NoError(
					t,
					db.Model(&types.Knowledge{}).
						Where("id = ?", k.ID).
						Update("metadata", types.JSON(`{"manual":{"content":"changed"}}`)).
						Error,
				)
				require.NoError(
					t,
					db.Model(&types.Knowledge{}).Where("id = ?", k.ID).Update("metadata", k.Metadata).Error,
				)
			case "tuple":
				require.NoError(
					t,
					db.Exec(
						"UPDATE knowledge_processing_spans SET parent_span_id='other-attempt' WHERE span_id='child'",
					).Error,
				)
			}
			require.Error(t, store.RecheckSpanOutputInputs(context.Background(), saved))
			actual, err := store.CaptureSpanOutputInputs(
				context.Background(),
				types.SpanOutputReadRequest{
					TenantID:        7,
					KnowledgeBaseID: k.KnowledgeBaseID,
					KnowledgeID:     k.ID,
					Attempt:         1,
				},
			)
			if mode == "span body" || mode == "tuple" {
				require.NoError(t, err)
				require.Equal(t, "unknown", actual.Rows[1].ContentProvenance)
				require.Empty(t, actual.Rows[1].Input)
				require.Empty(t, actual.Rows[1].Output)
				require.Empty(t, actual.Rows[1].ErrorMessage)
			} else {
				require.Error(t, err)
			}
		})
	}
}

func TestSpanOriginalGraphProjectionCannotCertifyFullMetadata(t *testing.T) {
	store, _, ctx, k, c, repo := spanFixture(t)
	graph, err := store.CaptureKnowledgeBuildInputs(
		ctx,
		types.KnowledgeBuildInputCaptureRequest{
			Projection:      types.KnowledgeBuildProjectionGraph,
			TenantID:        7,
			KnowledgeBaseID: k.KnowledgeBaseID,
			KnowledgeID:     k.ID,
			ChunkIDs:        []string{c.ID},
		},
	)
	require.NoError(t, err)
	ctx = WithKnowledgeBuildInputProof(ctx, store, graph.Proof)
	now := time.Now()
	row := &types.KnowledgeProcessingSpan{
		KnowledgeID: k.ID,
		Attempt:     1,
		SpanID:      "graph",
		Name:        "dynamic graph body",
		Kind:        types.SpanKindRoot,
		Status:      types.SpanStatusRunning,
		Input:       types.JSONMap{"body": "unproved metadata"},
		StartedAt:   &now,
	}
	require.NoError(t, repo.Upsert(ctx, row))
	require.Empty(t, row.WriteToken)
	snapshot := captureOriginalSpans(t, store, k, 1)
	require.Empty(t, snapshot.Rows[0].Input)
	require.Equal(t, "unknown", snapshot.Rows[0].ContentProvenance)
}

func TestSpanOriginalNextcloudETagChangeAndWithdrawalCannotCertifyOldAttempt(t *testing.T) {
	for _, mode := range []string{"withdraw", "ETagchange"} {
		t.Run(mode, func(t *testing.T) {
			store, db := knowledgeInputFixture(t)
			repo := NewKnowledgeRepository(db).(*knowledgeRepository)
			repo.nextcloudPublicationCheck = func(
				context.Context,
				*types.DataSourceConfig,
				string,
				string,
				int64,
				string,
				string,
			) error {
				return nil
			}
			require.NoError(
				t,
				repo.StageNextcloudVersion(
					context.Background(),
					7,
					"kb",
					"ds",
					"nextcloud:instance:77",
					"original-etag",
					"candidate",
				),
			)
			var k types.Knowledge
			require.NoError(t, db.Where("id='candidate'").Take(&k).Error)
			require.NoError(
				t,
				db.Model(&k).
					Updates(map[string]any{"parse_status": types.ParseStatusFinalizing, "enable_status": "disabled"}).
					Error,
			)
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
				"span-original-build",
				time.Minute,
			)
			require.NoError(t, err)
			ctx := WithNextcloudBuildLease(context.Background(), leases, lease, scope)
			proof, err := store.AdmitNextcloudBuildInputs(ctx, []byte("actual original Nextcloud bytes"))
			require.NoError(t, err)
			ctx = WithKnowledgeBuildInputProof(ctx, store, proof)
			installSpanInputs(t, db)
			spans := NewKnowledgeSpanRepository(db)
			root := startOriginalSpan(ctx, t, spans, k.ID, 1, "root", "", types.SpanKindRoot)
			finishOriginalSpan(ctx, t, spans, root)
			require.NoError(t, db.Where("id = ?", k.ID).Take(&k).Error)
			k.ParseStatus = types.ParseStatusCompleted
			k.EnableStatus = "enabled"
			require.NoError(t, repo.UpdateKnowledge(ctx, &k))
			published, err := repo.PublishNextcloudVersion(ctx, k.ID)
			require.NoError(t, err)
			require.True(t, published)
			saved := captureOriginalSpans(t, store, &k, 1)
			require.Equal(t, "original-etag", saved.SourceLineage.Sources[0].ETag)
			require.Equal(t, "complete", saved.Rows[0].ContentProvenance)
			if mode == "withdraw" {
				require.NoError(t, leases.RetireKnowledge(context.Background(), scope))
			} else {
				require.NoError(
					t,
					repo.StageNextcloudVersion(
						context.Background(),
						7,
						"kb",
						"ds",
						"nextcloud:instance:77",
						"new-etag",
						k.ID,
					),
				)
			}
			require.Error(t, store.RecheckSpanOutputInputs(context.Background(), saved))
			_, err = store.CaptureSpanOutputInputs(
				context.Background(),
				types.SpanOutputReadRequest{TenantID: 7, KnowledgeBaseID: "kb", KnowledgeID: k.ID, Attempt: 1},
			)
			require.Error(t, err)
		})
	}
}

func TestSpanOriginalKnowledgeDeletionClearsManifestAndContent(t *testing.T) {
	_, db, ctx, k, _, repo := spanFixture(t)
	root := startOriginalSpan(ctx, t, repo, k.ID, 1, "root", "", types.SpanKindRoot)
	finishOriginalSpan(ctx, t, repo, root)
	require.NoError(t, db.Unscoped().Where("id = ?", k.ID).Delete(&types.Knowledge{}).Error)
	for _, table := range []string{
		"knowledge_span_start_manifests",
		"knowledge_span_body_receipts",
		"knowledge_span_row_versions",
		"knowledge_processing_spans",
	} {
		var count int64
		require.NoError(t, db.Table(table).Count(&count).Error)
		require.Zero(t, count, table)
	}
}
