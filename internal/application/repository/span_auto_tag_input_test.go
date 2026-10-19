package repository

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

func autoSpanFixture(
	t *testing.T,
	pg, derived bool,
) (*KnowledgeInputProvenanceStore, *gorm.DB, context.Context, *types.Knowledge, interfaces.KnowledgeRepository, *types.KnowledgeAutoTagInputSnapshot, *types.KnowledgeProcessingSpan, KnowledgeSpanRepository) {
	t.Helper()
	store, db, ctx, tags, knowledge, k := displayTestFixture(t, pg)
	_ = displayTag(t, tags, ctx, "span-match")
	if derived {
		sourceID := "candidate"
		if pg {
			sourceID = "doc-a"
		}
		_, source, err := knowledge.(*knowledgeRepository).ResolveNextcloudSourceLineage(ctx, 7, "kb", sourceID)
		require.NoError(t, err)
		lineage, err := types.NewCompleteSourceLineage(source)
		require.NoError(t, err)
		origin, err := WithDerivedKnowledgeInput(ctx, lineage)
		require.NoError(t, err)
		input := &types.Knowledge{
			ID:              "span-candidate-origin",
			TenantID:        7,
			KnowledgeBaseID: "kb",
			Type:            types.KnowledgeTypeManual,
			Channel:         "web",
			CustomMetadata:  types.JSON(`{}`),
			ParseStatus:     types.ParseStatusCompleted,
			EnableStatus:    "enabled",
		}
		proof, err := store.CreateKnowledgeWithInput(origin, input, "actual unselected candidate origin")
		require.NoError(t, err)
		producer, err := WithDerivedKnowledgeDisplayInput(ctx, proof)
		require.NoError(t, err)
		require.NoError(
			t,
			tags.Create(
				producer,
				&types.KnowledgeTag{
					ID:              "span-nonmatch",
					TenantID:        7,
					KnowledgeBaseID: "kb",
					Name:            "unselected sourced candidate",
					SortOrder:       8,
				},
			),
		)
	} else {
		_ = displayTag(t, tags, ctx, "span-nonmatch")
	}
	actual, err := store.CaptureKnowledgeOutputInputs(
		ctx,
		types.KnowledgeBuildInputCaptureRequest{
			TenantID:        7,
			KnowledgeBaseID: "kb",
			KnowledgeID:     k.ID,
			ChunkIDs:        []string{"manual-chunk"},
		},
	)
	require.NoError(t, err)
	manifest, err := store.CaptureKnowledgeAutoTagInputs(ctx, actual.Proof)
	require.NoError(t, err)
	installSpanInputs(t, db)
	repo := NewKnowledgeSpanRepository(db)
	ctx = WithKnowledgeBuildInputProof(ctx, store, actual.Proof)
	root := startOriginalSpan(t, repo, ctx, k.ID, 1, "root", "", types.SpanKindRoot)
	ctx, err = WithSpanAutoTagOriginalInput(ctx, store, manifest)
	require.NoError(t, err)
	now := time.Now()
	row := &types.KnowledgeProcessingSpan{
		KnowledgeID:  k.ID,
		Attempt:      1,
		SpanID:       "auto-classifier",
		ParentSpanID: root.SpanID,
		Name:         autoTagProducerSpanName,
		Kind:         types.SpanKindSubSpan,
		Status:       types.SpanStatusRunning,
		StartedAt:    &now,
		Input:        types.JSONMap{"candidate_count": len(manifest.Tags)},
	}
	require.NoError(t, repo.Upsert(ctx, row))
	require.NotEmpty(t, row.WriteToken)
	return store, db, ctx, k, knowledge, manifest, row, repo
}

func runAutoSpanOriginalAdditionalManifest(t *testing.T, pg bool) {
	store, db, ctx, k, knowledge, manifest, row, repo := autoSpanFixture(t, pg, true)
	var original spanStartManifest
	require.NoError(t, db.Where("run_token=?", row.WriteToken).Take(&original).Error)
	var input spanMaterialInput
	require.NoError(t, json.Unmarshal([]byte(original.ActualInputs), &input))
	require.Len(t, input.Additional, 1)
	require.Equal(t, manifest.ManifestID, input.Additional[0].ID)
	require.Len(t, input.Additional[0].AutoTag.Tags, 2)
	writeCtx, err := WithKnowledgeAutoTagInput(ctx, store, manifest)
	require.NoError(t, err)
	require.NoError(t, knowledge.AddKnowledgeTagRelations(writeCtx, 7, "kb", k.ID, []string{"span-match"}))
	finishOriginalSpan(t, repo, ctx, row)
	snapshot, err := store.CaptureSpanOutputInputs(
		context.Background(),
		types.SpanOutputReadRequest{TenantID: 7, KnowledgeBaseID: "kb", KnowledgeID: k.ID, Attempt: 1},
	)
	require.NoError(t, err)
	require.Equal(t, manifest.SourceLineage, snapshot.SourceLineage)
	require.Equal(t, "complete", snapshot.Rows[1].ContentProvenance)
	var after spanStartManifest
	require.NoError(t, db.Where("run_token=?", row.WriteToken).Take(&after).Error)
	require.Equal(t, original, after, "End retains the exact original Begin manifest and run token")
	// Operational status changes do not change the model's consumed body.
	require.NoError(
		t,
		db.Model(&types.Knowledge{}).
			Where("id=?", k.ID).
			Updates(map[string]any{"parse_status": types.ParseStatusCompleted, "updated_at": time.Now()}).
			Error,
	)
	require.NoError(t, store.RecheckSpanOutputInputs(context.Background(), snapshot))
	source := manifest.SourceLineage.Sources[0]
	require.NoError(
		t,
		NewNextcloudContentLeaseStore(
			db,
		).RetireKnowledge(ctx, NextcloudContentScope{TenantID: 7, KnowledgeBaseID: "kb", KnowledgeID: source.KnowledgeID, DataSourceID: source.DataSourceID, ExternalID: source.ExternalID}),
	)
	_, err = store.CaptureSpanOutputInputs(
		context.Background(),
		types.SpanOutputReadRequest{TenantID: 7, KnowledgeBaseID: "kb", KnowledgeID: k.ID, Attempt: 1},
	)
	require.Error(t, err, "unselected candidate source withdrawal closes span counters/errors")
}

func TestAutoSpanSQLiteOriginalAdditionalManifest(t *testing.T) {
	runAutoSpanOriginalAdditionalManifest(t, false)
}

func TestAutoSpanPostgresOriginalAdditionalManifest(t *testing.T) {
	runAutoSpanOriginalAdditionalManifest(t, true)
}

func runAutoSpanCandidateAndRelationABA(t *testing.T, pg bool) {
	for _, mutation := range []string{"candidate name ABA", "relationship ABA", "human origin substitution", "extra manifest replacement"} {
		t.Run(mutation, func(t *testing.T) {
			store, db, ctx, k, knowledge, manifest, row, repo := autoSpanFixture(t, pg, false)
			writeCtx, err := WithKnowledgeAutoTagInput(ctx, store, manifest)
			require.NoError(t, err)
			require.NoError(t, knowledge.AddKnowledgeTagRelations(writeCtx, 7, "kb", k.ID, []string{"span-match"}))
			finishOriginalSpan(t, repo, ctx, row)
			before, err := store.CaptureSpanOutputInputs(
				context.Background(),
				types.SpanOutputReadRequest{TenantID: 7, KnowledgeBaseID: "kb", KnowledgeID: k.ID, Attempt: 1},
			)
			require.NoError(t, err)
			switch mutation {
			case "candidate name ABA":
				require.NoError(
					t,
					db.Model(&types.KnowledgeTag{}).
						Where("id='span-nonmatch'").
						Update("name", "changed unselected").
						Error,
				)
				require.NoError(
					t,
					db.Model(&types.KnowledgeTag{}).
						Where("id='span-nonmatch'").
						Update("name", "actual human span-nonmatch").
						Error,
				)
			case "relationship ABA":
				require.NoError(
					t,
					db.Create(&types.KnowledgeTagRelation{KnowledgeID: k.ID, TagID: "span-nonmatch"}).Error,
				)
				require.NoError(
					t,
					db.Where("knowledge_id=? AND tag_id='span-nonmatch'", k.ID).
						Delete(&types.KnowledgeTagRelation{}).
						Error,
				)
			case "human origin substitution":
				require.NoError(
					t,
					knowledge.SetKnowledgeTags(
						WithIndependentKnowledgeTagAssignmentInput(ctx, map[string][]string{k.ID: {"span-nonmatch"}}),
						k.ID,
						[]string{"span-nonmatch"},
					),
				)
			case "extra manifest replacement":
				data, err := json.Marshal(manifest)
				require.NoError(t, err)
				var forged types.KnowledgeAutoTagInputSnapshot
				require.NoError(t, json.Unmarshal(data, &forged))
				forged.ManifestID = "not-the-original-run"
				other, err := WithSpanAutoTagOriginalInput(ctx, store, &forged)
				require.NoError(t, err)
				now := time.Now()
				end := *row
				end.Status = types.SpanStatusFailed
				end.FinishedAt = &now
				require.Error(t, repo.Upsert(other, &end))
				return
			}
			require.Error(t, store.RecheckSpanOutputInputs(context.Background(), before))
		})
	}
}

func TestAutoSpanSQLiteCandidateAndRelationABA(t *testing.T) {
	runAutoSpanCandidateAndRelationABA(t, false)
}

func TestAutoSpanPostgresCandidateAndRelationABA(t *testing.T) {
	runAutoSpanCandidateAndRelationABA(t, true)
}

func TestAutoSpanUnknownBeginCannotBeSignedByAdditionalInputAtEnd(t *testing.T) {
	store, db, ctx, k, _, manifest, _, repo := autoSpanFixture(t, false, false)
	base := WithKnowledgeBuildInputProof(context.Background(), store, manifest.Original.Proof)
	now := time.Now()
	row := &types.KnowledgeProcessingSpan{
		KnowledgeID:  k.ID,
		Attempt:      1,
		SpanID:       "unknown-auto",
		ParentSpanID: "root",
		Name:         autoTagProducerSpanName,
		Kind:         types.SpanKindSubSpan,
		Status:       types.SpanStatusRunning,
		StartedAt:    &now,
		Input:        types.JSONMap{"secret": "SECRET incomplete candidate input"},
	}
	require.NoError(t, repo.Upsert(base, row))
	require.Empty(t, row.WriteToken)
	row.Status = types.SpanStatusFailed
	row.FinishedAt = &now
	row.ErrorMessage = "SECRET late candidate output"
	require.NoError(t, repo.Upsert(ctx, row))
	snapshot, err := store.CaptureSpanOutputInputs(
		context.Background(),
		types.SpanOutputReadRequest{TenantID: 7, KnowledgeBaseID: "kb", KnowledgeID: k.ID, Attempt: 1},
	)
	require.NoError(t, err)
	data, err := json.Marshal(snapshot.Rows)
	require.NoError(t, err)
	require.NotContains(t, string(data), "SECRET")
	require.Equal(t, "unknown", snapshot.Rows[len(snapshot.Rows)-1].ContentProvenance)
	_ = db
}

type autoSpanSnapshotPauseKey struct{}

func TestAutoSpanPostgresCoherentAdditionalInputsAndFinalRecheck(t *testing.T) {
	store, db, ctx, k, knowledge, manifest, row, repo := autoSpanFixture(t, true, false)
	writer, err := WithKnowledgeAutoTagInput(ctx, store, manifest)
	require.NoError(t, err)
	require.NoError(t, knowledge.AddKnowledgeTagRelations(writer, 7, "kb", k.ID, []string{"span-match"}))
	finishOriginalSpan(t, repo, ctx, row)
	before, err := store.CaptureSpanOutputInputs(
		context.Background(),
		types.SpanOutputReadRequest{TenantID: 7, KnowledgeBaseID: "kb", KnowledgeID: k.ID, Attempt: 1},
	)
	require.NoError(t, err)
	fired := false
	name := "auto_span_unselected_changed_after_header"
	require.NoError(t, db.Callback().Query().After("gorm:query").Register(name, func(tx *gorm.DB) {
		if fired || tx.Statement.Context.Value(autoSpanSnapshotPauseKey{}) != true ||
			tx.Statement.Table != "knowledges" {
			return
		}
		fired = true
		require.NoError(
			t,
			db.Model(&types.KnowledgeTag{}).
				Where("id='span-nonmatch'").
				Update("name", "concurrent unselected candidate").
				Error,
		)
	}))
	t.Cleanup(func() { _ = db.Callback().Query().Remove(name) })
	actual, err := store.CaptureSpanOutputInputs(
		context.WithValue(context.Background(), autoSpanSnapshotPauseKey{}, true),
		types.SpanOutputReadRequest{TenantID: 7, KnowledgeBaseID: "kb", KnowledgeID: k.ID, Attempt: 1},
	)
	require.NoError(t, err)
	require.True(t, fired)
	require.Equal(t, before.Receipt, actual.Receipt)
	require.Equal(t, "complete", actual.Rows[1].ContentProvenance)
	require.Error(
		t,
		store.RecheckSpanOutputInputs(context.Background(), actual),
		"one RR snapshot keeps the original span body/additional receipts coherent; the final checkpoint rejects committed unselected input mutation",
	)
}
