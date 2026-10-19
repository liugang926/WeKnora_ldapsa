package repository

import (
	"context"
	"os"
	"testing"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func tagOutputFixture(
	t *testing.T,
	pg bool,
) (*KnowledgeInputProvenanceStore, *gorm.DB, context.Context, *types.KnowledgeTag) {
	t.Helper()
	store, db, ctx, tags, knowledge, k := displayTestFixture(t, pg)
	path := "../../../migrations/sqlite/000060_tag_statistics_checkpoints.up.sql"
	migration := db
	if pg {
		path = "../../../migrations/versioned/000141_tag_statistics_checkpoints.up.sql"
		sqlDB, e := db.DB()
		require.NoError(t, e)
		migration, e = gorm.Open(postgres.New(postgres.Config{Conn: sqlDB, PreferSimpleProtocol: true}), &gorm.Config{})
		require.NoError(t, e)
	}
	b, e := os.ReadFile(path)
	require.NoError(t, e)
	require.NoError(t, migration.Exec(string(b)).Error)
	tag := displayTag(ctx, t, tags, "stats-tag")
	require.NoError(
		t,
		knowledge.SetKnowledgeTags(
			WithIndependentKnowledgeTagAssignmentInput(ctx, map[string][]string{k.ID: {tag.ID}}),
			k.ID,
			[]string{tag.ID},
		),
	)
	input, e := store.CaptureKnowledgeOutputInputs(
		ctx,
		types.KnowledgeBuildInputCaptureRequest{TenantID: 7, KnowledgeBaseID: "kb", KnowledgeID: k.ID},
	)
	require.NoError(t, e)
	var chunk types.Chunk
	require.NoError(t, db.Where("id='manual-chunk'").Take(&chunk).Error)
	chunk.TagID = tag.ID
	require.NoError(
		t,
		NewChunkRepository(db).UpdateChunk(WithKnowledgeBuildInputProof(ctx, store, input.Proof), &chunk),
	)
	return store, db, ctx, tag
}

func runTagOutputOriginalStats(t *testing.T, pg bool) {
	store, db, ctx, tag := tagOutputFixture(t, pg)
	snapshot, e := store.CaptureTagOutputInputs(ctx, types.TagOutputReadRequest{TenantID: 7, KnowledgeBaseID: "kb"})
	require.NoError(t, e)
	require.Len(t, snapshot.Knowledge, 1)
	require.Len(t, snapshot.Chunks, 1)
	require.Len(t, snapshot.TagProofs, 4)
	selection, e := store.SelectTagOutputInputs(
		ctx,
		snapshot,
		types.TagOutputSelectionRequest{Page: types.Pagination{PageSize: 1}, Keyword: "human"},
	)
	require.NoError(t, e)
	require.Equal(t, int64(1), selection.Total)
	require.Len(t, selection.Tags, 1)
	require.Equal(t, int64(1), selection.Tags[0].KnowledgeCount)
	require.Equal(t, int64(1), selection.Tags[0].ChunkCount)
	require.NoError(t, store.RecheckTagOutputInputs(ctx, snapshot, selection))
	// Every mutated field requires its original material receipt. A document
	// admission alone cannot certify a raw/legacy FAQ or document chunk.
	require.NoError(
		t,
		db.Model(&types.Chunk{}).Where("id='manual-chunk'").Update("content", "unknown derived chunk body").Error,
	)
	_, e = store.CaptureTagOutputInputs(ctx, snapshot.Request)
	require.Error(t, e)
	require.Error(t, store.RecheckTagOutputInputs(ctx, snapshot, selection))
	_ = tag
}
func TestTagOutputSQLiteOriginalStats(t *testing.T)   { runTagOutputOriginalStats(t, false) }
func TestTagOutputPostgresOriginalStats(t *testing.T) { runTagOutputOriginalStats(t, true) }
func runTagOutputABA(t *testing.T, pg bool) {
	for _, kind := range []string{"tag-membership", "relation-membership", "chunk-membership", "chunk-body", "knowledge-body", "count-soft-delete"} {
		t.Run(kind, func(t *testing.T) {
			store, db, ctx, _ := tagOutputFixture(t, pg)
			snapshot, e := store.CaptureTagOutputInputs(
				ctx,
				types.TagOutputReadRequest{TenantID: 7, KnowledgeBaseID: "kb"},
			)
			require.NoError(t, e)
			switch kind {
			case "tag-membership":
				tag := &types.KnowledgeTag{ID: "transient", TenantID: 7, KnowledgeBaseID: "kb", Name: "transient"}
				require.NoError(t, db.Create(tag).Error)
				require.NoError(t, db.Delete(tag).Error)
			case "relation-membership":
				require.NoError(t, db.Where("knowledge_id='manual'").Delete(&types.KnowledgeTagRelation{}).Error)
				require.NoError(
					t,
					db.Create(&types.KnowledgeTagRelation{KnowledgeID: "manual", TagID: "stats-tag"}).Error,
				)
			case "chunk-membership":
				c := &types.Chunk{
					ID:              "transient",
					TenantID:        7,
					KnowledgeBaseID: "kb",
					KnowledgeID:     "manual",
					TagID:           "stats-tag",
				}
				require.NoError(t, db.Create(c).Error)
				require.NoError(t, db.Unscoped().Delete(c).Error)
			case "chunk-body":
				require.NoError(t, db.Exec("UPDATE chunks SET content='ABA' WHERE id='manual-chunk'").Error)
				require.NoError(
					t,
					db.Exec("UPDATE chunks SET content='actual parsed body' WHERE id='manual-chunk'").Error,
				)
			case "knowledge-body":
				require.NoError(t, db.Exec("UPDATE knowledges SET title='ABA' WHERE id='manual'").Error)
				require.NoError(t, db.Exec("UPDATE knowledges SET title='human title' WHERE id='manual'").Error)
			case "count-soft-delete":
				require.NoError(
					t,
					db.Exec("UPDATE chunks SET deleted_at=CURRENT_TIMESTAMP WHERE id='manual-chunk'").Error,
				)
				require.NoError(t, db.Exec("UPDATE chunks SET deleted_at=NULL WHERE id='manual-chunk'").Error)
			}
			require.Error(
				t,
				store.RecheckTagOutputInputs(ctx, snapshot, nil),
				"whole-set revision must reject same final count/body",
			)
		})
	}
}
func TestTagOutputSQLiteWholeSetABA(t *testing.T)   { runTagOutputABA(t, false) }
func TestTagOutputPostgresWholeSetABA(t *testing.T) { runTagOutputABA(t, true) }
func runTagCommittedFields(t *testing.T, pg bool) {
	store, db, ctx, tags, _, _ := displayTestFixture(t, pg)
	marked := WithTagCommittedOutput(ctx)
	tag := displayTag(marked, t, tags, "committed-tag")
	snapshot, e := TagCommittedOutput(marked)
	require.NoError(t, e)
	require.Equal(t, tag.Name, snapshot.Tags[0].Name)
	require.NoError(t, store.RecheckTagOutputInputs(ctx, snapshot, nil))
	legacy := &types.KnowledgeTag{
		ID:              "legacy",
		TenantID:        7,
		KnowledgeBaseID: "kb",
		Name:            "unknown secret",
		Color:           "red",
		SortOrder:       2,
	}
	require.NoError(t, tags.Create(ctx, legacy))
	color := "green"
	legacy.Color = color
	marked = WithTagCommittedOutput(ctx)
	require.NoError(t, tags.Update(WithIndependentKnowledgeTagUpdateInput(marked, legacy.ID, nil, &color, nil), legacy))
	_, e = TagCommittedOutput(marked)
	require.Error(t, e)
	var count int64
	require.NoError(
		t,
		db.Table("knowledge_display_input_origins").Where("row_id='legacy' AND kind='tag-name'").Count(&count).Error,
	)
	require.Zero(t, count)
}
func TestTagOutputSQLiteCommittedFields(t *testing.T)   { runTagCommittedFields(t, false) }
func TestTagOutputPostgresCommittedFields(t *testing.T) { runTagCommittedFields(t, true) }

type tagOutputRRPauseKey struct{}

func TestTagOutputPostgresCoherentRRAndTransientCountMembership(t *testing.T) {
	store, db, ctx, _ := tagOutputFixture(t, true)
	request := types.TagOutputReadRequest{TenantID: 7, KnowledgeBaseID: "kb"}
	baseline, e := store.CaptureTagOutputInputs(ctx, request)
	require.NoError(t, e)
	fired := false
	name := "tag_output_rr_membership_change"
	require.NoError(t, db.Callback().Query().After("gorm:query").Register(name, func(tx *gorm.DB) {
		if fired || tx.Statement.Context.Value(tagOutputRRPauseKey{}) != true ||
			tx.Statement.Table != "knowledge_tags" {
			return
		}
		fired = true
		chunk := &types.Chunk{
			ID:              "rr-transient",
			TenantID:        7,
			KnowledgeBaseID: "kb",
			KnowledgeID:     "manual",
			TagID:           "stats-tag",
		}
		require.NoError(t, db.Create(chunk).Error)
		require.NoError(t, db.Unscoped().Delete(chunk).Error)
	}))
	t.Cleanup(func() { _ = db.Callback().Query().Remove(name) })
	captured, e := store.CaptureTagOutputInputs(context.WithValue(ctx, tagOutputRRPauseKey{}, true), request)
	require.NoError(t, e)
	require.True(t, fired)
	require.True(
		t,
		tagOutputEqual(baseline, captured),
		"one RR view binds tags, refs, chunks, original material and the pre-change scope version",
	)
	require.Error(
		t,
		store.RecheckTagOutputInputs(ctx, captured, nil),
		"final checkpoint sees committed add/delete even though the count/body returned to baseline",
	)
}
