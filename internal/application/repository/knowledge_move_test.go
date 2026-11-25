package repository

import (
	"context"
	"errors"
	"testing"

	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

func testRetainedKnowledgeMove(t *testing.T, pg bool) {
	db, store := bodyProtocolFixture(t, pg)
	require.NoError(t, db.AutoMigrate(&types.Chunk{}, &types.KnowledgeTagRelation{}))
	require.NoError(t, db.Exec("CREATE TABLE move_test_index (id text PRIMARY KEY, kb text)").Error)
	require.NoError(t, db.Exec("INSERT INTO move_test_index VALUES ('doc','kb')").Error)
	source := &types.KnowledgeBase{ID: "kb", TenantID: 7}
	target := &types.KnowledgeBase{ID: "dst", TenantID: 7}
	require.NoError(t, db.Create(target).Error)
	ctx, err := access.WithKBTransferTask(context.Background(), source, target, 7, access.KBTransferMove, "job", false)
	require.NoError(t, err)
	row := &types.Knowledge{
		ID: "doc", TenantID: 7, KnowledgeBaseID: source.ID, ParseStatus: types.ParseStatusProcessing, Title: "drawing",
		Metadata: types.JSON(`{"` + types.KnowledgeTransferMetadataKey + `":{"task_id":"job","source_id":"doc",` +
			`"source_kb":"kb","target_kb":"dst","phase":"moving","operation":"move","mode":"reuse_vectors"}}`),
	}
	require.NoError(t, db.Create(row).Error)
	require.NoError(t, db.Create(&types.Chunk{
		ID: "chunk", TenantID: 7, KnowledgeBaseID: source.ID, KnowledgeID: row.ID, Content: "ordinary original",
	}).Error)
	repo := NewKnowledgeRepository(db)
	var head originalBodyFieldHead
	readHead := func() originalBodyFieldHead {
		head = originalBodyFieldHead{}
		require.NoError(t, db.Where("source_table='chunks' AND row_key='chunk' AND column_name='content'").
			Take(&head).Error)
		return head
	}
	beforeHead := readHead()
	stored, err := repo.GetKnowledgeByID(ctx, 7, row.ID)
	require.NoError(t, err)
	destination := *stored
	destination.KnowledgeBaseID = target.ID
	destination.ParseStatus = types.ParseStatusCompleted
	var indexKB string
	assertScope := func(kb string) {
		t.Helper()
		var knowledge types.Knowledge
		var chunk types.Chunk
		require.NoError(t, db.First(&knowledge, "id=?", row.ID).Error)
		require.NoError(t, db.First(&chunk, "id=?", "chunk").Error)
		require.Equal(t, kb, knowledge.KnowledgeBaseID)
		require.Equal(t, kb, chunk.KnowledgeBaseID)
		require.Equal(t, "ordinary original", chunk.Content)
		require.Equal(t, "drawing", knowledge.Title)
		require.NoError(t, db.Raw("SELECT kb FROM move_test_index WHERE id='doc'").Scan(&indexKB).Error)
		require.Equal(t, kb, indexKB)
	}
	moveIndex := func(ctx context.Context) error {
		return KnowledgeMoveDB(ctx, db).Exec("UPDATE move_test_index SET kb='dst' WHERE id='doc'").Error
	}
	require.Error(t, repo.CommitKnowledgeMove(context.Background(), stored, &destination, true, nil))
	assertScope(source.ID)

	// A purged original rejects the move before the index callback or tag cleanup escapes.
	require.NoError(t, db.Model(&originalBodyPayload{}).Where("id=?", beforeHead.BodyID).
		Update("state", "purged").Error)
	require.ErrorIs(t, repo.CommitKnowledgeMove(ctx, stored, &destination, true, func(context.Context) error {
		t.Fatal("index writer reached for a purged original")
		return nil
	}), ErrOriginalBodyUnavailable)
	require.NoError(t, db.Model(&originalBodyPayload{}).Where("id=?", beforeHead.BodyID).Update("state", "live").Error)
	assertScope(source.ID)
	require.Equal(t, beforeHead, readHead())

	// Tampering with bytes outside the producer cannot mint a destination receipt.
	require.NoError(t, db.Exec("UPDATE chunks SET content='unexpected' WHERE id='chunk'").Error)
	require.ErrorIs(t, repo.CheckMoveOriginals(ctx, stored, target.ID), ErrOriginalBodyUnavailable)
	require.ErrorIs(t, repo.CommitKnowledgeMove(ctx, stored, &destination, true, func(context.Context) error {
		t.Fatal("index writer reached for an unverified source")
		return nil
	}), ErrOriginalBodyUnavailable)
	require.NoError(t, db.Exec("UPDATE chunks SET content='ordinary original' WHERE id='chunk'").Error)
	require.NoError(t, repo.CheckMoveOriginals(ctx, stored, target.ID))
	injected := errors.New("injected index failure")
	require.ErrorIs(t, repo.CommitKnowledgeMove(ctx, stored, &destination, true, func(ctx context.Context) error {
		if err := moveIndex(ctx); err != nil {
			return err
		}
		return injected
	}), injected)
	assertScope(source.ID)
	require.Equal(t, beforeHead, readHead())

	require.NoError(t, repo.CommitKnowledgeMove(ctx, stored, &destination, true, moveIndex))
	assertScope(target.ID)
	afterHead := readHead()
	require.Equal(t, beforeHead.SlotID, afterHead.SlotID)
	require.Equal(t, beforeHead.Revision+1, afterHead.Revision)
	require.NotEqual(t, beforeHead.BodyID, afterHead.BodyID)
	require.Equal(t, target.ID, afterHead.KnowledgeBaseID)
	// Source receipt is immutable; GC tracks the new current owner separately.
	var revision originalBodyFieldHead
	require.NoError(t, db.Table("original_body_field_revisions").Where("slot_id=? AND revision=?",
		beforeHead.SlotID, beforeHead.Revision).Take(&revision).Error)
	require.Equal(t, beforeHead, revision)
	require.NoError(t, db.Transaction(func(tx *gorm.DB) error {
		current, _, err := store.fieldCurrent(tx, beforeHead, 0)
		require.NoError(t, err)
		require.False(t, current)
		current, _, err = store.fieldCurrent(tx, afterHead, 0)
		require.NoError(t, err)
		require.True(t, current)
		return nil
	}))
}

func TestRetainedKnowledgeMoveSQLite(t *testing.T)   { testRetainedKnowledgeMove(t, false) }
func TestRetainedKnowledgeMovePostgres(t *testing.T) { testRetainedKnowledgeMove(t, true) }
