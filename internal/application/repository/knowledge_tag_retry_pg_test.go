package repository

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

func TestPostgresKnowledgeTagRetryAfterStatisticsConflict(t *testing.T) {
	_, db := collectionPostgresFixture(t)
	script, err := os.ReadFile("../../../migrations/versioned/000141_tag_statistics_checkpoints.up.sql")
	require.NoError(t, err)
	require.NoError(t, db.Exec(string(script)).Error)
	tag := types.KnowledgeTag{ID: "retry-tag", TenantID: 7, KnowledgeBaseID: "kb", Name: "retry"}
	require.NoError(t, db.Create(&tag).Error)

	// Commit a change from a second connection after the tag transaction has
	// established its repeatable-read snapshot but before the relation trigger.
	const hook = "advance_statistics_after_tag_snapshot"
	inserts := 0
	require.NoError(t, db.Callback().Create().Before("gorm:create").Register(hook, func(tx *gorm.DB) {
		if tx.Statement.Table == "knowledge_tag_relations" {
			inserts++
			if inserts == 1 {
				require.NoError(t, db.Exec("UPDATE knowledge_tag_statistics_scope_versions SET revision=revision+1 "+
					"WHERE tenant_id=7 AND knowledge_base_id='kb'").Error)
			}
		}
	}))
	t.Cleanup(func() { _ = db.Callback().Create().Remove(hook) })
	require.NoError(t, NewKnowledgeRepository(db).SetKnowledgeTags(context.Background(), "doc-a", []string{tag.ID}))
	require.Equal(t, 2, inserts)
	var count int64
	require.NoError(t, db.Model(&types.KnowledgeTagRelation{}).
		Where("knowledge_id=? AND tag_id=?", "doc-a", tag.ID).Count(&count).Error)
	require.EqualValues(t, 1, count)
}

func TestPostgresKnowledgeTagRetryLimits(t *testing.T) {
	db := leaseTestPostgres(t)
	conflict := &pgconn.PgError{Code: "40001"}
	ctx := context.Background()
	t.Run("non-serialization error", func(t *testing.T) {
		calls := 0
		other := errors.New("text containing 40001")
		err := retryKnowledgeTagWrite(ctx, db, func() error {
			calls++
			return other
		})
		require.ErrorIs(t, err, other)
		require.Equal(t, 1, calls)
	})
	t.Run("cancel after rollback", func(t *testing.T) {
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()
		calls := 0
		err := retryKnowledgeTagWrite(ctx, db, func() error {
			calls++
			cancel()
			return conflict
		})
		require.ErrorIs(t, err, context.Canceled)
		require.Equal(t, 1, calls)
	})
	t.Run("bounded", func(t *testing.T) {
		calls := 0
		err := retryKnowledgeTagWrite(ctx, db, func() error {
			calls++
			return conflict
		})
		require.ErrorIs(t, err, conflict)
		require.Equal(t, 5, calls)
	})
	t.Run("nested transactions propagate to owner", func(t *testing.T) {
		calls := 0
		err := db.Transaction(func(tx *gorm.DB) error {
			return retryKnowledgeTagWrite(ctx, tx, func() error {
				calls++
				return conflict
			})
		})
		require.ErrorIs(t, err, conflict)
		require.Equal(t, 1, calls)
	})
}

// A deferred trigger fails the first COMMIT, after the attempted replacement
// has already created its admission proof. Retry must retire the old row and
// publish exactly one new row, relation and proof.
func TestPostgresKnowledgeTagRetryReplacementCommit(t *testing.T) {
	store, db := collectionPostgresFixture(t)
	require.NoError(t, db.AutoMigrate(&types.DataSource{}))
	ds := types.DataSource{ID: "dav", TenantID: 7, KnowledgeBaseID: "kb", Type: "nextcloud_webdav"}
	require.NoError(t, db.Create(&ds).Error)
	tag := types.KnowledgeTag{ID: "dav-tag", TenantID: 7, KnowledgeBaseID: "kb", Name: "DAV"}
	require.NoError(t, db.Create(&tag).Error)
	lineage, err := types.NewCompleteSourceLineage()
	require.NoError(t, err)
	ctx, err := WithDerivedKnowledgeInput(context.Background(), lineage)
	require.NoError(t, err)
	old := &types.Knowledge{
		ID: "dav-old", TenantID: 7, KnowledgeBaseID: "kb", Type: "file", Channel: ds.Type,
		FileName: "dav.md", FilePath: "old.md", ParseStatus: types.ParseStatusPending,
		Metadata: types.JSON(`{"datasource_id":"dav","external_id":"one"}`),
	}
	_, err = store.CreateKnowledgeWithInput(ctx, old, []byte("old"))
	require.NoError(t, err)
	old, err = NewKnowledgeRepository(db).GetKnowledgeByID(ctx, 7, old.ID)
	require.NoError(t, err)
	replacement := *old
	replacement.ID, replacement.FilePath = "dav-new", "new.md"
	require.NoError(t, db.Exec(`CREATE SEQUENCE deferred_tag_attempt;
		CREATE FUNCTION reject_first_tag_commit() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN
			IF nextval('deferred_tag_attempt') = 1 THEN
				RAISE EXCEPTION 'aborted at commit' USING ERRCODE = '40001';
			END IF;
			RETURN NULL;
		END $$;
		CREATE CONSTRAINT TRIGGER retry_tag_commit AFTER INSERT ON knowledge_tag_relations
		DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION reject_first_tag_commit();`).Error)
	artifact := []byte("new")
	proof, err := ReplaceKnowledgeWithConfirmedInput(ctx, store, old, &replacement, []string{tag.ID}, artifact)
	require.NoError(t, err)
	require.NoError(t, KnowledgeInputArtifactMatches(proof, artifact))
	var attempts int64
	require.NoError(t, db.Raw("SELECT last_value FROM deferred_tag_attempt").Scan(&attempts).Error)
	require.EqualValues(t, 2, attempts)
	var count int64
	require.NoError(t, db.Model(&types.Knowledge{}).
		Where("id IN ?", []string{old.ID, replacement.ID}).Count(&count).Error)
	require.EqualValues(t, 1, count)
	require.NoError(t, db.Model(&knowledgeInputAdmission{}).
		Where("knowledge_id=?", replacement.ID).Count(&count).Error)
	require.EqualValues(t, 1, count)
	require.NoError(t, db.Model(&types.KnowledgeTagRelation{}).
		Where("knowledge_id=?", replacement.ID).Count(&count).Error)
	require.EqualValues(t, 1, count)
}
