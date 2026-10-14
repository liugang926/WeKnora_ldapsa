package postgres

import (
	"context"
	"encoding/json"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	apprepo "github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

type nextcloudVectorVersionStore interface {
	StageNextcloudVersion(context.Context, uint64, string, string, string, string, string) error
}

func nextcloudVectorTestDB(t *testing.T) *gorm.DB {
	t.Helper()
	dsn := os.Getenv("WEKNORA_PGVECTOR_RACE_TEST_DSN")
	if dsn == "" {
		t.Skip("set WEKNORA_PGVECTOR_RACE_TEST_DSN for a disposable pgvector PostgreSQL")
	}
	admin, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	require.NoError(t, err)
	adminSQL, err := admin.DB()
	require.NoError(t, err)
	t.Cleanup(func() { _ = adminSQL.Close() })
	schema := "nc_vector_" + strings.ReplaceAll(uuid.NewString()[:8], "-", "")
	require.NoError(t, admin.Exec(`CREATE SCHEMA "`+schema+`"`).Error)
	t.Cleanup(func() { _ = admin.Exec(`DROP SCHEMA "` + schema + `" CASCADE`).Error })
	u, err := url.Parse(dsn)
	require.NoError(t, err)
	query := u.Query()
	query.Set("search_path", schema+",public")
	u.RawQuery = query.Encode()
	db, err := gorm.Open(postgres.Open(u.String()), &gorm.Config{})
	require.NoError(t, err)
	sqlDB, err := db.DB()
	require.NoError(t, err)
	sqlDB.SetMaxOpenConns(12)
	t.Cleanup(func() { _ = sqlDB.Close() })
	require.NoError(t, db.Exec(`CREATE EXTENSION IF NOT EXISTS vector`).Error)
	require.NoError(t, db.AutoMigrate(&types.Knowledge{}))
	require.NoError(t, db.Exec(`CREATE TABLE chunks (
		id TEXT PRIMARY KEY, tenant_id BIGINT NOT NULL,
		knowledge_base_id TEXT NOT NULL, knowledge_id TEXT NOT NULL,
		image_info TEXT NOT NULL DEFAULT '')`).Error)
	for _, file := range []string{
		"000112_nextcloud_source_versions.up.sql",
		"000119_nextcloud_gc.up.sql",
		"000121_nextcloud_gc_object_claim.up.sql",
		"000122_nextcloud_gc_derived_inventory.up.sql",
		"000127_nextcloud_content_leases.up.sql",
	} {
		script, err := os.ReadFile("../../../../../migrations/versioned/" + file)
		require.NoError(t, err)
		require.NoError(t, db.Exec(string(script)).Error, file)
	}
	require.NoError(t, db.Exec(`CREATE TABLE embeddings (
		id BIGSERIAL PRIMARY KEY, source_id TEXT NOT NULL,
		source_type INTEGER NOT NULL, chunk_id TEXT,
		knowledge_id TEXT, knowledge_base_id TEXT, tag_id TEXT,
		content TEXT NOT NULL, dimension INTEGER NOT NULL,
		embedding halfvec NOT NULL, is_enabled BOOLEAN NOT NULL DEFAULT TRUE,
		created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
		updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP);
		CREATE UNIQUE INDEX embeddings_unique_source ON embeddings(source_id, source_type)`).Error)
	return db
}

func nextcloudVectorKnowledge(t *testing.T, db *gorm.DB, id, channel string) {
	t.Helper()
	meta := map[string]string{}
	if channel == types.ConnectorTypeNextcloud {
		meta = map[string]string{
			"datasource_id": "ds", "external_id": "nextcloud:instance:77",
			"nextcloud_etag": "", "nextcloud_target_etag": "",
		}
	}
	encoded, err := json.Marshal(meta)
	require.NoError(t, err)
	require.NoError(t, db.Create(&types.Knowledge{
		ID: id, TenantID: 7,
		KnowledgeBaseID: "kb", Channel: channel, Type: "file",
		ParseStatus: types.ParseStatusCompleted, EnableStatus: "enabled",
		Metadata: types.JSON(encoded),
	}).Error)
}

func nextcloudVectorInfo(knowledgeID, sourceID string) *types.IndexInfo {
	return &types.IndexInfo{
		KnowledgeID: knowledgeID, KnowledgeBaseID: "kb",
		SourceID: sourceID, ChunkID: sourceID, SourceType: types.ChunkSourceType,
		Content: "ORCHID-QUARTZ-2749", IsEnabled: true,
	}
}

func nextcloudVectorEmbedding(sourceIDs ...string) map[string]any {
	embeddings := make(map[string][]float32, len(sourceIDs))
	for _, sourceID := range sourceIDs {
		embeddings[sourceID] = []float32{1, 0}
	}
	return map[string]any{"embedding": embeddings}
}

func nextcloudVectorTestScope(id string) apprepo.NextcloudContentScope {
	return apprepo.NextcloudContentScope{
		TenantID: 7, KnowledgeBaseID: "kb",
		KnowledgeID: id, DataSourceID: "ds", ExternalID: "nextcloud:instance:77",
	}
}

func nextcloudVectorStage(t *testing.T, db *gorm.DB, etag, id string) {
	t.Helper()
	store := apprepo.NewKnowledgeRepository(db).(nextcloudVectorVersionStore)
	require.NoError(t, store.StageNextcloudVersion(context.Background(), 7, "kb", "ds",
		"nextcloud:instance:77", etag, id))
}

func nextcloudVectorLease(
	t *testing.T,
	db *gorm.DB,
	id string,
) (context.Context, *apprepo.NextcloudContentLeaseStore, apprepo.NextcloudContentLease) {
	t.Helper()
	store := apprepo.NewNextcloudContentLeaseStore(db)
	lease, err := store.AcquireKnowledge(context.Background(), nextcloudVectorTestScope(id),
		apprepo.NextcloudContentBuildLease, "vector-test", time.Minute)
	require.NoError(t, err)
	return apprepo.WithNextcloudBuildLease(context.Background(), store, lease,
		nextcloudVectorTestScope(id)), store, lease
}

func TestNextcloudPGVectorWriteAndStageSerialize(t *testing.T) {
	db := nextcloudVectorTestDB(t)
	ctx := context.Background()
	nextcloudVectorKnowledge(t, db, "old", types.ConnectorTypeNextcloud)
	nextcloudVectorStage(t, db, "etag-old", "old")
	nextcloudVectorKnowledge(t, db, "new", types.ConnectorTypeNextcloud)
	buildCtx, leases, lease := nextcloudVectorLease(t, db, "old")
	defer func() { require.NoError(t, leases.ReleaseLease(ctx, lease.ID)) }()
	repo := &pgRepository{db: db}
	blocker := db.Begin()
	require.NoError(t, blocker.Error)
	defer blocker.Rollback()
	require.NoError(t, blocker.Exec(`LOCK TABLE embeddings IN ACCESS EXCLUSIVE MODE`).Error)
	writeDone := make(chan error, 1)
	go func() {
		writeDone <- repo.BatchSave(buildCtx,
			[]*types.IndexInfo{nextcloudVectorInfo("old", "old-chunk")},
			nextcloudVectorEmbedding("old-chunk"))
	}()
	deadline := time.Now().Add(5 * time.Second)
	for {
		var waiting int64
		require.NoError(t, db.Raw(`SELECT count(*) FROM pg_locks l
			JOIN pg_class c ON c.oid = l.relation
			JOIN pg_namespace n ON n.oid = c.relnamespace
			WHERE NOT l.granted AND n.nspname = current_schema()
			AND c.relname = 'embeddings'`).Scan(&waiting).Error)
		if waiting > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("vector writer never reached the held embeddings table")
		}
		time.Sleep(20 * time.Millisecond)
	}
	stageDone := make(chan error, 1)
	go func() {
		stageDone <- apprepo.NewKnowledgeRepository(db).(nextcloudVectorVersionStore).
			StageNextcloudVersion(ctx, 7, "kb", "ds", "nextcloud:instance:77", "etag-new", "new")
	}()
	select {
	case err := <-stageDone:
		t.Fatalf("stage crossed the vector writer's uncommitted fence: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	require.NoError(t, blocker.Commit().Error)
	select {
	case err := <-writeDone:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("vector write did not finish")
	}
	select {
	case err := <-stageDone:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("source stage did not finish")
	}
	require.ErrorIs(t, repo.BatchSave(buildCtx,
		[]*types.IndexInfo{nextcloudVectorInfo("old", "late-chunk")},
		nextcloudVectorEmbedding("late-chunk")), apprepo.ErrNextcloudContentLeaseDenied)
	var late int64
	require.NoError(t, db.Table("embeddings").Where("source_id = ?", "late-chunk").Count(&late).Error)
	require.Zero(t, late)
	created, err := apprepo.NewNextcloudGCStore(db).InventoryRetired(ctx, time.Now().UTC(), 10)
	require.NoError(t, err)
	require.Equal(t, 1, created)
	var inventoried int64
	require.NoError(t, db.Table("nextcloud_gc_items").Where("kind = ?", "postgres_embedding").Count(&inventoried).Error)
	require.Equal(t, int64(1), inventoried)
}

func TestNextcloudPGVectorInventoryCrashRetryAndExactClaimDelete(t *testing.T) {
	db := nextcloudVectorTestDB(t)
	ctx := context.Background()
	nextcloudVectorKnowledge(t, db, "old", types.ConnectorTypeNextcloud)
	nextcloudVectorStage(t, db, "etag-old", "old")
	buildCtx, leases, lease := nextcloudVectorLease(t, db, "old")
	repo := &pgRepository{db: db}
	jobID := uuid.NewString()
	require.NoError(t, db.Exec(`INSERT INTO nextcloud_gc_jobs
		(id, tenant_id, knowledge_base_id, datasource_id, external_id, knowledge_id,
		 reason, state, not_before, original_not_before, next_attempt_at)
		VALUES (?, 7, 'kb', 'ds', 'nextcloud:instance:77', 'old',
		 'retired', 'blocked', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`, jobID).Error)
	require.NoError(t, db.Exec(`INSERT INTO nextcloud_gc_items
		(job_id, kind, object_ref, state) VALUES (?, 'derived_index', 'old', 'blocked')`, jobID).Error)
	require.NoError(t, db.Exec(`CREATE FUNCTION fail_vector_receipt() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN IF NEW.kind = 'postgres_embedding' THEN RAISE EXCEPTION 'simulated receipt crash'; END IF;
		RETURN NEW; END $$;
		CREATE TRIGGER fail_vector_receipt BEFORE INSERT ON nextcloud_gc_items
		FOR EACH ROW EXECUTE FUNCTION fail_vector_receipt()`).Error)
	info := nextcloudVectorInfo("old", "chunk-1")
	require.ErrorContains(t, repo.BatchSave(buildCtx, []*types.IndexInfo{info},
		nextcloudVectorEmbedding(info.SourceID)), "simulated receipt crash")
	var vectors, receipts int64
	require.NoError(t, db.Table("embeddings").Count(&vectors).Error)
	require.NoError(t, db.Table("nextcloud_gc_items").Where("kind = 'postgres_embedding'").Count(&receipts).Error)
	require.Zero(t, vectors)
	require.Zero(t, receipts)
	require.NoError(t, db.Exec(`DROP TRIGGER fail_vector_receipt ON nextcloud_gc_items`).Error)
	require.NoError(t, repo.BatchSave(buildCtx, []*types.IndexInfo{info},
		nextcloudVectorEmbedding(info.SourceID)))
	require.NoError(t, repo.BatchSave(buildCtx, []*types.IndexInfo{info},
		nextcloudVectorEmbedding(info.SourceID))) // conflict retry inventories once
	other := nextcloudVectorInfo("old", "chunk-2")
	require.NoError(t, repo.Save(buildCtx, other, nextcloudVectorEmbedding(other.SourceID)))
	require.NoError(t, db.Table("embeddings").Count(&vectors).Error)
	require.NoError(t, db.Table("nextcloud_gc_items").Where("kind = 'postgres_embedding'").Count(&receipts).Error)
	require.Equal(t, int64(2), vectors)
	require.Equal(t, int64(2), receipts)
	retrieve := types.RetrieveParams{
		RetrieverType: types.VectorRetrieverType,
		Embedding:     []float32{1, 0}, KnowledgeBaseIDs: []string{"kb"},
		KnowledgeIDs: []string{"old"}, TopK: 5, Threshold: 0,
	}
	before, err := repo.VectorRetrieve(ctx, retrieve)
	require.NoError(t, err)
	require.Len(t, before, 1)
	require.Len(t, before[0].Results, 2)
	var ids []string
	require.NoError(t, db.Table("nextcloud_gc_items").Where("job_id = ? AND kind = 'postgres_embedding'", jobID).
		Order("object_ref").Pluck("object_ref", &ids).Error)
	require.Len(t, ids, 2)
	require.NoError(t, leases.ReleaseLease(ctx, lease.ID))
	nextcloudVectorKnowledge(t, db, "new", types.ConnectorTypeNextcloud)
	nextcloudVectorStage(t, db, "etag-new", "new")
	var retiredMS int64
	require.NoError(t, db.Raw(`SELECT retired_at_ms FROM nextcloud_content_fences
		WHERE tenant_id = 7 AND knowledge_base_id = 'kb' AND knowledge_id = 'old'`).Scan(&retiredMS).Error)
	nowMS := time.Now().UnixMilli()
	require.NoError(t, db.Exec(`INSERT INTO nextcloud_content_lease_coverage
		(tenant_id, knowledge_base_id, activated_at_ms, legacy_drained_at_ms,
		 reader_revision, builder_revision) VALUES (7, 'kb', ?, ?, 'test-reader', 'test-builder')`,
		retiredMS-1000, nowMS).Error)
	claim, err := leases.ClaimKnowledgeGC(ctx, nextcloudVectorTestScope("old"),
		time.UnixMilli(retiredMS), time.Minute)
	require.NoError(t, err)
	// A caller can present a valid claim with the fence retirement time even
	// while the durable job's policy delay is still open. The exact delete must
	// consult that job, leaving both rows and receipts untouched until due.
	require.NoError(t, db.Exec(`UPDATE nextcloud_gc_jobs
		SET not_before = clock_timestamp() + INTERVAL '1 hour' WHERE id = ?`, jobID).Error)
	require.ErrorIs(t, repo.DeleteNextcloudExactVectorForGC(ctx, claim, jobID, ids[0]),
		apprepo.ErrNextcloudContentGCBusy)
	require.NoError(t, db.Table("embeddings").Where("knowledge_id = 'old'").Count(&vectors).Error)
	require.Equal(t, int64(2), vectors)
	var collectedBeforeDue int64
	require.NoError(t, db.Table("nextcloud_gc_items").Where(
		"job_id = ? AND kind = 'postgres_embedding' AND state = 'collected'", jobID).
		Count(&collectedBeforeDue).Error)
	require.Zero(t, collectedBeforeDue)
	require.NoError(t, db.Exec(`UPDATE nextcloud_gc_jobs
		SET not_before = clock_timestamp() - INTERVAL '1 second' WHERE id = ?`, jobID).Error)
	for _, id := range ids {
		require.NoError(t, repo.DeleteNextcloudExactVectorForGC(ctx, claim, jobID, id))
		require.NoError(t, repo.DeleteNextcloudExactVectorForGC(ctx, claim, jobID, id))
	}
	require.NoError(t, db.Table("embeddings").Where("knowledge_id = 'old'").Count(&vectors).Error)
	require.Zero(t, vectors, "old vectors are no longer retrievable even by direct pgvector lookup")
	after, err := repo.VectorRetrieve(ctx, retrieve)
	require.NoError(t, err)
	require.Len(t, after, 1)
	require.Empty(t, after[0].Results)
	var collected int64
	require.NoError(t, db.Table("nextcloud_gc_items").Where(
		"job_id = ? AND kind = 'postgres_embedding' AND state = 'collected'", jobID).Count(&collected).Error)
	require.Equal(t, int64(2), collected)
	var state string
	require.NoError(t, db.Table("nextcloud_gc_jobs").Select("state").Where("id = ?", jobID).Scan(&state).Error)
	require.Equal(t, "blocked", state)
	require.NoError(t, db.Table("nextcloud_gc_items").Select("state").Where(
		"job_id = ? AND kind = 'derived_index'", jobID).Scan(&state).Error)
	require.Equal(t, "blocked", state)
	require.NoError(t, leases.FinishGCClaim(ctx, claim, false))
	require.ErrorIs(t, repo.DeleteNextcloudExactVectorForGC(ctx, claim, jobID, ids[0]),
		apprepo.ErrNextcloudContentLeaseDenied)
}

func TestNextcloudPGVectorBackendCrashBeforeInventoryCommitRollsBack(t *testing.T) {
	db := nextcloudVectorTestDB(t)
	ctx := context.Background()
	nextcloudVectorKnowledge(t, db, "old", types.ConnectorTypeNextcloud)
	nextcloudVectorStage(t, db, "etag-old", "old")
	buildCtx, leases, lease := nextcloudVectorLease(t, db, "old")
	defer func() { require.NoError(t, leases.ReleaseLease(ctx, lease.ID)) }()
	jobID := uuid.NewString()
	require.NoError(t, db.Exec(`INSERT INTO nextcloud_gc_jobs
		(id, tenant_id, knowledge_base_id, datasource_id, external_id, knowledge_id,
		 reason, state, not_before, original_not_before, next_attempt_at)
		VALUES (?, 7, 'kb', 'ds', 'nextcloud:instance:77', 'old',
		 'retired', 'blocked', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`, jobID).Error)
	require.NoError(t, db.Exec(`INSERT INTO nextcloud_gc_items
		(job_id, kind, object_ref, state) VALUES (?, 'derived_index', 'old', 'blocked')`, jobID).Error)
	blocker := db.Begin()
	require.NoError(t, blocker.Error)
	defer blocker.Rollback()
	require.NoError(t, blocker.Exec(`LOCK TABLE nextcloud_gc_items IN ACCESS EXCLUSIVE MODE`).Error)
	repo := &pgRepository{db: db}
	info := nextcloudVectorInfo("old", "crash-chunk")
	writeDone := make(chan error, 1)
	go func() {
		writeDone <- repo.BatchSave(buildCtx, []*types.IndexInfo{info},
			nextcloudVectorEmbedding(info.SourceID))
	}()
	deadline := time.Now().Add(5 * time.Second)
	var pid int
	for {
		require.NoError(t, db.Raw(`SELECT COALESCE((SELECT l.pid FROM pg_locks l
			JOIN pg_class c ON c.oid = l.relation
			JOIN pg_namespace n ON n.oid = c.relnamespace
			JOIN pg_stat_activity a ON a.pid = l.pid
			WHERE NOT l.granted AND n.nspname = current_schema()
			AND c.relname = 'nextcloud_gc_items'
			AND a.query ILIKE '%INSERT INTO nextcloud_gc_items%'
			ORDER BY l.pid LIMIT 1), 0)`).Scan(&pid).Error)
		if pid > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("writer never reached the held inventory table")
		}
		time.Sleep(20 * time.Millisecond)
	}
	var terminated bool
	require.NoError(t, db.Raw(`SELECT pg_terminate_backend(?)`, pid).Scan(&terminated).Error)
	require.True(t, terminated)
	require.NoError(t, blocker.Commit().Error)
	select {
	case err := <-writeDone:
		require.Error(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("crashed writer did not return")
	}
	var vectors, receipts int64
	require.NoError(t, db.Table("embeddings").Count(&vectors).Error)
	require.NoError(t, db.Table("nextcloud_gc_items").Where("kind = 'postgres_embedding'").Count(&receipts).Error)
	require.Zero(t, vectors)
	require.Zero(t, receipts)
	require.NoError(t, repo.BatchSave(buildCtx, []*types.IndexInfo{info},
		nextcloudVectorEmbedding(info.SourceID)))
	require.NoError(t, db.Table("embeddings").Count(&vectors).Error)
	require.NoError(t, db.Table("nextcloud_gc_items").Where("kind = 'postgres_embedding'").Count(&receipts).Error)
	require.Equal(t, int64(1), vectors)
	require.Equal(t, int64(1), receipts)
}

func TestNextcloudPGVectorSameCandidateETagFencesOldLeaseAndOrdinaryWrites(t *testing.T) {
	db := nextcloudVectorTestDB(t)
	ctx := context.Background()
	nextcloudVectorKnowledge(t, db, "same", types.ConnectorTypeNextcloud)
	nextcloudVectorStage(t, db, "etag-1", "same")
	oldCtx, leases, oldLease := nextcloudVectorLease(t, db, "same")
	nextcloudVectorStage(t, db, "etag-2", "same")
	repo := &pgRepository{db: db}
	require.ErrorIs(t, repo.BatchSave(oldCtx,
		[]*types.IndexInfo{nextcloudVectorInfo("same", "stale")},
		nextcloudVectorEmbedding("stale")), apprepo.ErrNextcloudContentLeaseDenied)
	newCtx, _, newLease := nextcloudVectorLease(t, db, "same")
	require.Greater(t, newLease.Epoch, oldLease.Epoch)
	require.NoError(t, repo.BatchSave(newCtx,
		[]*types.IndexInfo{nextcloudVectorInfo("same", "current")},
		nextcloudVectorEmbedding("current")))
	require.NoError(t, leases.ReleaseLease(ctx, oldLease.ID))
	require.NoError(t, leases.ReleaseLease(ctx, newLease.ID))
	nextcloudVectorKnowledge(t, db, "manual", types.ChannelWeb)
	require.NoError(t, repo.Save(ctx, nextcloudVectorInfo("manual", "ordinary-1"),
		nextcloudVectorEmbedding("ordinary-1")))
	require.NoError(t, repo.BatchSave(ctx,
		[]*types.IndexInfo{nextcloudVectorInfo("manual", "ordinary-2")},
		nextcloudVectorEmbedding("ordinary-2")))
	// Historic non-document index callers keep their previous behavior.
	require.NoError(t, repo.BatchSave(ctx,
		[]*types.IndexInfo{{SourceID: "ordinary-unscoped", Content: "ordinary", IsEnabled: true}},
		nextcloudVectorEmbedding("ordinary-unscoped")))
	var count int64
	require.NoError(t, db.Table("embeddings").Where("source_id LIKE 'ordinary-%'").Count(&count).Error)
	require.Equal(t, int64(3), count)
	require.ErrorIs(t, repo.Save(ctx, nextcloudVectorInfo("same", "unleased"),
		nextcloudVectorEmbedding("unleased")), apprepo.ErrNextcloudContentLeaseInvalid)
	var leaked int64
	require.NoError(t, db.Table(
		"embeddings",
	).Where("source_id IN ?", []string{"stale", "unleased"}).Count(&leaked).Error)
	require.Zero(t, leaked)
}
