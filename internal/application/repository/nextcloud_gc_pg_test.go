package repository

import (
	"context"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// A real PostgreSQL row lock is required for the bind/GC race. The test uses
// a disposable schema and is opt-in so the normal unit suite needs no server.
func TestPostgresNextcloudGCBindRaceSerializesOnResource(t *testing.T) {
	dsn := os.Getenv("WEKNORA_GC_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("set WEKNORA_GC_TEST_POSTGRES_DSN for disposable PostgreSQL")
	}
	admin, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	require.NoError(t, err)
	adminSQL, err := admin.DB()
	require.NoError(t, err)
	defer func() { require.NoError(t, adminSQL.Close()) }()
	schema := "gc_bind_" + strings.ReplaceAll(uuid.NewString()[:8], "-", "")
	require.NoError(t, admin.Exec(`CREATE SCHEMA "`+schema+`"`).Error)
	defer admin.Exec(`DROP SCHEMA "` + schema + `" CASCADE`)
	scopedDSN := dsn + " search_path=" + schema
	if strings.Contains(dsn, "://") {
		u, err := url.Parse(dsn)
		require.NoError(t, err)
		q := u.Query()
		q.Set("search_path", schema)
		u.RawQuery = q.Encode()
		scopedDSN = u.String()
	}
	db, err := gorm.Open(postgres.Open(scopedDSN), &gorm.Config{})
	require.NoError(t, err)
	sqlDB, err := db.DB()
	require.NoError(t, err)
	sqlDB.SetMaxOpenConns(5)
	defer func() { require.NoError(t, sqlDB.Close()) }()
	require.NoError(t, db.Exec(`CREATE TABLE knowledge_bases (id TEXT PRIMARY KEY,
		tenant_id BIGINT NOT NULL, ever_had_nextcloud_source BOOLEAN NOT NULL DEFAULT TRUE)`).Error)
	require.NoError(t, db.Exec(`INSERT INTO knowledge_bases (id, tenant_id) VALUES ('kb', 7)`).Error)
	require.NoError(t, db.AutoMigrate(&types.Knowledge{}, &types.Chunk{}, &nextcloudSourceVersion{},
		&nextcloudGCJob{}, &nextcloudGCItem{}, &types.StoredResource{}, &types.ResourceBinding{},
		&NextcloudIndexedWithdrawal{}))
	now := time.Now().UTC()
	ref := nextcloudGCTestLocalResource(t, db, strings.Repeat("p", 22), "old")
	nextcloudGCTestSource(t, db, "old", ref, now.Add(-8*24*time.Hour))
	store := NewNextcloudGCStore(db)
	_, err = store.InventoryRetired(context.Background(), now, 100)
	require.NoError(t, err)
	var job nextcloudGCJob
	require.NoError(t, db.Take(&job).Error)
	var resource types.StoredResource
	require.NoError(t, db.Where("handle = ?", strings.Repeat("p", 22)).Take(&resource).Error)
	hold := db.Begin()
	require.NoError(t, hold.Error)
	require.NoError(t, hold.Clauses(clause.Locking{Strength: "UPDATE"}).Where("id = ?", resource.ID).
		Take(&types.StoredResource{}).Error)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	type claimResult struct {
		claim nextcloudGCObjectClaim
		err   error
	}
	claimDone := make(chan claimResult, 1)
	go func() {
		claim, err := store.claimObject(ctx, job.ID, "source_file", ref, now)
		claimDone <- claimResult{claim, err}
	}()
	bindDone := make(chan error, 1)
	go func() {
		bindDone <- (&resourceRepository{db: db}).CreateBinding(ctx, &types.ResourceBinding{
			ResourceID: resource.ID, TenantID: 7, OwnerType: types.ResourceOwnerMessage,
			OwnerID: "concurrent-owner", Relation: types.ResourceRelationArtifact,
		})
	}()
	// Both operations must wait for the same resource row. Once released,
	// either order is allowed; a successful bind and deletion claim must
	// never coexist.
	time.Sleep(100 * time.Millisecond)
	require.NoError(t, hold.Commit().Error)
	var got claimResult
	select {
	case got = <-claimDone:
	case <-ctx.Done():
		t.Fatal("GC claim did not finish")
	}
	var bindErr error
	select {
	case bindErr = <-bindDone:
	case <-ctx.Done():
		t.Fatal("concurrent bind did not finish")
	}
	require.NoError(t, got.err)
	require.NoError(t, db.Unscoped().Where("id = ?", resource.ID).Take(&resource).Error)
	if bindErr == nil {
		require.Equal(t, types.ResourceStateActive, resource.State)
		require.Nil(t, got.claim.resource)
	} else {
		require.Equal(t, types.ResourceStateDeleting, resource.State)
		require.NotNil(t, got.claim.resource)
	}
}
