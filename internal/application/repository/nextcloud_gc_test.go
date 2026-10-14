package repository

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	filesvc "github.com/Tencent/WeKnora/internal/application/service/file"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func nextcloudGCTestDB(t *testing.T) (*NextcloudGCStore, *gorm.DB) {
	t.Helper()
	db, err := gorm.Open(sqlite.Open("file:"+uuid.NewString()+"?mode=memory&cache=shared"), &gorm.Config{})
	require.NoError(t, err)
	sqlDB, err := db.DB()
	require.NoError(t, err)
	sqlDB.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = sqlDB.Close() })
	require.NoError(t, db.AutoMigrate(&types.Knowledge{}, &types.Chunk{}, &nextcloudSourceVersion{},
		&nextcloudGCJob{}, &nextcloudGCItem{}, &types.StoredResource{}, &types.ResourceBinding{}))
	return NewNextcloudGCStore(db), db
}

func nextcloudGCTestLocalResource(t *testing.T, db *gorm.DB, handle, knowledgeID string) string {
	t.Helper()
	ref := types.BuildResourcePath(handle)
	resource := &types.StoredResource{
		ID: uuid.NewString(), Handle: handle, TenantID: 7,
		StorageBackendID: "backend", Provider: "local",
		PhysicalPath: "storage://backend/local://7/" + knowledgeID + "/source.txt",
		LocationHash: uuid.NewString(), SourceProvenance: types.ResourceProvenanceNextcloud,
		Size: 23, State: types.ResourceStateActive,
	}
	require.NoError(t, db.Create(resource).Error)
	require.NoError(t, db.Create(&types.ResourceBinding{
		ResourceID: resource.ID, TenantID: 7,
		OwnerType: types.ResourceOwnerKnowledge, OwnerID: knowledgeID,
		Relation: types.ResourceRelationSourceFile,
	}).Error)
	return ref
}

func TestNextcloudGCPhysicalLocalDeleteFailureRetryAndConfirmedBytes(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC()
	store, db := nextcloudGCTestDB(t)
	ref := nextcloudGCTestLocalResource(t, db, strings.Repeat("a", 22), "old")
	nextcloudGCTestSource(t, db, "old", ref, now.Add(-8*24*time.Hour))
	created, err := store.InventoryRetired(ctx, now, 100)
	require.NoError(t, err)
	require.Equal(t, 1, created)
	deletes := 0
	store.ConfigureLocalDelete(func(_ context.Context, resource *types.StoredResource) (bool, error) {
		deletes++
		require.Equal(t, types.ResourceStateDeleting, resource.State)
		if deletes == 1 {
			return false, errors.New("disk temporarily unavailable")
		}
		return true, nil
	})
	processed, err := store.RunDue(ctx, now, 100)
	require.NoError(t, err)
	require.Equal(t, 1, processed)
	var resource types.StoredResource
	require.NoError(t, db.Unscoped().Where("handle = ?", strings.Repeat("a", 22)).Take(&resource).Error)
	require.Equal(t, types.ResourceStateDeleting, resource.State)
	var bindings int64
	require.NoError(t, db.Model(&types.ResourceBinding{}).Where("resource_id = ?", resource.ID).Count(&bindings).Error)
	require.Zero(t, bindings)
	status, err := store.ListStatus(ctx, 7, 100)
	require.NoError(t, err)
	require.Equal(t, "provider_delete_failed", status[0].LastErrorCode)
	require.Zero(t, status[0].ConfirmedReleasedBytes)
	require.Error(t, (&resourceRepository{db: db}).CreateBinding(ctx, &types.ResourceBinding{
		ResourceID: resource.ID, TenantID: 7, OwnerType: types.ResourceOwnerMessage,
		OwnerID: "late-owner", Relation: types.ResourceRelationArtifact,
	}), "a new owner cannot bind after the GC claim")
	processed, err = store.RunDue(ctx, now.Add(time.Hour), 100)
	require.NoError(t, err)
	require.Equal(t, 1, processed)
	require.Equal(t, 2, deletes)
	status, err = store.ListStatus(ctx, 7, 100)
	require.NoError(t, err)
	require.Equal(t, int64(23), status[0].ConfirmedReleasedBytes)
	require.NoError(t, db.Unscoped().Where("id = ?", resource.ID).Take(&resource).Error)
	require.Equal(t, types.ResourceStateDeleted, resource.State)
	processed, err = store.RunDue(ctx, now.Add(2*time.Hour), 100)
	require.NoError(t, err)
	require.Equal(t, 1, processed)
	status, err = store.ListStatus(ctx, 7, 100)
	require.NoError(t, err)
	require.Equal(t, "blocked", status[0].State)
	require.Equal(t, "derived_provenance_unverified", status[0].LastErrorCode)
	require.Equal(t, int64(23), status[0].ConfirmedReleasedBytes)
	require.Equal(t, 2, deletes, "a completed object cannot be deleted twice")
}

func TestNextcloudGCSharedResourceReleasesOnlyRetiredClaim(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC()
	store, db := nextcloudGCTestDB(t)
	ref := nextcloudGCTestLocalResource(t, db, strings.Repeat("b", 22), "old")
	nextcloudGCTestSource(t, db, "old", ref, now.Add(-8*24*time.Hour))
	var resource types.StoredResource
	require.NoError(t, db.Where("handle = ?", strings.Repeat("b", 22)).Take(&resource).Error)
	require.NoError(t, db.Create(&types.ResourceBinding{
		ResourceID: resource.ID, TenantID: 7,
		OwnerType: types.ResourceOwnerMessage, OwnerID: "still-reading",
		Relation: types.ResourceRelationArtifact,
	}).Error)
	_, err := store.InventoryRetired(ctx, now, 100)
	require.NoError(t, err)
	deletes := 0
	store.ConfigureLocalDelete(func(context.Context, *types.StoredResource) (
		bool,
		error,
	) {
		deletes++
		return true, nil
	})
	_, err = store.RunDue(ctx, now, 100)
	require.NoError(t, err)
	require.Zero(t, deletes)
	require.NoError(t, db.Where("id = ?", resource.ID).Take(&resource).Error)
	require.Equal(t, types.ResourceStateActive, resource.State)
	var bindings []types.ResourceBinding
	require.NoError(t, db.Where("resource_id = ?", resource.ID).Find(&bindings).Error)
	require.Len(t, bindings, 1)
	require.Equal(t, types.ResourceOwnerMessage, bindings[0].OwnerType)
	status, err := store.ListStatus(ctx, 7, 100)
	require.NoError(t, err)
	require.Zero(t, status[0].ConfirmedReleasedBytes)
}

func TestNextcloudGCUnscopedOrCloudObjectStaysBlockedWithBinding(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC()
	for _, testCase := range []struct {
		name, provider, path, backend string
	}{
		{"unscoped local", "local", "local://7/old/source.txt", ""},
		{"cloud", "minio", "storage://backend/minio://bucket/7/old/source.txt", "backend"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			store, db := nextcloudGCTestDB(t)
			handle := strings.Repeat("e", 22)
			ref := types.BuildResourcePath(handle)
			resource := &types.StoredResource{
				ID: uuid.NewString(), Handle: handle, TenantID: 7,
				StorageBackendID: testCase.backend, Provider: testCase.provider,
				PhysicalPath: testCase.path, LocationHash: uuid.NewString(),
				SourceProvenance: types.ResourceProvenanceNextcloud, Size: 23,
				State: types.ResourceStateActive,
			}
			require.NoError(t, db.Create(resource).Error)
			require.NoError(t, db.Create(&types.ResourceBinding{
				ResourceID: resource.ID, TenantID: 7,
				OwnerType: types.ResourceOwnerKnowledge, OwnerID: "old",
				Relation: types.ResourceRelationSourceFile,
			}).Error)
			nextcloudGCTestSource(t, db, "old", ref, now.Add(-8*24*time.Hour))
			_, err := store.InventoryRetired(ctx, now, 100)
			require.NoError(t, err)
			called := false
			store.ConfigureLocalDelete(func(context.Context, *types.StoredResource) (
				bool,
				error,
			) {
				called = true
				return true, nil
			})
			_, err = store.RunDue(ctx, now, 100)
			require.NoError(t, err)
			require.False(t, called)
			var count int64
			require.NoError(t, db.Model(&types.ResourceBinding{}).
				Where("resource_id = ?", resource.ID).Count(&count).Error)
			require.Equal(t, int64(1), count)
			var after types.StoredResource
			require.NoError(t, db.Where("id = ?", resource.ID).Take(&after).Error)
			require.Equal(t, types.ResourceStateActive, after.State)
			status, err := store.ListStatus(ctx, 7, 100)
			require.NoError(t, err)
			require.Equal(t, "unsupported_storage_backend", status[0].LastErrorCode)
			require.Zero(t, status[0].ConfirmedReleasedBytes)
		})
	}
}

func TestNextcloudGCObjectLeaseAllowsCrashRecoveryWithoutDoubleCredit(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC()
	store, db := nextcloudGCTestDB(t)
	ref := nextcloudGCTestLocalResource(t, db, strings.Repeat("c", 22), "old")
	nextcloudGCTestSource(t, db, "old", ref, now.Add(-8*24*time.Hour))
	_, err := store.InventoryRetired(ctx, now, 100)
	require.NoError(t, err)
	var job nextcloudGCJob
	require.NoError(t, db.Take(&job).Error)
	first, err := store.claimObject(ctx, job.ID, "source_file", ref, now)
	require.NoError(t, err)
	require.NotNil(t, first.resource)
	require.NotEmpty(t, first.token)
	concurrent, err := store.claimObject(ctx, job.ID, "source_file", ref, now.Add(time.Minute))
	require.NoError(t, err)
	require.Nil(t, concurrent.resource)
	require.Equal(t, "resource_delete_in_progress", concurrent.code)
	recovered, err := store.claimObject(ctx, job.ID, "source_file", ref, now.Add(11*time.Minute))
	require.NoError(t, err)
	require.NotNil(t, recovered.resource)
	require.NotEqual(t, first.token, recovered.token)
	require.Error(t, store.finishObject(ctx, job.ID, "source_file", ref,
		first.resource.ID, first.token, true, now.Add(11*time.Minute)))
	require.NoError(t, store.finishObject(ctx, job.ID, "source_file", ref,
		recovered.resource.ID, recovered.token, true, now.Add(11*time.Minute)))
	var item nextcloudGCItem
	require.NoError(t, db.Where("job_id = ? AND kind = ?", job.ID, "source_file").Take(&item).Error)
	require.Equal(t, int64(23), item.ConfirmedReleasedBytes)
	require.Equal(t, "collected", item.State)
}

func TestNextcloudGCScopedLocalProviderCrashAfterUnlinkRetriesExactly(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC()
	store, db := nextcloudGCTestDB(t)
	ref := nextcloudGCTestLocalResource(t, db, strings.Repeat("d", 22), "old")
	nextcloudGCTestSource(t, db, "old", ref, now.Add(-8*24*time.Hour))
	_, err := store.InventoryRetired(ctx, now, 100)
	require.NoError(t, err)
	base := t.TempDir()
	path := filepath.Join(base, "7", "old", "source.txt")
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
	require.NoError(t, os.WriteFile(path, []byte("persisted-source-object"), 0o600))
	provider := filesvc.NewBackendScopedFileService("backend", filesvc.NewLocalFileService(base, ""))
	attempts := 0
	store.ConfigureLocalDelete(func(ctx context.Context, resource *types.StoredResource) (bool, error) {
		attempts++
		err := provider.DeleteFile(ctx, resource.PhysicalPath)
		if errors.Is(err, os.ErrNotExist) {
			return false, nil
		}
		if err != nil {
			return false, err
		}
		if attempts == 1 {
			return false, errors.New("simulated crash before DB acknowledgement")
		}
		return true, nil
	})
	_, err = store.RunDue(ctx, now, 100)
	require.NoError(t, err)
	_, err = os.Stat(path)
	require.ErrorIs(t, err, os.ErrNotExist)
	status, err := store.ListStatus(ctx, 7, 100)
	require.NoError(t, err)
	require.Zero(t, status[0].ConfirmedReleasedBytes)
	_, err = store.RunDue(ctx, now.Add(time.Hour), 100)
	require.NoError(t, err)
	status, err = store.ListStatus(ctx, 7, 100)
	require.NoError(t, err)
	require.Zero(t, status[0].ConfirmedReleasedBytes, ("a crash before acknowledgement leaves physical bytes un" +
		"confirmed"))
	require.Equal(t, 2, attempts)
}

func TestNextcloudGCImageAliasCannotShortenOriginalRetention(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC()
	store, db := nextcloudGCTestDB(t)
	ref := nextcloudGCTestLocalResource(t, db, strings.Repeat("f", 22), "old")
	nextcloudGCTestSource(t, db, "old", ref, now.Add(-2*time.Hour))
	require.NoError(t, db.Create(&types.Chunk{
		ID: "chunk", TenantID: 7, KnowledgeBaseID: "kb",
		KnowledgeID: "old", ImageInfo: `[{"url":"` + ref + `"}]`,
	}).Error)
	_, err := store.InventoryRetired(ctx, now, 100)
	require.NoError(t, err)
	deletes := 0
	store.ConfigureLocalDelete(func(context.Context, *types.StoredResource) (
		bool,
		error,
	) {
		deletes++
		return true, nil
	})
	_, err = store.RunDue(ctx, now, 100)
	require.NoError(t, err)
	require.Zero(t, deletes)
	var resource types.StoredResource
	require.NoError(t, db.Where("handle = ?", strings.Repeat("f", 22)).Take(&resource).Error)
	require.Equal(t, types.ResourceStateActive, resource.State)
	var bindings int64
	require.NoError(t, db.Model(&types.ResourceBinding{}).Where("resource_id = ?", resource.ID).Count(&bindings).Error)
	require.Equal(t, int64(1), bindings)
	_, err = store.RunDue(ctx, now.Add(7*24*time.Hour), 100)
	require.NoError(t, err)
	require.Equal(t, 1, deletes)
	status, err := store.ListStatus(ctx, 7, 100)
	require.NoError(t, err)
	require.Equal(t, int64(23), status[0].ConfirmedReleasedBytes)
}

func TestNextcloudGCInvalidImageInventoryStopsPhysicalRelease(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC()
	store, db := nextcloudGCTestDB(t)
	ref := nextcloudGCTestLocalResource(t, db, strings.Repeat("g", 22), "old")
	nextcloudGCTestSource(t, db, "old", ref, now.Add(-8*24*time.Hour))
	require.NoError(t, db.Create(&types.Chunk{
		ID: "chunk", TenantID: 7, KnowledgeBaseID: "kb",
		KnowledgeID: "old", ImageInfo: `{broken`,
	}).Error)
	_, err := store.InventoryRetired(ctx, now, 100)
	require.NoError(t, err)
	deletes := 0
	store.ConfigureLocalDelete(func(context.Context, *types.StoredResource) (
		bool,
		error,
	) {
		deletes++
		return true, nil
	})
	_, err = store.RunDue(ctx, now.Add(time.Hour), 100)
	require.NoError(t, err)
	require.Zero(t, deletes)
	var resource types.StoredResource
	require.NoError(t, db.Where("handle = ?", strings.Repeat("g", 22)).Take(&resource).Error)
	require.Equal(t, types.ResourceStateActive, resource.State)
	var bindings int64
	require.NoError(t, db.Model(&types.ResourceBinding{}).Where("resource_id = ?", resource.ID).Count(&bindings).Error)
	require.Equal(t, int64(1), bindings)
}

func TestNextcloudGCRetryInventoriesLateImageReference(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC()
	store, db := nextcloudGCTestDB(t)
	nextcloudGCTestSource(t, db, "old", "", now.Add(-2*time.Hour))
	require.NoError(t, db.Create(&types.Chunk{
		ID: "chunk", TenantID: 7,
		KnowledgeBaseID: "kb", KnowledgeID: "old",
	}).Error)
	_, err := store.InventoryRetired(ctx, now, 100)
	require.NoError(t, err)
	var job nextcloudGCJob
	require.NoError(t, db.Take(&job).Error)
	_, err = store.RunDue(ctx, now, 100)
	require.NoError(t, err)
	require.NoError(t, db.Model(&types.Chunk{}).Where("id = ?", "chunk").
		Update("image_info", `[{"url":"resource://late-image"}]`).Error)

	_, err = store.RunDue(ctx, now.Add(time.Hour), 100)
	require.NoError(t, err)
	var item nextcloudGCItem
	require.NoError(t, db.Where("job_id = ? AND kind = ? AND object_ref = ?",
		job.ID, "extracted_image", "resource://late-image").Take(&item).Error)
	require.Equal(t, "blocked", item.State)
	_, err = store.RunDue(ctx, now.Add(2*time.Hour), 100)
	require.NoError(t, err)
	var count int64
	require.NoError(t, db.Model(&nextcloudGCItem{}).Where("job_id = ? AND kind = ?",
		job.ID, "extracted_image").Count(&count).Error)
	require.Equal(t, int64(1), count, "retry must not duplicate a persisted image item")
}

func TestNextcloudGCLateInvalidImageInventoryBlocksThenRecovers(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC()
	store, db := nextcloudGCTestDB(t)
	ref := nextcloudGCTestLocalResource(t, db, strings.Repeat("i", 22), "old")
	nextcloudGCTestSource(t, db, "old", ref, now.Add(-2*time.Hour))
	_, err := store.InventoryRetired(ctx, now, 100)
	require.NoError(t, err)
	var job nextcloudGCJob
	require.NoError(t, db.Take(&job).Error)
	deletes := 0
	store.ConfigureLocalDelete(func(context.Context, *types.StoredResource) (bool, error) {
		deletes++
		return true, nil
	})
	_, err = store.RunDue(ctx, now, 100)
	require.NoError(t, err)
	require.Zero(t, deletes, "the original is still inside its recovery window")
	// The initial inventory and first due run were valid. A later writer
	// leaves image_info corrupt before the original's recovery window ends.
	require.NoError(t, db.Create(&types.Chunk{
		ID: "late-chunk", TenantID: 7,
		KnowledgeBaseID: "kb", KnowledgeID: "old", ImageInfo: `{broken`,
	}).Error)
	_, err = store.RunDue(ctx, now.Add(7*24*time.Hour), 100)
	require.NoError(t, err)
	require.Zero(t, deletes)
	status, err := store.ListStatus(ctx, 7, 100)
	require.NoError(t, err)
	require.Equal(t, "invalid_image_inventory", status[0].LastErrorCode)
	var resource types.StoredResource
	require.NoError(t, db.Where("handle = ?", strings.Repeat("i", 22)).Take(&resource).Error)
	require.Equal(t, types.ResourceStateActive, resource.State)
	var bindings int64
	require.NoError(t, db.Model(&types.ResourceBinding{}).
		Where("resource_id = ?", resource.ID).Count(&bindings).Error)
	require.Equal(t, int64(1), bindings)

	require.NoError(t, db.Model(&types.Chunk{}).Where("id = ?", "late-chunk").
		Update("image_info", `[{"url":"`+ref+`"}]`).Error)
	_, err = store.RunDue(ctx, now.Add(7*24*time.Hour+time.Hour), 100)
	require.NoError(t, err)
	require.Equal(t, 1, deletes)
	var imageItem nextcloudGCItem
	require.NoError(t, db.Where("job_id = ? AND kind = ? AND object_ref = ?",
		job.ID, "extracted_image", ref).Take(&imageItem).Error)
	require.Equal(t, "collected", imageItem.State)
	require.NoError(t, db.Unscoped().Where("id = ?", resource.ID).Take(&resource).Error)
	require.Equal(t, types.ResourceStateDeleted, resource.State)
	status, err = store.ListStatus(ctx, 7, 100)
	require.NoError(t, err)
	require.NotEqual(t, "invalid_image_inventory", status[0].LastErrorCode)
	require.Equal(t, "blocked", status[0].State, "derived copies still have no deletion proof")
}

func TestNextcloudGCPhysicalClaimFailureIsDurable(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC()
	store, db := nextcloudGCTestDB(t)
	nextcloudGCTestSource(t, db, "old", types.BuildResourcePath(strings.Repeat("h", 22)),
		now.Add(-8*24*time.Hour))
	_, err := store.InventoryRetired(ctx, now, 100)
	require.NoError(t, err)
	store.ConfigureLocalDelete(func(context.Context, *types.StoredResource) (bool, error) {
		t.Fatal("provider must not run without a resource row")
		return false, nil
	})
	_, err = store.RunDue(ctx, now, 100)
	require.Error(t, err)
	status, err := store.ListStatus(ctx, 7, 100)
	require.NoError(t, err)
	require.Equal(t, "blocked", status[0].State)
	require.Equal(t, "gc_step_failed", status[0].LastErrorCode)
	require.Zero(t, status[0].ConfirmedReleasedBytes)
}

func nextcloudGCTestSource(t *testing.T, db *gorm.DB, id, filePath string, at time.Time) {
	t.Helper()
	metadata, err := json.Marshal(map[string]string{
		"datasource_id": "ds", "external_id": "node",
		"nextcloud_etag": "", "nextcloud_binding_id": "binding", "nextcloud_file_id": "77",
	})
	require.NoError(t, err)
	require.NoError(t, db.Create(&types.Knowledge{
		ID: id, TenantID: 7, KnowledgeBaseID: "kb", Channel: types.ConnectorTypeNextcloud,
		ParseStatus: types.ParseStatusCompleted, EnableStatus: "enabled", Metadata: types.JSON(metadata),
		FilePath: filePath, StorageSize: 23, CreatedAt: at, UpdatedAt: at,
	}).Error)
	require.NoError(t, db.Create(&nextcloudSourceVersion{
		TenantID: 7, KnowledgeBaseID: "kb",
		DataSourceID: "ds", ExternalID: "node", State: "published", CandidateKnowledgeID: "new",
		DesiredETag: "new-etag", UpdatedAt: at,
	}).Error)
}

func TestNextcloudGCInventoryPersistsExactItemsAndRetriesWithoutClaimingBytes(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC)
	store, db := nextcloudGCTestDB(t)
	nextcloudGCTestSource(t, db, "old", "resource://opaque-source", now.Add(-2*time.Hour))
	require.NoError(t, db.Create(&types.Chunk{
		ID: "chunk", TenantID: 7, KnowledgeBaseID: "kb",
		KnowledgeID: "old", ImageInfo: `[{"url":"resource://opaque-image"}]`,
	}).Error)

	created, err := store.InventoryRetired(ctx, now, 100)
	require.NoError(t, err)
	require.Equal(t, 1, created)
	created, err = store.InventoryRetired(ctx, now, 100)
	require.NoError(t, err)
	require.Zero(t, created)
	var items []nextcloudGCItem
	require.NoError(t, db.Order("kind").Find(&items).Error)
	require.Len(t, items, 5)
	require.Equal(t, []string{"derived_chunk", "derived_index", "extracted_image", "knowledge", "source_file"},
		[]string{items[0].Kind, items[1].Kind, items[2].Kind, items[3].Kind, items[4].Kind})
	require.Equal(t, "chunk", items[0].ObjectRef)
	require.Equal(t, "resource://opaque-image", items[2].ObjectRef)
	require.Equal(t, "resource://opaque-source", items[4].ObjectRef)

	processed, err := store.RunDue(ctx, now, 100)
	require.NoError(t, err)
	require.Equal(t, 1, processed)
	status, err := store.ListStatus(ctx, 7, 100)
	require.NoError(t, err)
	require.Len(t, status, 1)
	require.Equal(t, "blocked", status[0].State)
	require.Equal(t, "safe_object_cleanup_unavailable", status[0].LastErrorCode)
	require.Equal(t, now.Add(-time.Hour), status[0].NotBefore)
	require.Equal(t, now.Add(-2*time.Hour).Add(7*24*time.Hour), status[0].OriginalNotBefore)
	require.Equal(t, int64(23), status[0].EstimatedBytes)
	require.Zero(t, status[0].ConfirmedReleasedBytes)
	require.Equal(t, int64(5), status[0].PendingItems)
	encoded, err := json.Marshal(status)
	require.NoError(t, err)
	require.NotContains(t, string(encoded), "resource://")
	processed, err = store.RunDue(ctx, now.Add(30*time.Minute), 100)
	require.NoError(t, err)
	require.Zero(t, processed)
	processed, err = store.RunDue(ctx, now.Add(time.Hour), 100)
	require.NoError(t, err)
	require.Equal(t, 1, processed)
	status, err = store.ListStatus(ctx, 7, 100)
	require.NoError(t, err)
	require.Equal(t, 2, status[0].Attempts)
	require.Zero(t, status[0].ConfirmedReleasedBytes)
	var row types.Knowledge
	require.NoError(t, db.Where("id = ?", "old").Take(&row).Error)
}

func TestNextcloudGCTombstoneSkipsOrdinarySafetyWindow(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC()
	store, db := nextcloudGCTestDB(t)
	nextcloudGCTestSource(t, db, "old", "resource://source", now)
	require.NoError(t, db.Model(&nextcloudSourceVersion{}).
		Where(("tenant_id = ? AND knowledge_base_id = ? AND datasource_"+
			"id = ? AND external_id = ?"), 7, "kb", "ds", "node").
		Updates(map[string]any{"state": "tombstone", "candidate_knowledge_id": ""}).Error)
	created, err := store.InventoryRetired(ctx, now, 100)
	require.NoError(t, err)
	require.Equal(t, 1, created)
	status, err := store.ListStatus(ctx, 7, 100)
	require.NoError(t, err)
	require.Equal(t, "tombstone", status[0].Reason)
	require.Equal(t, now, status[0].NotBefore)
	require.Equal(t, now, status[0].OriginalNotBefore)
	processed, err := store.RunDue(ctx, now, 100)
	require.NoError(t, err)
	require.Equal(t, 1, processed)
	status, err = store.ListStatus(ctx, 7, 100)
	require.NoError(t, err)
	require.Equal(t, "blocked", status[0].State)
	require.Zero(t, status[0].ConfirmedReleasedBytes)
}

func TestNextcloudGCBlocksCandidateThatBecameCurrent(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC()
	store, db := nextcloudGCTestDB(t)
	nextcloudGCTestSource(t, db, "old", "", now.Add(-2*time.Hour))
	created, err := store.InventoryRetired(ctx, now, 100)
	require.NoError(t, err)
	require.Equal(t, 1, created)
	require.NoError(t, db.Model(&nextcloudSourceVersion{}).
		Where(("tenant_id = ? AND knowledge_base_id = ? AND datasource_"+
			"id = ? AND external_id = ?"), 7, "kb", "ds", "node").
		Update("candidate_knowledge_id", "old").Error)
	_, err = store.RunDue(ctx, now, 100)
	require.NoError(t, err)
	status, err := store.ListStatus(ctx, 7, 100)
	require.NoError(t, err)
	require.Equal(t, "blocked", status[0].State)
	require.Equal(t, "candidate_is_current", status[0].LastErrorCode)
	require.Equal(t, int64(0), status[0].ConfirmedReleasedBytes)
}

func TestNextcloudGCEmptyLocalRowsStillBlockUnknownHistoricalDerivedOutput(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC()
	store, db := nextcloudGCTestDB(t)
	nextcloudGCTestSource(t, db, "old", "", now.Add(-2*time.Hour))
	created, err := store.InventoryRetired(ctx, now, 100)
	require.NoError(t, err)
	require.Equal(t, 1, created)
	processed, err := store.RunDue(ctx, now, 100)
	require.NoError(t, err)
	require.Equal(t, 1, processed)
	status, err := store.ListStatus(ctx, 7, 100)
	require.NoError(t, err)
	require.Equal(t, "blocked", status[0].State)
	require.Equal(t, "derived_provenance_unverified", status[0].LastErrorCode)
	require.Equal(t, int64(2), status[0].PendingItems)
	require.Zero(t, status[0].ConfirmedReleasedBytes)
	var blocker nextcloudGCItem
	require.NoError(t, db.Where("job_id = ? AND kind = ? AND object_ref = ?",
		status[0].ID, "derived_index", "old").Take(&blocker).Error)
	require.Equal(t, "blocked", blocker.State)
	var row types.Knowledge
	require.NoError(t, db.Where("id = ?", "old").Take(&row).Error)
}

func TestNextcloudGCInvalidImageInventoryRemainsBlockedThenRecovers(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC()
	store, db := nextcloudGCTestDB(t)
	nextcloudGCTestSource(t, db, "old", "", now.Add(-2*time.Hour))
	require.NoError(t, db.Create(&types.Chunk{
		ID: "chunk", TenantID: 7, KnowledgeBaseID: "kb",
		KnowledgeID: "old", ImageInfo: `{broken`,
	}).Error)
	created, err := store.InventoryRetired(ctx, now, 100)
	require.NoError(t, err)
	require.Equal(t, 1, created)
	status, err := store.ListStatus(ctx, 7, 100)
	require.NoError(t, err)
	require.Equal(t, "invalid_image_inventory", status[0].LastErrorCode)
	require.Equal(t, "blocked", status[0].State)
	require.Zero(t, status[0].ConfirmedReleasedBytes)
	require.NoError(t, db.Model(&types.Chunk{}).Where("id = ?", "chunk").
		Update("image_info", `[{"url":"resource://recovered-image"}]`).Error)
	processed, err := store.RunDue(ctx, now.Add(time.Hour), 100)
	require.NoError(t, err)
	require.Equal(t, 1, processed)
	status, err = store.ListStatus(ctx, 7, 100)
	require.NoError(t, err)
	require.Equal(t, "safe_object_cleanup_unavailable", status[0].LastErrorCode)
	var item nextcloudGCItem
	require.NoError(t, db.Where("kind = ?", "extracted_image").Take(&item).Error)
	require.Equal(t, "resource://recovered-image", item.ObjectRef)
}

func TestNextcloudGCInventoriesExactLocalIndexRowsAndLateWrites(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC()
	store, db := nextcloudGCTestDB(t)
	require.NoError(t, db.Exec(`CREATE TABLE lite_embeddings
		(id INTEGER PRIMARY KEY, knowledge_base_id TEXT NOT NULL, knowledge_id TEXT NOT NULL)`).Error)
	nextcloudGCTestSource(t, db, "old", "", now.Add(-2*time.Hour))
	require.NoError(t, db.Create(&types.Chunk{
		ID: "chunk-old", TenantID: 7,
		KnowledgeBaseID: "kb", KnowledgeID: "old",
	}).Error)
	require.NoError(t, db.Exec(`INSERT INTO lite_embeddings (id, knowledge_base_id, knowledge_id)
		VALUES (11, 'kb', 'old'), (12, 'other-kb', 'old'), (13, 'kb', 'other')`).Error)
	created, err := store.InventoryRetired(ctx, now, 100)
	require.NoError(t, err)
	require.Equal(t, 1, created)
	var job nextcloudGCJob
	require.NoError(t, db.Take(&job).Error)
	var items []nextcloudGCItem
	require.NoError(t, db.Where("job_id = ?", job.ID).Order("kind, object_ref").Find(&items).Error)
	require.Equal(t, []string{"derived_chunk:chunk-old", "derived_index:old", "knowledge:old", "sqlite_embedding:11"},
		[]string{
			items[0].Kind + ":" + items[0].ObjectRef, items[1].Kind + ":" + items[1].ObjectRef,
			items[2].Kind + ":" + items[2].ObjectRef, items[3].Kind + ":" + items[3].ObjectRef,
		})

	// A late row is inventoried on the retry, before the job can claim it is
	// collected. Current code deliberately keeps derived rows blocked.
	require.NoError(t, db.Exec(`INSERT INTO lite_embeddings (id, knowledge_base_id, knowledge_id)
		VALUES (14, 'kb', 'old')`).Error)
	processed, err := store.RunDue(ctx, now, 100)
	require.NoError(t, err)
	require.Equal(t, 1, processed)
	require.NoError(t, db.Where("job_id = ? AND kind = ?", job.ID, "sqlite_embedding").
		Order("object_ref").Find(&items).Error)
	require.Len(t, items, 2)
	require.Equal(t, "11", items[0].ObjectRef)
	require.Equal(t, "14", items[1].ObjectRef)
	var chunks, embeddings int64
	require.NoError(t, db.Model(&types.Chunk{}).Where("id = ?", "chunk-old").Count(&chunks).Error)
	require.NoError(t, db.Table(
		"lite_embeddings",
	).Where("knowledge_id = ? AND knowledge_base_id = ?", "old", "kb").Count(&embeddings).Error)
	require.Equal(t, int64(1), chunks)
	require.Equal(t, int64(2), embeddings)
	status, err := store.ListStatus(ctx, 7, 100)
	require.NoError(t, err)
	require.Equal(t, "blocked", status[0].State)
	require.Zero(t, status[0].ConfirmedReleasedBytes)
}

func TestNextcloudGCEmbeddingWithoutChunkOrModelCannotComplete(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC()
	store, db := nextcloudGCTestDB(t)
	require.NoError(t, db.Exec(`CREATE TABLE lite_embeddings
		(id INTEGER PRIMARY KEY, knowledge_base_id TEXT NOT NULL, knowledge_id TEXT NOT NULL)`).Error)
	nextcloudGCTestSource(t, db, "old", "", now.Add(-2*time.Hour))
	require.NoError(t, db.Exec(`INSERT INTO lite_embeddings (id, knowledge_base_id, knowledge_id)
		VALUES (41, 'kb', 'old')`).Error)
	_, err := store.InventoryRetired(ctx, now, 100)
	require.NoError(t, err)
	_, err = store.RunDue(ctx, now, 100)
	require.NoError(t, err)
	status, err := store.ListStatus(ctx, 7, 100)
	require.NoError(t, err)
	require.Equal(t, "blocked", status[0].State)
	require.Equal(t, "safe_object_cleanup_unavailable", status[0].LastErrorCode)
	var items []nextcloudGCItem
	require.NoError(t, db.Where("job_id = ?", status[0].ID).Order("kind").Find(&items).Error)
	require.Equal(t, []string{"derived_index", "knowledge", "sqlite_embedding"},
		[]string{items[0].Kind, items[1].Kind, items[2].Kind})
}

func TestNextcloudGCStepFailureIsDurableAndRetried(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC()
	store, db := nextcloudGCTestDB(t)
	nextcloudGCTestSource(t, db, "old", "resource://source", now.Add(-2*time.Hour))
	created, err := store.InventoryRetired(ctx, now, 100)
	require.NoError(t, err)
	require.Equal(t, 1, created)
	require.NoError(t, db.Delete(&nextcloudSourceVersion{},
		"tenant_id = ? AND knowledge_base_id = ? AND datasource_id = ? AND external_id = ?",
		7, "kb", "ds", "node").Error)
	_, err = store.RunDue(ctx, now, 100)
	require.Error(t, err)
	status, err := store.ListStatus(ctx, 7, 100)
	require.NoError(t, err)
	require.Equal(t, "retry", status[0].State)
	require.Equal(t, "gc_step_failed", status[0].LastErrorCode)
	require.Equal(t, 1, status[0].Attempts)
	require.Zero(t, status[0].ConfirmedReleasedBytes)
	require.NoError(t, db.Create(&nextcloudSourceVersion{
		TenantID: 7, KnowledgeBaseID: "kb",
		DataSourceID: "ds", ExternalID: "node", State: "published", CandidateKnowledgeID: "new",
	}).Error)
	processed, err := store.RunDue(ctx, now.Add(time.Hour), 100)
	require.NoError(t, err)
	require.Equal(t, 1, processed)
	status, err = store.ListStatus(ctx, 7, 100)
	require.NoError(t, err)
	require.Equal(t, "blocked", status[0].State)
	require.Equal(t, "safe_object_cleanup_unavailable", status[0].LastErrorCode)
}

func TestNextcloudGCSQLiteMigrationCreatesDurableTables(t *testing.T) {
	db, err := gorm.Open(sqlite.Open("file:"+uuid.NewString()+"?mode=memory&cache=shared"), &gorm.Config{})
	require.NoError(t, err)
	applyScript := func(path string) {
		t.Helper()
		script, readErr := os.ReadFile(path)
		require.NoError(t, readErr)
		for _, statement := range strings.Split(string(script), ";") {
			statement = strings.TrimSpace(statement)
			if statement != "" {
				require.NoError(t, db.Exec(statement).Error)
			}
		}
	}
	applyScript("../../../migrations/sqlite/000038_nextcloud_gc.up.sql")
	require.True(t, db.Migrator().HasTable("nextcloud_gc_jobs"))
	require.True(t, db.Migrator().HasTable("nextcloud_gc_items"))
	require.NoError(t, db.Exec(`INSERT INTO nextcloud_gc_jobs
		(id, tenant_id, knowledge_base_id, datasource_id, external_id,
		 knowledge_id, reason, state, not_before, original_not_before, next_attempt_at)
		VALUES ('job', 7, 'kb', 'ds', 'node', 'old', 'retired', 'blocked',
		 CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`).Error)
	require.NoError(t, db.Exec(`INSERT INTO nextcloud_gc_items
		(job_id, kind, object_ref, state, estimated_bytes)
		VALUES ('job', 'source_file', 'resource://original', 'blocked', 23)`).Error)
	require.NoError(t, db.Exec(`INSERT INTO nextcloud_gc_jobs
		(id, tenant_id, knowledge_base_id, datasource_id, external_id,
		 knowledge_id, reason, state, not_before, original_not_before, next_attempt_at, completed_at)
		VALUES ('previously-collected', 7, 'kb', 'ds', 'other', 'old-other', 'retired', 'collected',
		 CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`).Error)
	applyScript("../../../migrations/sqlite/000040_nextcloud_gc_object_claim.up.sql")
	applyScript("../../../migrations/sqlite/000041_nextcloud_gc_derived_inventory.up.sql")
	var reopened nextcloudGCJob
	require.NoError(t, db.Where("id = ?", "previously-collected").Take(&reopened).Error)
	require.Equal(t, "blocked", reopened.State)
	require.Nil(t, reopened.CompletedAt)
	require.Equal(t, "derived_provenance_unverified", reopened.LastErrorCode)
	var legacyBlocker nextcloudGCItem
	require.NoError(t, db.Where("job_id = ? AND kind = ? AND object_ref = ?",
		"previously-collected", "derived_index", "old-other").Take(&legacyBlocker).Error)
	require.Equal(t, "blocked", legacyBlocker.State)
	var item nextcloudGCItem
	require.NoError(t, db.Where("job_id = ? AND kind = ?", "job", "source_file").Take(&item).Error)
	require.Equal(t, "blocked", item.State)
	require.Equal(t, int64(23), item.EstimatedBytes)
	require.Empty(t, item.LeaseToken)
	require.True(t, db.Migrator().HasColumn(&nextcloudGCItem{}, "lease_until"))
	require.NoError(t, db.Exec(`UPDATE nextcloud_gc_items
		SET state = 'deleting', lease_token = 'claim' WHERE job_id = 'job' AND kind = 'source_file'`).Error)
	require.NoError(t, db.Where("job_id = ? AND kind = ?", "job", "source_file").Take(&item).Error)
	require.Equal(t, "deleting", item.State)
	require.Equal(t, "claim", item.LeaseToken)
	require.NoError(t, db.Exec(`INSERT INTO nextcloud_gc_items (job_id, kind, object_ref, state)
		VALUES ('job', 'derived_chunk', 'chunk-id', 'pending')`).Error)
	require.NoError(t, db.Exec(`INSERT INTO nextcloud_gc_items (job_id, kind, object_ref, state)
		VALUES ('job', 'sqlite_embedding', '123', 'pending')`).Error)
}
