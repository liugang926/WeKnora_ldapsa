package repository

import (
	"context"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func setupDataSourceRepoTestDB(t *testing.T) *gorm.DB {
	t.Helper()
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	require.NoError(t, err)
	require.NoError(t, db.AutoMigrate(&types.DataSource{}, &types.SyncLog{}))
	require.NoError(t, db.Exec(`CREATE TABLE knowledge_bases (
		id TEXT PRIMARY KEY, tenant_id INTEGER NOT NULL,
		ever_had_nextcloud_source BOOLEAN NOT NULL DEFAULT 0
	)`).Error)
	require.NoError(t, db.Exec("INSERT INTO knowledge_bases (id, tenant_id) VALUES ('kb-1', 1)").Error)
	return db
}

func TestDataSourceRepositoryUpdateSyncStateClearsErrorMessage(t *testing.T) {
	db := setupDataSourceRepoTestDB(t)
	repo := NewDataSourceRepository(db)
	now := time.Now().UTC()
	result := types.JSON(`{"total":0}`)

	ds := &types.DataSource{
		ID:              "ds-1",
		TenantID:        1,
		KnowledgeBaseID: "kb-1",
		Name:            "Feishu",
		Type:            types.ConnectorTypeFeishu,
		Status:          types.DataSourceStatusError,
		ErrorMessage:    "previous failure",
	}
	require.NoError(t, repo.Create(context.Background(), ds))

	ds.Status = types.DataSourceStatusActive
	ds.ErrorMessage = ""
	ds.LastSyncAt = &now
	ds.LastSyncResult = result
	require.NoError(t, repo.UpdateSyncState(context.Background(), ds))

	var stored types.DataSource
	require.NoError(t, db.First(&stored, "id = ?", ds.ID).Error)
	assert.Equal(t, types.DataSourceStatusActive, stored.Status)
	assert.Empty(t, stored.ErrorMessage)
	assert.Equal(t, result.ToString(), stored.LastSyncResult.ToString())
	require.NotNil(t, stored.LastSyncAt)
}

func TestDataSourceRepositoryRetryableNextcloudEventFailureCannotUndoPause(t *testing.T) {
	ctx := context.Background()
	db := setupDataSourceRepoTestDB(t)
	repo := NewDataSourceRepository(db)
	originalCursor := types.JSON(`{"connector_cursor":{"instance_id":"instance-1"}}`)
	ds := &types.DataSource{
		ID: "nextcloud-pause-race", TenantID: 1,
		KnowledgeBaseID: "kb-1", Name: "Nextcloud", Type: types.ConnectorTypeNextcloud,
		Status: types.DataSourceStatusActive, LastSyncCursor: originalCursor,
	}
	require.NoError(t, db.Create(ds).Error)
	staleWorker := *ds // Worker loaded active before the administrator's pause.
	staleWorker.ErrorMessage = "temporary source read failure"
	staleWorker.LastSyncResult = types.JSON(`{"failed":1}`)
	require.NoError(t, db.Model(&types.DataSource{}).Where("id = ?", ds.ID).
		Update("status", types.DataSourceStatusPaused).Error)

	updated, err := repo.UpdateNextcloudRetryableEventFailure(ctx, &staleWorker)
	require.NoError(t, err)
	require.False(t, updated)
	var stored types.DataSource
	require.NoError(t, db.First(&stored, "id = ?", ds.ID).Error)
	require.Equal(t, types.DataSourceStatusPaused, stored.Status)
	require.Equal(t, originalCursor, stored.LastSyncCursor)
	require.Empty(t, stored.ErrorMessage)
	require.Empty(t, stored.LastSyncResult)

	// With an active source, only failure details change; the cursor and
	// status still cannot be written from the worker's stale snapshot.
	require.NoError(t, db.Model(&types.DataSource{}).Where("id = ?", ds.ID).
		Update("status", types.DataSourceStatusActive).Error)
	updated, err = repo.UpdateNextcloudRetryableEventFailure(ctx, &staleWorker)
	require.NoError(t, err)
	require.True(t, updated)
	require.NoError(t, db.First(&stored, "id = ?", ds.ID).Error)
	require.Equal(t, types.DataSourceStatusActive, stored.Status)
	require.Equal(t, originalCursor, stored.LastSyncCursor)
	require.Equal(t, staleWorker.ErrorMessage, stored.ErrorMessage)
	require.Equal(t, staleWorker.LastSyncResult, stored.LastSyncResult)
}

func TestNextcloudSyncStateCASPreservesConcurrentStatus(t *testing.T) {
	for _, scenario := range []struct {
		name, original, current, desired string
		deleted                          bool
		updated                          bool
	}{
		{
			"active success",
			types.DataSourceStatusActive,
			types.DataSourceStatusActive,
			types.DataSourceStatusActive,
			false,
			true,
		},
		{
			"pause before success",
			types.DataSourceStatusActive,
			types.DataSourceStatusPaused,
			types.DataSourceStatusActive,
			false,
			false,
		},
		{
			"pause before auth failure",
			types.DataSourceStatusActive,
			types.DataSourceStatusPaused,
			types.DataSourceStatusError,
			false,
			false,
		},
		{
			"auth failure",
			types.DataSourceStatusActive,
			types.DataSourceStatusActive,
			types.DataSourceStatusError,
			false,
			true,
		},
		{
			"manual error retry succeeds",
			types.DataSourceStatusError,
			types.DataSourceStatusError,
			types.DataSourceStatusActive,
			false,
			true,
		},
		{
			"manual paused run succeeds",
			types.DataSourceStatusPaused,
			types.DataSourceStatusPaused,
			types.DataSourceStatusPaused,
			false,
			true,
		},
		{
			"resume during paused manual run",
			types.DataSourceStatusPaused,
			types.DataSourceStatusActive,
			types.DataSourceStatusPaused,
			false,
			false,
		},
		{
			"source deleted before completion",
			types.DataSourceStatusActive,
			types.DataSourceStatusActive,
			types.DataSourceStatusActive,
			true,
			false,
		},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			ctx := context.Background()
			db := setupDataSourceRepoTestDB(t)
			repo := NewDataSourceRepository(db)
			oldCursor := types.JSON(`{"marker":"old"}`)
			newCursor := types.JSON(`{"marker":"new"}`)
			ds := &types.DataSource{
				ID: "nextcloud-cas", TenantID: 1, KnowledgeBaseID: "kb-1",
				Name: "Nextcloud", Type: types.ConnectorTypeNextcloud,
				Status: scenario.original, LastSyncCursor: oldCursor, ErrorMessage: "old error",
			}
			require.NoError(t, db.Create(ds).Error)
			worker := *ds
			worker.Status = scenario.desired
			worker.LastSyncCursor = newCursor
			worker.LastSyncResult = types.JSON(`{"total":1}`)
			worker.ErrorMessage = "new result"
			now := time.Now().UTC()
			worker.LastSyncAt = &now
			if scenario.current != scenario.original {
				require.NoError(t, db.Model(&types.DataSource{}).Where("id = ?", ds.ID).
					Update("status", scenario.current).Error)
			}
			if scenario.deleted {
				require.NoError(t, db.Delete(ds).Error)
			}
			updated, err := repo.UpdateNextcloudSyncStateCAS(ctx, &worker, scenario.original)
			require.NoError(t, err)
			require.Equal(t, scenario.updated, updated)
			var stored types.DataSource
			require.NoError(t, db.Unscoped().First(&stored, "id = ?", ds.ID).Error)
			if scenario.updated {
				require.Equal(t, scenario.desired, stored.Status)
				require.Equal(t, newCursor, stored.LastSyncCursor)
				require.Equal(t, worker.LastSyncResult, stored.LastSyncResult)
				require.Equal(t, worker.ErrorMessage, stored.ErrorMessage)
				require.NotNil(t, stored.LastSyncAt)
			} else {
				require.Equal(t, scenario.current, stored.Status)
				require.Equal(t, oldCursor, stored.LastSyncCursor)
				require.Empty(t, stored.LastSyncResult)
				require.Equal(t, "old error", stored.ErrorMessage)
				require.Nil(t, stored.LastSyncAt)
			}
		})
	}
}

func TestDataSourceRepositoryUpdatePersistsDisabledSyncDeletions(t *testing.T) {
	db := setupDataSourceRepoTestDB(t)
	repo := NewDataSourceRepository(db)
	ctx := context.Background()

	ds := &types.DataSource{
		ID:              "ds-sync-deletions",
		TenantID:        1,
		KnowledgeBaseID: "kb-1",
		Name:            "Feishu",
		Type:            types.ConnectorTypeFeishu,
		SyncDeletions:   true,
	}
	require.NoError(t, repo.Create(ctx, ds))

	ds.SyncDeletions = false
	require.NoError(t, repo.Update(ctx, ds))
	assert.False(t, ds.SyncDeletions)

	var stored types.DataSource
	require.NoError(t, db.First(&stored, "id = ?", ds.ID).Error)
	assert.False(t, stored.SyncDeletions)

	ds.SyncDeletions = true
	require.NoError(t, repo.Update(ctx, ds))
	assert.True(t, ds.SyncDeletions)
	require.NoError(t, db.First(&stored, "id = ?", ds.ID).Error)
	assert.True(t, stored.SyncDeletions)
}

func TestDataSourceRepositoryCreatePersistsDisabledSyncDeletions(t *testing.T) {
	db := setupDataSourceRepoTestDB(t)
	repo := NewDataSourceRepository(db)
	ctx := context.Background()

	ds := &types.DataSource{
		ID:              "ds-create-sync-deletions",
		TenantID:        1,
		KnowledgeBaseID: "kb-1",
		Name:            "Feishu",
		Type:            types.ConnectorTypeFeishu,
		SyncDeletions:   false,
	}
	require.NoError(t, repo.Create(ctx, ds))
	assert.False(t, ds.SyncDeletions, "Create must not leave the in-memory field hydrated to the GORM default")

	var stored types.DataSource
	require.NoError(t, db.First(&stored, "id = ?", ds.ID).Error)
	assert.False(t, stored.SyncDeletions)

	// A later Updates() on the same pointer must not persist the hydrated default.
	ds.Name = "Renamed"
	require.NoError(t, repo.Update(ctx, ds))
	require.NoError(t, db.First(&stored, "id = ?", ds.ID).Error)
	assert.False(t, stored.SyncDeletions)
	assert.Equal(t, "Renamed", stored.Name)
}

func TestDataSourceRepositoryCreatePersistsEnabledSyncDeletions(t *testing.T) {
	db := setupDataSourceRepoTestDB(t)
	repo := NewDataSourceRepository(db)
	ctx := context.Background()

	ds := &types.DataSource{
		ID:              "ds-create-sync-deletions-enabled",
		TenantID:        1,
		KnowledgeBaseID: "kb-1",
		Name:            "Feishu",
		Type:            types.ConnectorTypeFeishu,
		SyncDeletions:   true,
	}
	require.NoError(t, repo.Create(ctx, ds))
	assert.True(t, ds.SyncDeletions)

	var stored types.DataSource
	require.NoError(t, db.First(&stored, "id = ?", ds.ID).Error)
	assert.True(t, stored.SyncDeletions)
}

func TestDataSourceRepositoryDeleteSoftDeletesOnSQLite(t *testing.T) {
	db := setupDataSourceRepoTestDB(t)
	repo := NewDataSourceRepository(db)
	ctx := context.Background()

	target := &types.DataSource{
		ID:              "ds-delete-target",
		TenantID:        1,
		KnowledgeBaseID: "kb-1",
		Name:            "Delete target",
		Type:            types.ConnectorTypeFeishu,
	}
	other := &types.DataSource{
		ID:              "ds-delete-other",
		TenantID:        1,
		KnowledgeBaseID: "kb-1",
		Name:            "Other data source",
		Type:            types.ConnectorTypeFeishu,
	}
	require.NoError(t, repo.Create(ctx, target))
	require.NoError(t, repo.Create(ctx, other))

	require.NoError(t, repo.Delete(ctx, target.ID))

	var deleted types.DataSource
	require.NoError(t, db.Unscoped().First(&deleted, "id = ?", target.ID).Error)
	assert.True(t, deleted.DeletedAt.Valid)

	found, err := repo.FindByID(ctx, target.ID)
	assert.Error(t, err)
	assert.Nil(t, found)

	untouched, err := repo.FindByID(ctx, other.ID)
	require.NoError(t, err)
	assert.Equal(t, other.ID, untouched.ID)
}

func TestSyncLogRepositoryUpdateResultClearsErrorMessage(t *testing.T) {
	db := setupDataSourceRepoTestDB(t)
	repo := NewSyncLogRepository(db)
	finishedAt := time.Now().UTC()
	result := types.JSON(`{"total":0}`)

	log := &types.SyncLog{
		ID:           "log-1",
		DataSourceID: "ds-1",
		TenantID:     1,
		Status:       types.SyncLogStatusFailed,
		ErrorMessage: "previous failure",
		ItemsTotal:   1,
		ItemsFailed:  1,
	}
	require.NoError(t, repo.Create(context.Background(), log))

	log.Status = types.SyncLogStatusSuccess
	log.ErrorMessage = ""
	log.FinishedAt = &finishedAt
	log.ItemsTotal = 0
	log.ItemsFailed = 0
	log.Result = result
	require.NoError(t, repo.UpdateResult(context.Background(), log))

	var stored types.SyncLog
	require.NoError(t, db.First(&stored, "id = ?", log.ID).Error)
	assert.Equal(t, types.SyncLogStatusSuccess, stored.Status)
	assert.Empty(t, stored.ErrorMessage)
	assert.Zero(t, stored.ItemsTotal)
	assert.Zero(t, stored.ItemsFailed)
	assert.Equal(t, result.ToString(), stored.Result.ToString())
	require.NotNil(t, stored.FinishedAt)
}
