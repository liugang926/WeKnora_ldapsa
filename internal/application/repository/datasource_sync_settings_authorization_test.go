package repository

import (
	"testing"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
)

func TestDingTalkSchedulePatchCommitsGrantWithSettings(t *testing.T) {
	store, db, editor, _, _, _ := displayTestFixture(t, false)
	require.NoError(t, db.AutoMigrate(&types.DataSource{}, &dingTalkScheduleAuthorization{}))
	require.NoError(t, db.Exec(`CREATE TABLE dingtalk_source_input_revisions (
		datasource_id TEXT PRIMARY KEY, revision INTEGER NOT NULL)`).Error)
	source := &types.DataSource{
		ID: "finance", TenantID: 7, KnowledgeBaseID: "kb", Type: types.ConnectorTypeDingTalk,
		Status: types.DataSourceStatusActive, SyncMode: "full",
	}
	require.NoError(t, db.Create(source).Error)
	require.NoError(t, db.Exec("INSERT INTO dingtalk_source_input_revisions VALUES (?, 1)", source.ID).Error)
	repo := &DataSourceRepository{db: db, nameAuthorityFactory: store.displayAuthorityFactory}
	marked := WithDingTalkScheduleEdit(editor)
	source.SyncSchedule = "0 30 4 * * *"
	require.NoError(t, repo.PatchSyncSettings(marked, source, nil, []string{"sync_schedule"}))
	grant, err := findDingTalkScheduleAuthorization(db, source.ID, source.TenantID)
	require.NoError(t, err)
	require.NotEmpty(t, grant.GrantID)
	require.Equal(t, "display-user", grant.UserID)
	require.Equal(t, source.SyncSchedule, grant.Schedule)
	source.SyncMode = "incremental"
	require.NoError(t, repo.PatchSyncSettings(editor, source, nil, []string{"sync_mode"}))
	same, err := findDingTalkScheduleAuthorization(db, source.ID, source.TenantID)
	require.NoError(t, err)
	require.Equal(t, grant.GrantID, same.GrantID)

	// The setting and the previous grant must survive a failed authorization together.
	require.NoError(t, db.Model(&types.User{}).Where("id = ?", grant.UserID).Update("is_active", false).Error)
	source.SyncSchedule = "0 0 8 * * *"
	require.Error(t, repo.PatchSyncSettings(marked, source, nil, []string{"sync_schedule"}))
	saved, err := repo.FindByID(editor, source.ID)
	require.NoError(t, err)
	require.Equal(t, grant.Schedule, saved.SyncSchedule)
	same, err = findDingTalkScheduleAuthorization(db, source.ID, source.TenantID)
	require.NoError(t, err)
	require.Equal(t, grant.GrantID, same.GrantID)

	// A generic edit cannot resurrect an earlier grant by restoring its cron.
	require.NoError(t, repo.PatchSyncSettings(editor, source, nil, []string{"sync_schedule"}))
	source.SyncSchedule = grant.Schedule
	require.NoError(t, repo.PatchSyncSettings(editor, source, nil, []string{"sync_schedule"}))
	same, err = findDingTalkScheduleAuthorization(db, source.ID, source.TenantID)
	require.NoError(t, err)
	require.Empty(t, same.GrantID)
}

func TestPairedNextcloudResourcePatchCannotChangeBoundConfiguration(t *testing.T) {
	_, db, editor, _, _, _ := displayTestFixture(t, false)
	require.NoError(t, db.AutoMigrate(&types.DataSource{}, &NextcloudSourcePairing{}))
	source := &types.DataSource{
		ID: "paired", TenantID: 7, KnowledgeBaseID: "kb", Type: types.ConnectorTypeNextcloud,
		Status: types.DataSourceStatusActive, Config: types.JSON(`{"resource_ids":["bound"]}`),
	}
	require.NoError(t, db.Create(source).Error)
	require.NoError(t, db.Create(&NextcloudSourcePairing{
		OperationID: "operation", DataSourceID: source.ID, TenantID: source.TenantID,
		KnowledgeBaseID: source.KnowledgeBaseID, State: "active",
	}).Error)
	repo := NewDataSourceRepository(db)
	previous := source.Config
	source.Config = types.JSON(`{"resource_ids":["other"]}`)
	source.SyncSchedule = "0 0 6 * * *"
	err := repo.PatchSyncSettings(editor, source, previous, []string{"config", "sync_schedule"})
	require.ErrorIs(t, err, ErrNextcloudSourcePairingConflict)
	saved, err := repo.FindByID(editor, source.ID)
	require.NoError(t, err)
	require.JSONEq(t, string(previous), string(saved.Config))
	require.Empty(t, saved.SyncSchedule)
	require.NoError(t, repo.PatchSyncSettings(editor, source, nil, []string{"sync_schedule"}))
	saved, err = repo.FindByID(editor, source.ID)
	require.NoError(t, err)
	require.JSONEq(t, string(previous), string(saved.Config))
	require.Equal(t, source.SyncSchedule, saved.SyncSchedule)
}
