package service

import (
	"testing"

	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/datasource"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

func TestDingTalkEditorSavePreservesAuthorizedConfiguration(t *testing.T) {
	t.Setenv("SYSTEM_AES_KEY", "weknora-system-aes-key-32bytes!!")
	h, db, _, q := datasourceReplacementFixture(t)
	sources := repository.NewDataSourceRepository(db)
	factory := func(tx *gorm.DB) *repository.KnowledgeInputProvenanceStore {
		store := repository.NewKnowledgeInputProvenanceStore(tx)
		groups := NewGroupAccessService(repository.NewGroupAccessRepository(tx))
		repository.ConfigureKnowledgeInputActorGroupAccess(store, groups)
		return store
	}
	repository.ConfigureDataSourceNameInputWriters(sources, nil, factory)
	original := &types.DataSourceConfig{
		Type:        types.ConnectorTypeDingTalk,
		Credentials: map[string]any{"app_secret": "test-only"}, ResourceIDs: []string{"a", "b"},
	}
	encrypted, err := original.ToJSON()
	require.NoError(t, err)
	second, err := original.ToJSON()
	require.NoError(t, err)
	require.NotEqual(t, encrypted, second)
	require.NoError(t, db.Model(h.source).Updates(map[string]any{
		"config": encrypted, "type": types.ConnectorTypeDingTalk,
		"sync_schedule": "0 15 3 * * *",
	}).Error)
	require.NoError(t, db.Exec(`CREATE TABLE dingtalk_source_input_revisions (
		datasource_id TEXT PRIMARY KEY, revision INTEGER NOT NULL)`).Error)
	require.NoError(t, db.Exec("INSERT INTO dingtalk_source_input_revisions VALUES (?, 1)", h.source.ID).Error)
	require.NoError(t, db.Exec(`CREATE TABLE dingtalk_schedule_authorizations (
data_source_id TEXT PRIMARY KEY, tenant_id INTEGER,
		knowledge_base_id TEXT, source_revision INTEGER, schedule TEXT, grant_id TEXT, user_id TEXT)`).Error)
	require.NoError(t, db.Exec(`CREATE TRIGGER test_dingtalk_config_revision AFTER UPDATE OF config ON data_sources
		WHEN OLD.config IS NOT NEW.config BEGIN UPDATE dingtalk_source_input_revisions SET revision = revision+1
		WHERE datasource_id = NEW.id; END`).Error)

	scheduler := datasource.NewScheduler(sources, nil, nil)
	svc := WithConnectorScheduleIdentity(&DataSourceService{dsRepo: sources, scheduler: scheduler})
	update := &types.DataSource{
		ID: h.source.ID, TenantID: h.source.TenantID, KnowledgeBaseID: h.source.KnowledgeBaseID,
		Type: types.ConnectorTypeDingTalk, SyncSchedule: "0 15 3 * * *",
		Config: types.JSON(`{"credentials":{},"resource_ids":["a","b"],"settings":{}}`),
	}
	_, err = svc.UpdateDataSource(repository.WithDingTalkScheduleEdit(h.ctx), update)
	require.NoError(t, err)
	const lookupGrant = "SELECT grant_id FROM dingtalk_schedule_authorizations WHERE data_source_id = ?"
	var grantID string
	require.NoError(t, db.Raw(lookupGrant, update.ID).Scan(&grantID).Error)
	require.NotEmpty(t, grantID)
	// Reopen from the server and submit the old editor's complete public config.
	update = &types.DataSource{
		ID: update.ID, Type: types.ConnectorTypeDingTalk,
		Config: types.JSON(`{"resource_ids":["b","a"],"credentials":{},"settings":{}}`),
	}
	updated, err := svc.UpdateDataSource(h.ctx, update)
	require.NoError(t, err)
	require.JSONEq(t, string(encrypted), string(updated.Config))
	var sameGrant string
	require.NoError(t, db.Raw(lookupGrant, updated.ID).Scan(&sameGrant).Error)
	require.Equal(t, grantID, sameGrant)
	state, err := q.inputs.DingTalkScheduleAuthorizationStatus(h.ctx, updated.TenantID, updated.ID)
	require.NoError(t, err)
	require.Equal(t, "authorized", state)
	changed := *updated
	changed.Config = types.JSON(`{"resource_ids":["a"],"settings":{"include_uploaded_files":false}}`)
	require.False(t, unchangedDingTalkEditorConfig(updated, &changed))

	// Another editor commits a new resource selection immediately after the
	// request reads its predecessor, before our write acquires the KB lock.
	concurrent := types.JSON(`{"resource_ids":["other"],"settings":{}}`)
	require.NoError(t, db.Callback().Query().After("gorm:query").Register("swap_schedule_scope", func(tx *gorm.DB) {
		if tx.Statement.Table != "data_sources" || concurrent == nil {
			return
		}
		next := concurrent
		concurrent = nil
		require.NoError(t, db.Model(h.source).UpdateColumn("config", next).Error)
	}))
	t.Cleanup(func() { require.NoError(t, db.Callback().Query().Remove("swap_schedule_scope")) })
	request := &types.DataSource{
		ID: updated.ID, Type: types.ConnectorTypeDingTalk, SyncSchedule: updated.SyncSchedule,
		Config: types.JSON(`{"resource_ids":["a","b"],"settings":{}}`),
	}
	_, err = svc.UpdateDataSource(repository.WithDingTalkScheduleEdit(h.ctx), request)
	require.ErrorIs(t, err, types.ErrDataSourceSettingsConflict)
	require.NoError(t, db.Raw(lookupGrant, updated.ID).Scan(&sameGrant).Error)
	require.Equal(t, grantID, sameGrant)
	state, err = q.inputs.DingTalkScheduleAuthorizationStatus(h.ctx, updated.TenantID, updated.ID)
	require.NoError(t, err)
	require.Equal(t, "required", state)
}
