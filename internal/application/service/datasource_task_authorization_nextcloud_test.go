package service

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	apprepo "github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/hibiken/asynq"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

type nextcloudTaskAccessProbe struct {
	interfaces.GroupAccessService
	calls int
	ctx   context.Context
}

func (p *nextcloudTaskAccessProbe) EffectivePermission(ctx context.Context, _ uint64,
	_ types.ResourceType, _ string, _ types.ResourceAction, _ time.Time,
) (types.EffectiveResourcePermission, error) {
	p.calls++
	p.ctx = ctx
	return types.EffectiveResourcePermission{Reason: "processing permission revoked"}, nil
}

type nextcloudTaskLogRepo struct {
	processSyncSyncLogRepo
	createCalls int
	createCtx   context.Context
	afterCreate func()
}

func (r *nextcloudTaskLogRepo) Create(ctx context.Context, log *types.SyncLog) error {
	r.createCalls++
	r.createCtx = ctx
	if _, exists := r.logs[log.ID]; exists {
		return errors.New("duplicate sync log")
	}
	if err := r.processSyncSyncLogRepo.Create(ctx, log); err != nil {
		return err
	}
	if r.afterCreate != nil {
		r.afterCreate()
	}
	return nil
}

type nextcloudTaskAdmissionFixture struct {
	db      *gorm.DB
	svc     *DataSourceService
	logs    *nextcloudTaskLogRepo
	access  *nextcloudTaskAccessProbe
	payload types.DataSourceSyncPayload
}

func newNextcloudTaskAdmissionFixture(t *testing.T, trigger string) *nextcloudTaskAdmissionFixture {
	t.Helper()
	t.Setenv("SYSTEM_AES_KEY", "0123456789abcdef0123456789abcdef")
	db, err := gorm.Open(sqlite.Open(filepath.Join(t.TempDir(), "task-admission.db")), &gorm.Config{})
	require.NoError(t, err)
	sqlDB, err := db.DB()
	require.NoError(t, err)
	t.Cleanup(func() { _ = sqlDB.Close() })
	require.NoError(t, db.AutoMigrate(&types.DataSource{}, &types.KnowledgeBase{},
		&types.Knowledge{}, &types.SyncLog{}, &apprepo.NextcloudSourcePairing{}))
	for _, migration := range []string{
		"000031_nextcloud_source_versions.up.sql", "000032_nextcloud_event_inbox.up.sql",
		"000033_nextcloud_event_dispatch.up.sql", "000047_nextcloud_candidate_retry.up.sql",
	} {
		schema, err := os.ReadFile(filepath.Join("../../../migrations/sqlite", migration))
		require.NoError(t, err)
		require.NoError(t, db.Exec(string(schema)).Error)
	}
	config, err := (&types.DataSourceConfig{
		Type: types.ConnectorTypeNextcloud, ResourceIDs: []string{"binding"},
		Settings:    map[string]interface{}{"base_url": "https://nextcloud.example"},
		Credentials: map[string]interface{}{"token": "test-machine-token", "key_id": "test-key"},
	}).ToJSON()
	require.NoError(t, err)
	ds := &types.DataSource{ID: "ds", TenantID: 7, KnowledgeBaseID: "kb",
		Type: types.ConnectorTypeNextcloud, Status: types.DataSourceStatusActive, Config: config}
	kb := &types.KnowledgeBase{ID: "kb", TenantID: 7}
	require.NoError(t, db.Create(ds).Error)
	require.NoError(t, db.Create(kb).Error)
	baseURL, configSHA, bindingID, err := apprepo.NextcloudEventDataSourceIdentity(ds.Config)
	require.NoError(t, err)
	pair := &apprepo.NextcloudSourcePairing{OperationID: "pair", TenantID: 7,
		KnowledgeBaseID: "kb", DataSourceID: "ds", InstanceID: "instance", BindingID: bindingID,
		BaseURL: baseURL, ConfigSHA: configSHA, State: "active", PublicationEpoch: 3}
	require.NoError(t, db.Create(pair).Error)
	payload := types.DataSourceSyncPayload{
		TenantID: 7, DataSourceID: "ds", SyncLogID: "log", Trigger: trigger,
		NextcloudEventConnectionID: "connection", NextcloudEventConfigSHA256: configSHA,
		NextcloudRetryKnowledgeBaseID: "kb", NextcloudRetryExternalID: "nextcloud:instance:77",
		NextcloudRetryETag: "etag", NextcloudRetryCandidateID: "failed-candidate",
		NextcloudRetryInstanceID: "instance", NextcloudRetryBindingID: bindingID,
		NextcloudRetryConfigSHA256: configSHA, NextcloudRetryPairOperationID: pair.OperationID,
		NextcloudRetryPairingEpoch: pair.PublicationEpoch, NextcloudRetryLeaseToken: "lease",
	}
	if trigger == "nextcloud_event" {
		require.NoError(t, db.Exec(`INSERT INTO nextcloud_event_connections
			(connection_id, tenant_id, knowledge_base_id, datasource_id, nextcloud_instance_id,
			 binding_id, datasource_base_url, datasource_config_sha256, status,
			 current_key_id, current_secret_ciphertext)
			VALUES ('connection', 7, 'kb', 'ds', 'instance', ?, ?, ?, 'active', 'test-key', 'enc:v1:test')`,
			bindingID, baseURL, configSHA).Error)
		require.NoError(t, db.Exec(`INSERT INTO nextcloud_event_dispatch
			(connection_id, state, last_sync_log_id) VALUES ('connection', 'queued', 'log')`).Error)
	} else {
		now := time.Now().UTC()
		require.NoError(t, db.Create(&types.Knowledge{ID: "failed-candidate", TenantID: 7,
			KnowledgeBaseID: "kb", Channel: types.ConnectorTypeNextcloud,
			ParseStatus: types.ParseStatusFailed}).Error)
		require.NoError(t, db.Exec(`INSERT INTO nextcloud_source_versions
			(tenant_id, knowledge_base_id, datasource_id, external_id, desired_etag, candidate_knowledge_id, state)
			VALUES (7, 'kb', 'ds', 'nextcloud:instance:77', 'etag', 'failed-candidate', 'staging')`).Error)
		require.NoError(t, db.Exec(`INSERT INTO nextcloud_candidate_retry_jobs
			(tenant_id, knowledge_base_id, datasource_id, external_id, desired_etag, failed_candidate_id,
			 first_staged_at, next_attempt_at, state, lease_token, lease_until, sync_log_id)
			VALUES (7, 'kb', 'ds', 'nextcloud:instance:77', 'etag', 'failed-candidate',
			        ?, ?, 'leased', 'lease', ?, 'log')`,
			now, now, now.Add(time.Minute)).Error)
	}
	logs := &nextcloudTaskLogRepo{processSyncSyncLogRepo: processSyncSyncLogRepo{logs: map[string]*types.SyncLog{}}}
	probe := &nextcloudTaskAccessProbe{}
	return &nextcloudTaskAdmissionFixture{db: db, logs: logs, access: probe, payload: payload,
		svc: &DataSourceService{
			dsRepo: newKBDeleteDSRepo("kb", ds), syncLogRepo: logs,
			kbService:               &processSyncKBService{kb: kb},
			knowledgeService:        &knowledgeService{groupAccess: probe},
			nextcloudEventInbox:     apprepo.NewNextcloudEventInboxRepository(db),
			nextcloudSourcePairings: apprepo.NewNextcloudSourcePairingRepository(db),
		}}
}

func (f *nextcloudTaskAdmissionFixture) run(ctx context.Context, t *testing.T) error {
	t.Helper()
	raw, err := json.Marshal(f.payload)
	require.NoError(t, err)
	return f.svc.ProcessSync(ctx, asynq.NewTask(types.TypeDataSourceSync, raw))
}

func (f *nextcloudTaskAdmissionFixture) revoke(t *testing.T) {
	t.Helper()
	if f.payload.Trigger == "nextcloud_event" {
		require.NoError(t, f.db.Exec(`UPDATE nextcloud_event_connections SET status = 'revoked'`).Error)
	} else {
		require.NoError(t, f.db.Exec(`UPDATE nextcloud_candidate_retry_jobs SET lease_until = ?`,
			time.Now().UTC().Add(-time.Minute)).Error)
	}
}

// Nextcloud admission and live group authorization must both apply to the
// same task, even when the worker starts with a different ambient identity.
func TestProcessSyncNextcloudTaskRestoresActorBeforeAdmissionAndAuthorization(t *testing.T) {
	for _, trigger := range []string{"nextcloud_event", "nextcloud_candidate_retry"} {
		for _, actor := range []struct {
			name      string
			initiator types.TaskInitiator
			principal types.Principal
			caller    types.Caller
		}{
			{name: "human", initiator: types.TaskInitiator{UserID: "actor", CallerTenantID: 8,
				Role: types.TenantRoleContributor},
				principal: types.Principal{Type: types.PrincipalWebUser, ID: "actor"},
				caller:    types.Caller{TenantID: 8, UserID: "actor", Role: types.TenantRoleContributor}},
			{name: "legacy", principal: types.Principal{Type: "background_unverified", ID: "task"},
				caller: types.Caller{Role: types.TenantRoleViewer}},
			{name: "machine", initiator: types.TaskInitiator{APIKeyID: 42, APIKeyName: "scheduler", UserID: "actor"},
				principal: types.Principal{Type: types.PrincipalAPITenant, ID: "task"},
				caller:    types.Caller{Role: types.TenantRoleViewer}},
		} {
			t.Run(trigger+"/"+actor.name, func(t *testing.T) {
				f := newNextcloudTaskAdmissionFixture(t, trigger)
				f.payload.Initiator = actor.initiator
				ambient := types.WithTaskAuthorization(context.Background(), 99,
					types.TaskInitiator{UserID: "ambient-admin", Role: types.TenantRoleAdmin})
				require.ErrorIs(t, f.run(ambient, t), asynq.SkipRetry)
				require.Equal(t, 1, f.access.calls)
				for _, ctx := range []context.Context{f.logs.createCtx, f.access.ctx} {
					principal, ok := types.PrincipalFromContext(ctx)
					require.True(t, ok)
					require.Equal(t, actor.principal, principal)
					require.Equal(t, actor.caller, types.CallerFromContext(ctx))
					tenantID, ok := types.TenantIDFromContext(ctx)
					require.True(t, ok)
					require.Equal(t, uint64(7), tenantID)
					require.True(t, types.IsBackgroundTask(ctx))
				}
				require.Equal(t, types.SyncLogStatusFailed, f.logs.logs["log"].Status)
				require.Equal(t, "processing permission revoked", f.logs.logs["log"].ErrorMessage)
			})
		}
	}
}

func TestProcessSyncNextcloudTaskAdmissionRemainsFencedAndIdempotent(t *testing.T) {
	for _, trigger := range []string{"nextcloud_event", "nextcloud_candidate_retry"} {
		for _, scenario := range []string{"revoked before start", "revoked while admitting", "duplicate delivery"} {
			t.Run(trigger+"/"+scenario, func(t *testing.T) {
				f := newNextcloudTaskAdmissionFixture(t, trigger)
				f.payload.Initiator = types.TaskInitiator{UserID: "actor", Role: types.TenantRoleContributor}
				switch scenario {
				case "revoked before start":
					f.revoke(t)
				case "revoked while admitting":
					f.logs.afterCreate = func() { f.revoke(t) }
				case "duplicate delivery":
					f.logs.logs["log"] = &types.SyncLog{ID: "log", DataSourceID: "ds",
						TenantID: 7, Status: types.SyncLogStatusRunning}
				}
				require.NoError(t, f.run(context.Background(), t))
				require.Zero(t, f.access.calls, "revoked or duplicate tasks must stop before source access")
				if scenario == "revoked before start" {
					require.Zero(t, f.logs.createCalls)
					require.Empty(t, f.logs.logs)
				} else {
					require.Equal(t, 1, f.logs.createCalls)
					if scenario == "revoked while admitting" {
						require.Equal(t, types.SyncLogStatusCanceled, f.logs.logs["log"].Status)
					} else {
						require.Equal(t, types.SyncLogStatusRunning, f.logs.logs["log"].Status)
					}
				}
			})
		}
	}
}
