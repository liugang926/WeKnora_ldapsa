package service

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	apprepo "github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/datasource"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/google/uuid"
	"github.com/hibiken/asynq"
	"github.com/stretchr/testify/require"
)

type automaticSourceProbe struct {
	datasource.Connector
	calls  int
	ctx    context.Context
	err    error
	before func(context.Context, datasource.StreamHandler) error
}

func (p *automaticSourceProbe) FetchStream(
	ctx context.Context,
	_ *types.DataSourceConfig,
	_ *types.SyncCursor,
	handler datasource.StreamHandler,
) (*types.SyncCursor, error) {
	p.calls++
	p.ctx = ctx
	if p.before != nil {
		if err := p.before(ctx, handler); err != nil {
			return nil, err
		}
	}
	return nil, p.err
}

// Pair through the actual scoped producer, AD delegation, manager signature and
// grant snapshot. Unlike a generic independent pair, this is a managed and
// restricted KB with no invented WeKnora web user for its Nextcloud manager.
func automaticSourceFixture(t *testing.T, tableCSV ...bool) (*machinePipelineFixture, *atomic.Bool) {
	t.Helper()
	upstream := newScopedConfigurationWorkerSource(t, &atomic.Int32{})
	address, err := url.Parse(upstream.URL)
	require.NoError(t, err)
	proxy := httputil.NewSingleHostReverseProxy(address)
	revoked := &atomic.Bool{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if len(tableCSV) > 0 && tableCSV[0] &&
			serveAutomaticSourceCSV(t, w, r) {
			return
		}
		if strings.HasSuffix(r.URL.Path, "/authorize-manager") && revoked.Load() {
			w.WriteHeader(http.StatusForbidden)
			return
		}
		proxy.ServeHTTP(w, r)
	}))
	t.Cleanup(server.Close)
	var credential types.NextcloudOnboardingCredential
	var operation types.NextcloudOnboardingOperation
	var pairCtx context.Context
	live := &machinePipelineSource{Pair: apprepo.NextcloudSourcePairing{
		OperationID: uuid.NewString(), TenantID: 7,
		KnowledgeBaseID: "kb", InstanceID: "instance", BindingID: scopedWorkerSourceBinding, BaseURL: server.URL,
		PublicationEpoch: 1, KeyID: "actual_worker_source",
	}, Token: "actual-worker-source", ExternalID: "nextcloud:instance:77", ETag: "actual-etag"}
	live.BeforePrepare = func(
		f *enrichmentTestFixture,
		kb *types.KnowledgeBase,
		pair *apprepo.NextcloudSourcePairing,
	) (context.Context, error) {
		installScopedWorkerTemplateDDL(t, f)
		require.NoError(
			t,
			f.db.Exec(
				("CREATE UNIQUE INDEX IF NOT EXISTS automatic_source_policy_key ON resource_" +
					"access_policies(tenant_id,resource_type,resource_id)"),
			).Error,
		)
		now := time.Now().UTC()
		require.NoError(t, f.db.Create(&types.Directory{
			ID: scopedWorkerDirectory, Protocol: types.DirectoryProtocolAD,
			Enabled: true, SnapshotVersion: 1, LastSuccessfulSyncAt: &now, StaleAfterSeconds: 900,
		}).Error)
		require.NoError(t, f.db.Create(&types.DirectoryIdentity{
			ID: "automatic-source-manager", DirectoryID: scopedWorkerDirectory,
			ObjectGUID: scopedWorkerGUID, Status: types.DirectoryObjectActive, SnapshotVersion: 1,
		}).Error)
		require.NoError(
			t,
			f.db.Create(
				&types.NextcloudOnboardingDelegation{
					TenantID:    7,
					DirectoryID: scopedWorkerDirectory,
					ObjectGUID:  scopedWorkerGUID,
				},
			).Error,
		)
		require.NoError(
			t,
			f.db.Model(&types.Model{}).
				Where("id = ?", kb.EmbeddingModelID).
				Update("type", types.ModelTypeEmbedding).
				Error,
		)
		require.NoError(
			t,
			f.db.Model(&types.Model{}).
				Where("id = ?", kb.SummaryModelID).
				Update("type", types.ModelTypeKnowledgeQA).
				Error,
		)
		credential = scopedWorkerCredential(t, f.db, "automatic-source-credential", server.URL, kb.ID)
		store := apprepo.NewNextcloudOnboardingStore(f.db)
		var err error
		operation, err = store.Reserve(context.Background(), credential, types.NextcloudOnboardingRequest{
			OperationID:          pair.OperationID,
			TenantID:             7,
			InstanceID:           "instance",
			BindingID:            pair.BindingID,
			ActorDirectoryID:     scopedWorkerDirectory,
			ActorObjectGUID:      scopedWorkerGUID,
			ProcessingTemplateID: kb.ID,
			Mode:                 "create",
			Name:                 "Actual automatic source",
		})
		if err != nil {
			return context.Background(), err
		}
		pair.KnowledgeBaseID = operation.KnowledgeBaseID
		var actual types.KnowledgeBase
		require.NoError(t, f.db.Where("id = ?", operation.KnowledgeBaseID).Take(&actual).Error)
		*kb = actual
		pairCtx, err = store.BeginPairProducer(
			context.Background(),
			credential,
			operation,
			scopedWorkerDirectory,
			scopedWorkerGUID,
		)
		return pairCtx, err
	}
	live.AfterActivate = func(f *enrichmentTestFixture, pair *apprepo.NextcloudSourcePairing) {
		scopedWorkerActivate(pairCtx, t, f, credential, operation, pair)
	}
	m := newMachinePipelineFixtureWithSource(t, false, "owned automatic source", live, false, true)
	groups := NewGroupAccessService(apprepo.NewGroupAccessRepository(m.f.db))
	ConfigureGroupAccessNextcloudScopes(groups, apprepo.NewNextcloudOnboardingStore(m.f.db))
	m.f.s.groupAccess = groups
	background := types.WithTaskAuthorization(context.Background(), 7, types.TaskInitiator{})
	permission, err := groups.EffectivePermission(
		background,
		7,
		types.GroupResourceTypeKnowledgeBase,
		m.ds.KnowledgeBaseID,
		types.ResourceActionEdit,
		time.Now().UTC(),
	)
	require.NoError(t, err)
	require.False(t, permission.Allowed, "a real restricted policy must reject an unverified ordinary task")
	return m, revoked
}

// Only the document transport is replaced for the CSV variant. Manager and
// personal authorization still reach the original signed controlled source.
func serveAutomaticSourceCSV(t *testing.T, w http.ResponseWriter, r *http.Request) bool {
	t.Helper()
	const bindingPath = "/bindings/" + scopedWorkerSourceBinding
	manifest := strings.HasSuffix(r.URL.Path, bindingPath+"/manifest")
	content := strings.HasSuffix(r.URL.Path, bindingPath+"/files/77/content")
	publication := strings.HasSuffix(r.URL.Path, bindingPath+"/files/77/publication-check")
	if !manifest && !content && !publication {
		return false
	}
	if !automaticSourceCSVSignature(t, r) {
		w.WriteHeader(http.StatusUnauthorized)
		return true
	}
	const artifact = "column_a,column_b\n1,2\n3,4\n"
	switch {
	case manifest:
		require.Equal(t, http.MethodGet, r.Method)
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{
			"generation": "actual-worker-csv-generation", "complete": true, "next_cursor": nil,
			"items": []map[string]any{{
				"file_id": 77, "etag": "actual-etag", "name": "source.csv", "path": "source.csv",
				"mime_type": "text/csv", "size": len(artifact), "mtime": 1700000000,
			}},
		}))
	case content:
		require.Equal(t, http.MethodGet, r.Method)
		require.Equal(t, `"actual-etag"`, r.Header.Get("If-Match"))
		w.Header().Set("ETag", `"actual-etag"`)
		w.Header().Set("Content-Type", "text/csv")
		_, err := io.WriteString(w, artifact)
		require.NoError(t, err)
	case publication:
		require.Equal(t, http.MethodPost, r.Method)
		var input struct {
			InstanceID string `json:"instance_id"`
			ETag       string `json:"etag"`
			Path       string `json:"path"`
		}
		require.NoError(t, json.NewDecoder(r.Body).Decode(&input))
		require.Equal(t, "instance", input.InstanceID)
		require.Equal(t, "actual-etag", input.ETag)
		require.Equal(t, "source.csv", input.Path)
		w.WriteHeader(http.StatusNoContent)
	}
	return true
}

func automaticSourceCSVSignature(t *testing.T, r *http.Request) bool {
	t.Helper()
	if r.Header.Get("Authorization") != "Bearer actual-worker-source" ||
		r.Header.Get("X-WeKnora-Key-Id") != "actual_worker_source" ||
		r.Header.Get("X-WeKnora-Timestamp") == "" || r.Header.Get("X-WeKnora-Nonce") == "" {
		t.Error("controlled CSV source request lacks its exact signed machine identity")
		return false
	}
	raw, err := io.ReadAll(r.Body)
	if err != nil {
		t.Error("cannot read controlled CSV signed body")
		return false
	}
	r.Body = io.NopCloser(strings.NewReader(string(raw)))
	bodySHA := sha256.Sum256(raw)
	canonical := strings.Join([]string{
		"weknora-hmac-sha256-v1", r.Method, r.URL.EscapedPath(), r.URL.RawQuery,
		hex.EncodeToString(bodySHA[:]), r.Header.Get("X-WeKnora-Timestamp"),
		r.Header.Get("X-WeKnora-Nonce"), r.Header.Get("X-WeKnora-Key-Id"),
	}, "\n")
	key := sha256.Sum256([]byte("actual-worker-source"))
	mac := hmac.New(sha256.New, key[:])
	_, err = mac.Write([]byte(canonical))
	if err != nil {
		t.Error("cannot authenticate controlled CSV request")
		return false
	}
	signature, err := hex.DecodeString(r.Header.Get("X-WeKnora-Signature"))
	if err != nil || !hmac.Equal(mac.Sum(nil), signature) {
		t.Error("controlled CSV request does not bind its actual body and source scope")
		return false
	}
	return true
}

func automaticSourcePayload(t *testing.T, m *machinePipelineFixture, trigger string) types.DataSourceSyncPayload {
	t.Helper()
	const logID = "automatic-dispatch-log"
	payload := types.DataSourceSyncPayload{DataSourceID: m.ds.ID, TenantID: 7, SyncLogID: logID, Trigger: trigger}
	var pair apprepo.NextcloudSourcePairing
	require.NoError(t, m.f.db.Where("datasource_id = ?", m.ds.ID).Take(&pair).Error)
	if trigger == "nextcloud_event" {
		var connection struct{ ConnectionID string }
		require.NoError(
			t,
			m.f.db.Table("nextcloud_event_connections").
				Where("datasource_id = ? AND status = 'active'", m.ds.ID).
				Take(&connection).
				Error,
		)
		require.NoError(
			t,
			m.f.db.Exec(
				`INSERT INTO nextcloud_event_dispatch(connection_id,state,last_sync_log_id) VALUES (?,'queued',?)
			ON CONFLICT(connection_id) DO UPDATE SET state='queued',last_sync_log_id=excluded.last_sync_log_id`,
				connection.ConnectionID,
				logID,
			).Error,
		)
		payload.NextcloudEventConnectionID, payload.NextcloudEventConfigSHA256 = connection.ConnectionID, pair.ConfigSHA
	} else {
		// A retry follows a real prior inventory, never a first-import cursor.
		// Obtain that inventory through the signed connector before introducing
		// the failed candidate and its durable retry lease.
		config, err := m.ds.ParseConfig()
		require.NoError(t, err)
		connector := m.source.connectorRegistryConnectorForTest(t)
		_, cursor, err := connector.(datasource.FullSyncWithCursor).FetchAllFromCursor(
			context.Background(),
			config,
			config.ResourceIDs,
			nil,
		)
		require.NoError(t, err)
		require.NotNil(t, cursor)
		m.ds.LastSyncCursor, err = cursor.ToJSON()
		require.NoError(t, err)
		require.NoError(t, m.source.dsRepo.UpdateSyncState(context.Background(), m.ds))
		now := time.Now().UTC()
		metadata, err := json.Marshal(map[string]string{
			"datasource_id": m.ds.ID, "external_id": m.externalID,
			"source_resource_id": pair.BindingID, "nextcloud_instance_id": pair.InstanceID,
			"nextcloud_binding_id": pair.BindingID, "nextcloud_file_id": "77", "nextcloud_etag": "actual-etag",
		})
		require.NoError(t, err)
		require.NoError(t, m.f.db.Create(&types.Knowledge{
			ID: "automatic-failed-candidate", TenantID: 7,
			KnowledgeBaseID: m.ds.KnowledgeBaseID, Channel: types.ConnectorTypeNextcloud,
			Metadata: metadata, ParseStatus: types.ParseStatusFailed,
		}).Error)
		require.NoError(
			t,
			m.f.db.Exec(
				("INSERT INTO nextcloud_source_versions(tenant_id,knowledge_base_id,datasour"+
					"ce_id,external_id,desired_etag,candidate_knowledge_id,state)\n\t\t\tVALUES"+
					" (7,?,?,?,'actual-etag','automatic-failed-candidate','staging')"),
				m.ds.KnowledgeBaseID,
				m.ds.ID,
				m.externalID,
			).Error,
		)
		require.NoError(
			t,
			m.f.db.Exec(
				("INSERT INTO nextcloud_candidate_retry_jobs(tenant_id,knowledge_base_id,dat"+
					"asource_id,external_id,desired_etag,failed_candidate_id,\n\t\t\tfirst_stag"+
					"ed_at,next_attempt_at,state,lease_token,lease_until,sync_log_id)\n\t\t\tVA"+
					"LUES (7,?,?,?,'actual-etag','automatic-failed-candidate',?,?,'leased','aut"+
					"omatic-owned-lease',?,?)"),
				m.ds.KnowledgeBaseID,
				m.ds.ID,
				m.externalID,
				now,
				now,
				now.Add(time.Minute),
				logID,
			).Error,
		)
		payload.NextcloudRetryKnowledgeBaseID, payload.NextcloudRetryExternalID = m.ds.KnowledgeBaseID, m.externalID
		payload.NextcloudRetryETag, payload.NextcloudRetryCandidateID = "actual-etag", "automatic-failed-candidate"
		payload.NextcloudRetryInstanceID, payload.NextcloudRetryBindingID = pair.InstanceID, pair.BindingID
		payload.NextcloudRetryConfigSHA256, payload.NextcloudRetryPairOperationID = pair.ConfigSHA, pair.OperationID
		payload.NextcloudRetryPairingEpoch, payload.NextcloudRetryLeaseToken = pair.PublicationEpoch,
			"automatic-owned-lease"
	}
	return payload
}

func TestNextcloudAutomaticRestrictedSyncRequiresCurrentManagedDispatch(t *testing.T) {
	for _, trigger := range []string{"nextcloud_event", "nextcloud_candidate_retry"} {
		for _, scenario := range []string{
			"current",
			"manager revoked",
			"manager revoked during fetch",
			"pair retired",
			"wrong task",
			"unknown attempt",
			"replayed attempt",
			"cross KB",
			"bad retry token",
			"manual",
			"schedule",
			"legacy unknown",
			"forged ordinary origin",
		} {
			if trigger == "nextcloud_event" && scenario == "bad retry token" {
				continue
			}
			ordinary := scenario == "manual" || scenario == "schedule" || scenario == "legacy unknown" ||
				scenario == "forged ordinary origin"
			if trigger == "nextcloud_candidate_retry" && ordinary {
				continue
			}
			t.Run(trigger+"/"+scenario, func(t *testing.T) {
				m, revoked := automaticSourceFixture(t)
				payload := automaticSourcePayload(t, m, trigger)
				taskID := "ncevent:" + payload.NextcloudEventConnectionID + ":" + payload.SyncLogID
				if trigger == "nextcloud_candidate_retry" {
					taskID = "nccandidate:" + payload.SyncLogID
				}
				ctx := types.WithTaskRetryMetadata(types.WithTaskExecutionID(context.Background(), taskID), 0, 0)
				switch scenario {
				case "manager revoked":
					revoked.Store(true)
				case "pair retired":
					require.NoError(
						t,
						m.f.db.Exec(
							"UPDATE nextcloud_source_pairings SET state='pending' WHERE datasource_id=?",
							m.ds.ID,
						).Error,
					)
				case "wrong task":
					ctx = types.WithTaskExecutionID(ctx, "ncevent:forged")
				case "unknown attempt":
					ctx = types.WithTaskExecutionID(context.Background(), taskID)
				case "replayed attempt":
					ctx = types.WithTaskRetryMetadata(ctx, 1, 0)
				case "cross KB":
					if trigger == "nextcloud_candidate_retry" {
						payload.NextcloudRetryKnowledgeBaseID = "unrelated-kb"
					} else {
						payload.TenantID = 8
					}
				case "bad retry token":
					payload.NextcloudRetryLeaseToken = "forged"
				case "manual", "schedule", "legacy unknown", "forged ordinary origin":
					payload.Trigger = scenario
					if scenario == "legacy unknown" {
						payload.Trigger = ""
					}
					if scenario == "forged ordinary origin" {
						payload.Trigger = "manual"
						ctx = apprepo.WithIndependentKnowledgeInput(ctx)
					}
					require.NoError(t, m.source.syncLogRepo.Create(context.Background(), &types.SyncLog{
						ID:           payload.SyncLogID,
						DataSourceID: m.ds.ID,
						TenantID:     7,
						Status:       types.SyncLogStatusRunning,
						StartedAt:    time.Now().UTC(),
					}))
				}
				stop := errors.New("automatic dispatch crossed the real restricted ACL gate")
				connector := &automaticSourceProbe{Connector: m.source.connectorRegistryConnectorForTest(t), err: stop}
				if scenario == "manager revoked during fetch" {
					connector.before = func(ctx context.Context, handler datasource.StreamHandler) error {
						revoked.Store(true)
						return handler.(interface {
							ObserveNextcloudIdentity(context.Context, string, string) error
						}).ObserveNextcloudIdentity(ctx, "instance", scopedWorkerSourceBinding)
					}
				}
				registry := datasource.NewConnectorRegistry()
				require.NoError(t, registry.Register(connector))
				m.source.connectorRegistry = registry
				body, err := json.Marshal(payload)
				require.NoError(t, err)
				err = m.source.ProcessSync(ctx, asynq.NewTask(types.TypeDataSourceSync, body))
				switch scenario {
				case "current":
					require.ErrorIs(t, err, stop)
					require.Equal(t, 1, connector.calls)
					require.Empty(t, types.CallerFromContext(connector.ctx).UserID)
					principal, ok := types.PrincipalFromContext(connector.ctx)
					require.True(t, ok)
					require.Equal(t, "background_unverified", string(principal.Type))
				case "manager revoked during fetch":
					require.Equal(t, 1, connector.calls)
					require.Error(t, err)
					require.NotErrorIs(t, err, stop, "a later current-manager denial must close the active scan")
				default:
					require.Zero(t, connector.calls, "refused authority must never reach connector transport")
					require.NotErrorIs(t, err, stop)
					if ordinary {
						require.ErrorIs(
							t,
							err,
							asynq.SkipRetry,
							"ordinary tasks still require the actual restricted KB edit ACL",
						)
					}
				}
			})
		}
	}
}

func (s *DataSourceService) connectorRegistryConnectorForTest(t *testing.T) datasource.Connector {
	t.Helper()
	connector, err := s.connectorRegistry.Get(types.ConnectorTypeNextcloud)
	require.NoError(t, err)
	return connector
}
