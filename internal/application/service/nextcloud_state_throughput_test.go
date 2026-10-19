package service

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/application/repository"
	nextcloud "github.com/Tencent/WeKnora/internal/datasource/connector/nextcloud"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/utils"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func TestNextcloudStateTenThousandMetadataHTTPDurableACKThroughput(t *testing.T) {
	if testing.Short() {
		t.Skip("metadata-only 10k throughput workload")
	}
	db, _, state, _, _, _, doc := scopedGraphPipelineFixture(t, false)
	receiver, err := gorm.Open(sqlite.Open("file:"+filepath.Join(t.TempDir(), "receiver.db")+"?_journal_mode=WAL&_synchronous=FULL&_busy_timeout=5000"), &gorm.Config{})
	require.NoError(t, err)
	receiverSQL, err := receiver.DB()
	require.NoError(t, err)
	receiverSQL.SetMaxOpenConns(1)
	defer receiverSQL.Close()
	require.NoError(t, receiver.Exec("CREATE TABLE state_inbox(event_id TEXT PRIMARY KEY,body_sha256 TEXT NOT NULL,received_at BIGINT NOT NULL)").Error)
	var received atomic.Int64
	key := repository.NextcloudStateSigningKey([]byte("0123456789abcdef0123456789abcdef"))
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, err := io.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(500)
			return
		}
		digest := sha256.Sum256(raw)
		mac := hmac.New(sha256.New, key)
		mac.Write(nextcloud.StatePushCanonical(r.URL.EscapedPath(), hex.EncodeToString(digest[:]), r.Header.Get("X-WeKnora-Timestamp"), r.Header.Get("X-WeKnora-Nonce"), r.Header.Get("X-WeKnora-Connection-Id"), r.Header.Get("X-WeKnora-Key-Id")))
		if r.Header.Get("X-WeKnora-Signature") != hex.EncodeToString(mac.Sum(nil)) {
			w.WriteHeader(401)
			return
		}
		var payload repository.NextcloudFileStateV2
		if json.Unmarshal(raw, &payload) != nil {
			w.WriteHeader(400)
			return
		}
		if err := receiver.Exec("INSERT OR IGNORE INTO state_inbox VALUES(?,?,?)", payload.EventID, hex.EncodeToString(digest[:]), time.Now().UnixMilli()).Error; err != nil {
			w.WriteHeader(500)
			return
		}
		received.Add(1)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{"event_id": payload.EventID, "status": "accepted", "persisted": true})
	}))
	defer server.Close()
	t.Setenv("WEKNORA_NEXTCLOUD_DEV_HTTP", "1")
	t.Setenv("WEKNORA_NEXTCLOUD_ALLOWED_ORIGINS", server.URL)
	cfg := &types.DataSourceConfig{Type: types.ConnectorTypeNextcloud, ResourceIDs: []string{"binding-77"}, Settings: map[string]any{"base_url": server.URL}, Credentials: map[string]any{"token": "metadata-workload-test-only", "key_id": "machine"}}
	blob, err := cfg.ToJSON()
	require.NoError(t, err)
	require.NoError(t, db.Table("data_sources").Where("id='ds-77'").Update("config", blob).Error)
	base, hash, binding, err := repository.NextcloudEventDataSourceIdentity(blob)
	require.NoError(t, err)
	require.NoError(t, db.Table("nextcloud_source_pairings").Where("datasource_id='ds-77'").Updates(map[string]any{"datasource_base_url": base, "datasource_config_sha256": hash}).Error)
	require.NoError(t, db.Table("nextcloud_event_connections").Where("connection_id='pipeline-connection-0001'").Updates(map[string]any{"datasource_base_url": base, "datasource_config_sha256": hash}).Error)
	encrypted, err := utils.EncryptAESGCM("0123456789abcdef0123456789abcdef", utils.GetAESKey())
	require.NoError(t, err)
	require.NoError(t, db.Table("nextcloud_event_connections").Where("connection_id='pipeline-connection-0001'").Update("current_secret_ciphertext", encrypted).Error)
	require.NoError(t, db.Exec("DELETE FROM nextcloud_status_outbox").Error)
	now := time.Now().UTC().Unix()
	var pair repository.NextcloudSourcePairing
	require.NoError(t, db.Where("datasource_id='ds-77'").Take(&pair).Error)
	require.NoError(t, db.Table("nextcloud_source_status_epochs").Create(map[string]any{"connection_id": "pipeline-connection-0001", "publication_epoch": 0, "pair_operation_id": pair.OperationID, "verified_at": now}).Error)
	// These are declared metadata workloads, not fabricated parse/graph success.
	for batch := 0; batch < 100; batch++ {
		versions := []map[string]any{}
		states := []map[string]any{}
		outbox := []map[string]any{}
		for ordinal := 0; ordinal < 100; ordinal++ {
			file := strconv.Itoa(1000 + batch*100 + ordinal)
			event := "metadata-" + file
			stages := map[string]repository.NextcloudStageState{}
			for _, stage := range []string{"parse", "chunk", "embedding", "index"} {
				stages[stage] = repository.NextcloudStageState{State: "pending"}
			}
			body, _ := json.Marshal(repository.NextcloudFileStateV2{SchemaVersion: 2, EventID: event, ConnectionID: "pipeline-connection-0001", InstanceID: "instance", BindingID: binding, PairOperationID: pair.OperationID, TenantID: "7", KnowledgeBaseID: doc.KnowledgeBaseID, DataSourceID: "ds-77", FileID: file, SourceETag: "etag-" + file, SourceContentRevision: "0", PublicationEpoch: "0", HeadRevision: "1", StatusRevision: "1", BuildAttempt: "0", ProcessingConfigRevision: "1", SyncState: "discovered", Stages: stages, PermissionRevision: "0", WithdrawalState: "none", PermissionReclaimState: "unverified", GCState: "cleanup_pending", PublicationState: "building", Graph: repository.NextcloudGraphState{State: "not_enabled"}, OccurredAt: now, LastVerifiedAt: &now})
			versions = append(versions, map[string]any{"tenant_id": 7, "knowledge_base_id": doc.KnowledgeBaseID, "datasource_id": "ds-77", "external_id": "nextcloud:instance:" + file, "desired_etag": "etag-" + file, "candidate_knowledge_id": "", "state": "staging", "updated_at": time.Now().UTC()})
			states = append(states, map[string]any{"connection_id": "pipeline-connection-0001", "file_id": file, "publication_epoch": 0, "head_revision": 1, "status_revision": 1, "config_revision": 1, "head_identity": "metadata-workload", "config_digest": "metadata-workload", "payload_json": string(body), "body_digest": strings.Repeat("0", 64), "latest_event_id": event, "last_verified_at": now, "occurred_at": now})
			outbox = append(outbox, map[string]any{"event_id": event, "connection_id": "pipeline-connection-0001", "file_id": file, "publication_epoch": 0, "head_revision": 1, "status_revision": 1, "payload_json": string(body), "state": "pending", "priority": 1, "created_at": now})
		}
		require.NoError(t, db.Transaction(func(tx *gorm.DB) error {
			if err := tx.Table("nextcloud_source_versions").Create(&versions).Error; err != nil {
				return err
			}
			if err := tx.Table("nextcloud_file_states").Create(&states).Error; err != nil {
				return err
			}
			return tx.Table("nextcloud_status_outbox").Create(&outbox).Error
		}))
	}
	dispatcher := NewNextcloudStateDispatcher(state, nil)
	started := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	dispatcher.deliver(ctx, time.Now().UTC())
	elapsed := time.Since(started)
	var delivered int64
	require.NoError(t, db.Table("nextcloud_status_outbox").Where("state='delivered'").Count(&delivered).Error)
	require.EqualValues(t, 10000, delivered)
	require.EqualValues(t, 10000, received.Load())
	var persisted int64
	require.NoError(t, receiver.Table("state_inbox").Count(&persisted).Error)
	require.EqualValues(t, 10000, persisted)
	var p95 int64
	require.NoError(t, receiver.Table("state_inbox").Select("received_at").Order("received_at").Offset(9499).Limit(1).Scan(&p95).Error)
	t.Logf("p95_receiver_persist_lag_ms=%d dispatch_cadence_upper_bound_ms=5000", p95-started.UnixMilli())
	t.Log(fmt.Sprintf("metadata_rows=10000 exact_persisted_acks=%d elapsed_ms=%d throughput_per_second=%.1f entire_batch_sender_lag_ms=%d; includes actual HTTP, HMAC, sender DB and receiver FULL SQLite commits; excludes Nextcloud Files/LDAP costs and document bodies", persisted, elapsed.Milliseconds(), 10000/elapsed.Seconds(), elapsed.Milliseconds()))
}
