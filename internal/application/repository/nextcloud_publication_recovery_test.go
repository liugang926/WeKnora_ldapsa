package repository

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/datasource/connector/nextcloud"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/postgres"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func recoveryTestPlan(t *testing.T, modify func(*NextcloudRecoveryPlan)) *NextcloudRecoveryPlan {
	t.Helper()
	p := NextcloudRecoveryPlan{SchemaVersion: 1, InstanceID: "instance", StreamID: "53f8d222-2331-4731-b799-9e179638cfd0", CheckpointRecordSHA256: strings.Repeat("a", 64), RecoveryRecordSHA256: strings.Repeat("b", 64), CheckpointSequence: "5", ThroughSequence: "7", CheckpointDatabaseChainSHA256: strings.Repeat("c", 64), ThroughDatabaseChainSHA256: strings.Repeat("d", 64), DatabasePrefixHashes: map[string]string{"5": strings.Repeat("c", 64), "6": strings.Repeat("e", 64), "7": strings.Repeat("d", 64)}, CloseBindings: []NextcloudRecoveryBinding{{"binding", "7"}}, WithdrawFiles: []NextcloudRecoveryFile{{"binding", "77", "6"}}, AuthoritativeReconciliationRequired: true}
	if modify != nil {
		modify(&p)
	}
	raw, err := json.Marshal(p)
	require.NoError(t, err)
	key := []byte("0123456789abcdef0123456789abcdef")
	mac := hmac.New(sha256.New, key)
	mac.Write(raw)
	envelope, err := json.Marshal(map[string]string{"plan": string(raw), "hmac_sha256": hex.EncodeToString(mac.Sum(nil))})
	require.NoError(t, err)
	verified, err := VerifyNextcloudRecoveryPlan(envelope, key)
	require.NoError(t, err)
	return verified
}
func recoveryTestAnchor() NextcloudRecoveryAnchor {
	return NextcloudRecoveryAnchor{"instance", "53f8d222-2331-4731-b799-9e179638cfd0", strings.Repeat("a", 64), "5", strings.Repeat("c", 64)}
}
func recoverySchema(t *testing.T, db *gorm.DB) {
	t.Helper()
	version := "sqlite/000064"
	if db.Dialector.Name() == "postgres" {
		version = "versioned/000145"
	}
	b, e := os.ReadFile("../../../migrations/" + version + "_nextcloud_publication_recovery.up.sql")
	require.NoError(t, e)
	require.NoError(t, db.Exec(string(b)).Error)
}
func recoveryFixture(t *testing.T, pg bool) (*knowledgeRepository, *gorm.DB, *types.Knowledge, *types.Knowledge) {
	t.Helper()
	var r *knowledgeRepository
	var db *gorm.DB
	if pg {
		r, db = nextcloudVersionPGTestRepo(t)
		require.NoError(t, db.AutoMigrate(&types.KnowledgeBase{}, &types.Chunk{}, &nextcloudCandidateRetryJob{}))
		b, e := os.ReadFile("../../../migrations/versioned/000126_nextcloud_source_revisions.up.sql")
		require.NoError(t, e)
		simplePGExec(t, db, string(b))
	} else {
		r, db = newNextcloudVersionTestRepo(t)
		require.NoError(t, db.AutoMigrate(&types.KnowledgeBase{}))
		b, e := os.ReadFile("../../../migrations/sqlite/000045_nextcloud_source_revisions.up.sql")
		require.NoError(t, e)
		require.NoError(t, db.Exec(string(b)).Error)
	}
	require.NoError(t, db.AutoMigrate(&types.SyncLog{}))
	require.NoError(t, db.Create(&types.KnowledgeBase{ID: "kb", TenantID: 7, Name: "recovery"}).Error)
	require.NoError(t, db.Model(&NextcloudSourcePairing{}).Where("datasource_id=?", "ds").Updates(map[string]any{"publication_epoch": 3, "key_id": "pair_test"}).Error)
	recoverySchema(t, db)
	old := insertNextcloudVersionTestKnowledge(t, db, "old-ready", "", types.ParseStatusCompleted, "enabled")
	old.FileName = "file.md"
	old.FilePath = filepath.Join(t.TempDir(), "registered-imported-copy.md")
	require.NoError(t, os.WriteFile(old.FilePath, []byte("original bytes remain"), 0600))
	require.NoError(t, db.Save(old).Error)
	require.NoError(t, r.StageNextcloudVersion(context.Background(), 7, "kb", "ds", "nextcloud:instance:77", "old-etag", old.ID))
	ready, e := r.PublishNextcloudVersion(context.Background(), old.ID)
	require.NoError(t, e)
	require.True(t, ready)
	pending := insertNextcloudVersionTestKnowledge(t, db, "pending-worker", "", types.ParseStatusPending, "disabled")
	m := pending.GetMetadata()
	m["external_id"] = "nextcloud:instance:78"
	m["nextcloud_file_id"] = "78"
	pending.Metadata, _ = json.Marshal(m)
	require.NoError(t, db.Save(pending).Error)
	require.NoError(t, r.StageNextcloudVersion(context.Background(), 7, "kb", "ds", "nextcloud:instance:78", "pending-etag", pending.ID))
	require.NoError(t, db.Create(&types.SyncLog{ID: "pending-sync", TenantID: 7, DataSourceID: "ds", Status: types.SyncLogStatusRunning, StartedAt: time.Now().UTC(), WorkerActive: true, WorkerAttemptToken: "old-worker"}).Error)
	return r, db, old, pending
}
func simplePGExec(t *testing.T, db *gorm.DB, script string) {
	t.Helper()
	sqlDB, e := db.DB()
	require.NoError(t, e)
	_, e = sqlDB.Exec(script)
	require.NoError(t, e)
}

type recoveryGuardDS struct{ db *gorm.DB }

func (d recoveryGuardDS) FindByID(ctx context.Context, id string) (*types.DataSource, error) {
	var ds types.DataSource
	e := d.db.WithContext(ctx).Where("id=?", id).Take(&ds).Error
	return &ds, e
}

func recoveryPostgresBackupRollback(t *testing.T, r *knowledgeRepository, db *gorm.DB, old *types.Knowledge) *gorm.DB {
	t.Helper()
	ctx := context.Background()
	if os.Getenv("WEKNORA_RECOVERY_PG_DUMP_RESTORE") != "1" {
		return db
	}

	var schema string
	require.NoError(t, db.Raw("SELECT current_schema()").Scan(&schema).Error)
	require.Regexp(t, regexp.MustCompile(`^content_lease_[a-f0-9]{8}$`), schema)
	dsn := os.Getenv("WEKNORA_LEASE_TEST_POSTGRES_DSN")
	dump, err := exec.Command("pg_dump", "--format=custom", "--schema="+schema, "--dbname="+dsn).Output()
	require.NoError(t, err)
	require.NoError(t, r.TombstoneNextcloudVersion(ctx, 7, "kb", "ds", "nextcloud:instance:77"))
	require.Empty(t, reloadedNextcloudETag(t, db, old.ID))
	sqlDB, err := db.DB()
	require.NoError(t, err)
	require.NoError(t, sqlDB.Close())
	restore := exec.Command("pg_restore", "--clean", "--if-exists", "--dbname="+dsn)
	restore.Stdin = bytes.NewReader(dump)
	output, err := restore.CombinedOutput()
	require.NoError(t, err, string(output))
	db, err = gorm.Open(postgres.Open(dsn+" search_path="+schema), &gorm.Config{})
	require.NoError(t, err)
	restoredSQL, err := db.DB()
	require.NoError(t, err)
	t.Cleanup(func() { _ = restoredSQL.Close() })
	r.db = db
	require.Equal(t, "old-etag", reloadedNextcloudETag(t, db, old.ID))

	return db
}

func recoveryClosureContract(t *testing.T, pg bool) {
	r, db, old, pending := recoveryFixture(t, pg)
	ctx := context.Background()
	store := NewNextcloudPublicationRecoveryStore(db)
	_, identity, e := r.ResolveNextcloudSourceLineage(ctx, 7, "kb", old.ID)
	require.NoError(t, e)
	lineage, e := types.NewCompleteSourceLineage(identity)
	require.NoError(t, e)
	buildLease, e := NewNextcloudContentLeaseStore(db).AcquireKnowledge(ctx, NextcloudContentScope{7, "kb", pending.ID, "ds", "nextcloud:instance:78"}, NextcloudContentBuildLease, "old-worker", time.Minute)
	require.NoError(t, e)
	inv, e := store.Capture(ctx, recoveryTestAnchor())
	require.NoError(t, e)
	pin, e := recoveryJSONSHA(inv)
	require.NoError(t, e)
	// An opt-in fixture executes pg_dump/pg_restore inside the owned internal
	// PostgreSQL container. Only its exact per-test nonce schema is captured.
	if pg {
		db = recoveryPostgresBackupRollback(t, r, db, old)
		store = NewNextcloudPublicationRecoveryStore(db)
	}
	// Real SQLite backup-era rollback: the later live tombstone is lost on restore,
	// while the independently retained external plan preserves its withdrawal.
	if !pg {
		backup := filepath.Join(t.TempDir(), "backup.sqlite")
		require.NoError(t, db.Exec("VACUUM INTO ?", backup).Error)
		require.NoError(t, r.TombstoneNextcloudVersion(ctx, 7, "kb", "ds", "nextcloud:instance:77"))
		require.Empty(t, reloadedNextcloudETag(t, db, old.ID))
		restored, e := gorm.Open(sqlite.Open(backup), &gorm.Config{})
		require.NoError(t, e)
		sqlDB, e := restored.DB()
		require.NoError(t, e)
		sqlDB.SetMaxOpenConns(1)
		t.Cleanup(func() { _ = sqlDB.Close() })
		db = restored
		r.db = restored
		store = NewNextcloudPublicationRecoveryStore(restored)
		require.Equal(t, "old-etag", reloadedNextcloudETag(t, db, old.ID))
	}
	physical := old.FilePath
	p := recoveryTestPlan(t, nil)
	receipt, e := store.Apply(ctx, p, inv, pin)
	require.NoError(t, e)
	require.Equal(t, p.digest, receipt.PlanSHA256)
	again, e := store.Apply(ctx, p, inv, pin)
	require.NoError(t, e)
	require.Equal(t, receipt, again)
	require.Empty(t, reloadedNextcloudETag(t, db, old.ID))
	var persisted types.Knowledge
	require.NoError(t, db.Unscoped().Where("id=?", old.ID).Take(&persisted).Error)
	require.Equal(t, types.ParseStatusCompleted, persisted.ParseStatus)
	// The actual public guard, RAG source snapshot and history lineage snapshot all
	// deny the restored ready row. An old build lease also cannot complete writes.
	guard := access.NewNextcloudPublicationGuard(nil, recoveryGuardDS{db})
	require.ErrorIs(t, guard.CheckKnowledge(ctx, &persisted), access.ErrNextcloudPublicationDenied)
	_, _, e = r.ResolveNextcloudSourceLineage(ctx, 7, "kb", old.ID)
	require.ErrorIs(t, e, ErrNextcloudLineageSnapshotDenied)
	matches, e := r.RecheckNextcloudSourceLineage(ctx, lineage)
	require.NoError(t, e)
	require.False(t, matches)
	require.ErrorIs(t, NewNextcloudContentLeaseStore(db).ValidateBuildLeaseInTx(ctx, db, buildLease, NextcloudContentScope{7, "kb", pending.ID, "ds", "nextcloud:instance:78"}), ErrNextcloudContentLeaseDenied)
	var log types.SyncLog
	require.NoError(t, db.Where("id='pending-sync'").Take(&log).Error)
	require.Equal(t, types.SyncLogStatusCanceled, log.Status)
	require.False(t, log.WorkerActive)
	require.Error(t, db.Model(&types.DataSource{}).Where("id='ds'").Update("status", types.DataSourceStatusActive).Error)
	require.Error(t, db.Model(&nextcloudSourceVersion{}).Where("external_id='nextcloud:instance:77'").Updates(map[string]any{"state": "published", "candidate_knowledge_id": old.ID, "desired_etag": "old-etag"}).Error)
	bad := recoveryTestPlan(t, func(p *NextcloudRecoveryPlan) { p.RecoveryRecordSHA256 = strings.Repeat("f", 64) })
	_, e = store.Apply(ctx, bad, inv, pin)
	require.ErrorIs(t, e, ErrNextcloudRecoveryDenied)
	mutated := *p
	mutated.InstanceID = "cross-instance"
	_, e = store.Apply(ctx, &mutated, inv, pin)
	require.ErrorIs(t, e, ErrNextcloudRecoveryDenied)
	data, e := os.ReadFile(physical)
	require.NoError(t, e)
	require.Equal(t, "original bytes remain", string(data))
	var retiredFence nextcloudContentFence
	require.NoError(t, db.Where("tenant_id=? AND knowledge_base_id=? AND knowledge_id=?", 7, "kb", old.ID).Take(&retiredFence).Error)
	require.NotNil(t, retiredFence.RetiredAtMS)
	_, e = NewNextcloudContentLeaseStore(db).ClaimKnowledgeGC(ctx, NextcloudContentScope{7, "kb", old.ID, "ds", "nextcloud:instance:77"}, time.UnixMilli(*retiredFence.RetiredAtMS), time.Minute)
	require.ErrorIs(t, e, ErrNextcloudContentGCUncovered)
	var scope nextcloudRecoveryScope
	require.NoError(t, db.Where("plan_sha256=?", p.digest).Take(&scope).Error)
	_, e = store.Reconcile(ctx, p.digest, "cross-pair")
	require.ErrorIs(t, e, ErrNextcloudRecoveryDenied)
}
func TestNextcloudPublicationRecoverySQLiteBackupReplay(t *testing.T) {
	recoveryClosureContract(t, false)
}
func TestPostgresNextcloudPublicationRecoveryClosure(t *testing.T) { recoveryClosureContract(t, true) }

func recoveryNewGenerationContract(t *testing.T, pg bool) {
	r, db, old, _ := recoveryFixture(t, pg)
	ctx := context.Background()
	require.NoError(t, db.AutoMigrate(&nextcloudControlRecoveryGate{}))
	buildMigration := "../../../migrations/sqlite/000079_nextcloud_recovery_build_leases.up.sql"
	if pg {
		buildMigration = "../../../migrations/versioned/000160_nextcloud_recovery_build_leases.up.sql"
	}
	ddl, e := os.ReadFile(buildMigration)
	require.NoError(t, e)
	require.NoError(t, db.Exec(string(ddl)).Error)
	state := "active"
	generation := "g1"
	epoch := int64(5)
	withdrawnPresent := false
	manifestCalls := 0
	unstable := false
	wrongInstance := false
	wrongPair := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		require.NotEmpty(t, req.Header.Get("X-WeKnora-Signature"))
		require.Equal(t, "pair_test", req.Header.Get("X-WeKnora-Key-Id"))
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.HasSuffix(req.URL.Path, "/source-pairing/commit"):
			var body map[string]string
			require.NoError(t, json.NewDecoder(req.Body).Decode(&body))
			ack := map[string]string{"operation_id": body["operation_id"], "instance_id": body["instance_id"], "binding_id": "binding", "tenant_id": body["tenant_id"], "knowledge_base_id": body["knowledge_base_id"], "data_source_id": body["data_source_id"], "state": "active"}
			if wrongPair {
				ack["data_source_id"] = "cross-tenant-source"
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"pairing": ack})
		case strings.HasSuffix(req.URL.Path, "/files/78/publication-check"):
			var body struct {
				InstanceID string `json:"instance_id"`
				ETag       string `json:"etag"`
				Path       string `json:"path"`
			}
			require.NoError(t, json.NewDecoder(req.Body).Decode(&body))
			require.Equal(t, "instance", body.InstanceID)
			require.Equal(t, "new-etag", body.ETag)
			require.Equal(t, "current.md", body.Path)
			w.WriteHeader(http.StatusNoContent)
		case strings.HasSuffix(req.URL.Path, "/capabilities"):
			instance := "instance"
			if wrongInstance {
				instance = "cross-instance"
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"protocol_version": "1", "instance_id": instance})
		case strings.HasSuffix(req.URL.Path, "/bindings"):
			_ = json.NewEncoder(w).Encode(map[string]any{"bindings": []any{map[string]any{"id": "binding", "name": "owned", "root_file_id": 1, "publication_state": state, "publication_epoch": epoch}}})
		case strings.HasSuffix(req.URL.Path, "/recovery-state"):
			_ = json.NewEncoder(w).Encode(map[string]any{"instance_id": "instance", "binding_id": "binding", "publication_epoch": epoch, "stream_id": recoveryTestAnchor().StreamID, "sequence": "7", "chain_sha256": strings.Repeat("d", 64), "controls_closed": false, "control_inventory_sha256": strings.Repeat("f", 64)})
		case strings.HasSuffix(req.URL.Path, "/manifest"):
			manifestCalls++
			g := generation
			if unstable && manifestCalls%2 == 0 {
				g = "fork"
			}
			items := []any{map[string]any{"file_id": 78, "etag": "new-etag", "name": "current.md", "path": "current.md", "mime_type": "text/markdown", "size": 10, "mtime": 1, "url": "/file/78"}}
			if withdrawnPresent {
				items = append(items, map[string]any{"file_id": 77, "etag": "old-etag", "name": "old.md", "path": "old.md", "mime_type": "text/markdown", "size": 10, "mtime": 1, "url": "/file/77"})
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"generation": g, "items": items, "complete": true})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	t.Setenv("WEKNORA_NEXTCLOUD_DEV_HTTP", "1")
	t.Setenv("WEKNORA_NEXTCLOUD_ALLOWED_ORIGINS", server.URL)
	var ds types.DataSource
	require.NoError(t, db.Where("id='ds'").Take(&ds).Error)
	cfg, e := ds.ParseConfig()
	require.NoError(t, e)
	cfg.Settings["base_url"] = server.URL
	ds.Config, e = cfg.ToJSON()
	require.NoError(t, e)
	require.NoError(t, db.Save(&ds).Error)
	require.NoError(t, db.Where("id=?", ds.ID).Take(&ds).Error)
	base, sha, binding, e := NextcloudEventDataSourceIdentity(ds.Config)
	require.NoError(t, e)
	require.NoError(t, db.Model(&NextcloudSourcePairing{}).Where("datasource_id='ds'").Updates(map[string]any{"datasource_base_url": base, "datasource_config_sha256": sha, "binding_id": binding}).Error)
	store := NewNextcloudPublicationRecoveryStore(db)
	inv, e := store.Capture(ctx, recoveryTestAnchor())
	require.NoError(t, e)
	pin, _ := recoveryJSONSHA(inv)
	if pg {
		db = recoveryPostgresBackupRollback(t, r, db, old)
		store = NewNextcloudPublicationRecoveryStore(db)
	}
	plan := recoveryTestPlan(t, nil)
	_, e = store.Apply(ctx, plan, inv, pin)
	require.NoError(t, e)
	op := inv.Pairs[0].OperationID
	metadata, e := nextcloudControlMetadataInventory(db)
	require.NoError(t, e)
	_, e = store.ReopenControls(ctx, plan.digest, inv, pin, plan, onboardingDigest(metadata), "", "")
	require.ErrorIs(t, e, ErrNextcloudRecoveryDenied)
	wrongPair = true
	_, e = store.Reconcile(ctx, plan.digest, op)
	require.Error(t, e)
	wrongPair = false
	wrongInstance = true
	_, e = store.Reconcile(ctx, plan.digest, op)
	require.Error(t, e)
	wrongInstance = false
	unstable = true
	_, e = store.Reconcile(ctx, plan.digest, op)
	require.Error(t, e)
	unstable = false
	withdrawnPresent = true
	_, e = store.Reconcile(ctx, plan.digest, op)
	require.ErrorIs(t, e, ErrNextcloudRecoveryDenied)
	withdrawnPresent = false
	state = "stopped"
	_, e = store.Reconcile(ctx, plan.digest, op)
	require.ErrorIs(t, e, ErrNextcloudRecoveryDenied)
	state = "active"
	epoch = 3
	_, e = store.Reconcile(ctx, plan.digest, op)
	require.ErrorIs(t, e, ErrNextcloudRecoveryDenied)
	epoch = 5
	evidence, e := store.Reconcile(ctx, plan.digest, op)
	require.NoError(t, e)
	require.Len(t, evidence.ReconciliationSHA256, 64)
	again, e := store.Reconcile(ctx, plan.digest, op)
	require.NoError(t, e)
	require.Equal(t, evidence, again)
	// The old immutable ID cannot be staged again after the KB has reopened.
	require.ErrorIs(t, r.StageNextcloudVersion(ctx, 7, "kb", "ds", "nextcloud:instance:77", "old-etag", old.ID), ErrNextcloudContentLeaseDenied)
	fresh := insertNextcloudVersionTestKnowledge(t, db, "fresh-generation", "", types.ParseStatusCompleted, "enabled")
	m := fresh.GetMetadata()
	m["external_id"] = "nextcloud:instance:78"
	m["nextcloud_file_id"] = "78"
	m["nextcloud_path"] = "current.md"
	m["nextcloud_publication_epoch"] = "5"
	fresh.Metadata, _ = json.Marshal(m)
	require.NoError(t, db.Save(fresh).Error)
	require.NoError(t, r.StageNextcloudVersion(ctx, 7, "kb", "ds", "nextcloud:instance:78", "new-etag", fresh.ID))
	leases := NewNextcloudContentLeaseStore(db)
	freshScope := NextcloudContentScope{TenantID: 7, KnowledgeBaseID: "kb", KnowledgeID: fresh.ID, DataSourceID: "ds", ExternalID: "nextcloud:instance:78"}
	_, e = leases.AcquireKnowledge(ctx, freshScope, NextcloudContentReadLease, "owner-reader", time.Minute)
	require.ErrorIs(t, e, ErrNextcloudContentLeaseDenied, "ordinary owner reads remain closed for new generations")
	_, e = leases.AcquireKBRead(ctx, 7, "kb", "owner-search", time.Minute)
	require.ErrorIs(t, e, ErrNextcloudContentLeaseDenied)
	build, e := leases.AcquireKnowledge(ctx, freshScope, NextcloudContentBuildLease, "maintenance-fresh", time.Minute)
	require.NoError(t, e)
	require.NoError(t, leases.ValidateBuildLeaseInTx(ctx, db, build, freshScope))
	buildCtx := WithNextcloudBuildLease(ctx, leases, build, freshScope)
	r.nextcloudPublicationCheck = nextcloud.CheckCurrentPublication
	ready, e := r.PublishNextcloudVersion(buildCtx, fresh.ID)
	require.NoError(t, e)
	require.True(t, ready)
	_, _, e = r.ResolveNextcloudSourceLineage(ctx, 7, "kb", fresh.ID)
	require.ErrorIs(t, e, ErrNextcloudLineageSnapshotDenied, "ordinary source readers remain closed even for a newly indexed candidate")
	snapshot, e := store.Snapshot(ctx, plan.digest, inv, pin)
	require.NoError(t, e)
	_, e = store.ReopenControls(ctx, plan.digest, inv, pin, plan, snapshot.ControlState["sha256"].(string), "", "")
	require.ErrorIs(t, e, ErrNextcloudRecoveryDenied, "new generations must cover full indexed manifest before control reopen")
	require.NoError(t, db.Create(&types.Chunk{ID: "fresh-indexed", TenantID: 7, KnowledgeBaseID: "kb", KnowledgeID: fresh.ID, Content: "fresh bytes", Status: int(types.ChunkStatusIndexed), IsEnabled: true, IndexStatus: "ready"}).Error)
	snapshot, e = store.Snapshot(ctx, plan.digest, inv, pin)
	require.NoError(t, e)
	_, e = store.ReopenControls(ctx, plan.digest, inv, pin, plan, strings.Repeat("0", 64), "", "")
	require.ErrorIs(t, e, ErrNextcloudRecoveryDenied, "stale operator inventory CAS cannot release controls")
	badTail := recoveryTestPlan(t, func(p *NextcloudRecoveryPlan) {
		p.ThroughDatabaseChainSHA256 = strings.Repeat("e", 64)
		p.DatabasePrefixHashes["7"] = p.ThroughDatabaseChainSHA256
	})
	_, e = store.ReopenControls(ctx, plan.digest, inv, pin, badTail, snapshot.ControlState["sha256"].(string), "", "")
	require.ErrorIs(t, e, ErrNextcloudRecoveryDenied, "saved signed tail is not current NC tail")
	done, e := store.ReopenControls(ctx, plan.digest, inv, pin, plan, snapshot.ControlState["sha256"].(string), "", "")
	require.NoError(t, e)
	require.Equal(t, true, done["controls_reconciled"])
	closed, e := nextcloudControlsClosed(db, "instance")
	require.NoError(t, e)
	require.False(t, closed)
	_, identity, e := r.ResolveNextcloudSourceLineage(ctx, 7, "kb", fresh.ID)
	require.NoError(t, e)
	require.Equal(t, fresh.ID, identity.KnowledgeID)
	require.ErrorIs(t, r.StageNextcloudVersion(ctx, 7, "kb", "ds", "nextcloud:instance:77", "old-etag", old.ID), ErrNextcloudContentLeaseDenied, "global control reopening never revives retired old body IDs")

	_, e = store.Apply(ctx, plan, inv, pin)
	require.NoError(t, e)
	_, _, e = r.ResolveNextcloudSourceLineage(ctx, 7, "kb", fresh.ID)
	require.NoError(t, e)
	_, _, e = r.ResolveNextcloudSourceLineage(ctx, 7, "kb", old.ID)
	require.ErrorIs(t, e, ErrNextcloudLineageSnapshotDenied)
}

func TestNextcloudRecoveryRejectsPinnedScopeAndPlanGaps(t *testing.T) {
	_, db, _, _ := recoveryFixture(t, false)
	store := NewNextcloudPublicationRecoveryStore(db)
	ctx := context.Background()
	inv, e := store.Capture(ctx, recoveryTestAnchor())
	require.NoError(t, e)
	pin, _ := recoveryJSONSHA(inv)
	p := recoveryTestPlan(t, nil)
	altered := *inv
	altered.Pairs = append([]NextcloudRecoveryPair{}, inv.Pairs...)
	altered.Pairs[0].TenantID = 8
	_, e = store.Apply(ctx, p, &altered, pin)
	require.ErrorIs(t, e, ErrNextcloudRecoveryDenied)
	require.NoError(t, db.Model(&NextcloudSourcePairing{}).Where("datasource_id='ds'").Update("nextcloud_instance_id", "cross-instance").Error)
	_, e = store.Apply(ctx, p, inv, pin)
	require.ErrorIs(t, e, ErrNextcloudRecoveryDenied)
	raw, e := json.Marshal(p)
	require.NoError(t, e)
	key := []byte("0123456789abcdef0123456789abcdef")
	for _, edit := range []func(map[string]any){func(v map[string]any) { delete(v["database_prefix_hashes"].(map[string]any), "6") }, func(v map[string]any) { v["through_sequence"] = "4" }, func(v map[string]any) { v["ingress_reopen_permitted"] = true }} {
		var v map[string]any
		require.NoError(t, json.Unmarshal(raw, &v))
		edit(v)
		b, e := json.Marshal(v)
		require.NoError(t, e)
		mac := hmac.New(sha256.New, key)
		mac.Write(b)
		envelope, _ := json.Marshal(map[string]string{"plan": string(b), "hmac_sha256": hex.EncodeToString(mac.Sum(nil))})
		_, e = VerifyNextcloudRecoveryPlan(envelope, key)
		require.ErrorIs(t, e, ErrNextcloudRecoveryDenied)
	}
}

func TestNextcloudRecoveryClosesUntouchedBindingsAndRejectsMissingScopes(t *testing.T) {
	r, db, _, _ := recoveryFixture(t, false)
	ctx := context.Background()
	require.NoError(t, db.Create(&types.KnowledgeBase{ID: "untouched-kb", TenantID: 9, Name: "other original pair"}).Error)
	cfg := &types.DataSourceConfig{Type: types.ConnectorTypeNextcloud, ResourceIDs: []string{"untouched-binding"}, Settings: map[string]any{"base_url": "https://nextcloud.example"}, Credentials: map[string]any{"token": "test-machine-token", "key_id": "pair_test"}}
	blob, e := cfg.ToJSON()
	require.NoError(t, e)
	ds := &types.DataSource{ID: "untouched-ds", TenantID: 9, KnowledgeBaseID: "untouched-kb", Type: types.ConnectorTypeNextcloud, Status: types.DataSourceStatusActive, Config: blob}
	require.NoError(t, db.Create(ds).Error)
	base, sha, binding, e := NextcloudEventDataSourceIdentity(blob)
	require.NoError(t, e)
	pair := NextcloudSourcePairing{OperationID: "45f2c992-5329-44c3-afee-c173cc9c897e", TenantID: 9, KnowledgeBaseID: ds.KnowledgeBaseID, DataSourceID: ds.ID, InstanceID: "instance", BindingID: binding, BaseURL: base, ConfigSHA: sha, PublicationEpoch: 3, KeyID: "pair_test", State: "active"}
	require.NoError(t, db.Create(&pair).Error)
	metadata, _ := json.Marshal(map[string]string{"datasource_id": ds.ID, "external_id": "nextcloud:instance:99", "nextcloud_instance_id": "instance", "nextcloud_binding_id": binding, "source_resource_id": binding, "nextcloud_file_id": "99", "nextcloud_path": "untouched.md", "nextcloud_etag": ""})
	untouched := &types.Knowledge{ID: "untouched-ready", TenantID: 9, KnowledgeBaseID: ds.KnowledgeBaseID, Channel: types.ConnectorTypeNextcloud, Type: "file", ParseStatus: types.ParseStatusCompleted, EnableStatus: "enabled", Metadata: metadata}
	require.NoError(t, db.Create(untouched).Error)
	require.NoError(t, r.StageNextcloudVersion(ctx, 9, ds.KnowledgeBaseID, ds.ID, "nextcloud:instance:99", "untouched-etag", untouched.ID))
	ready, e := r.PublishNextcloudVersion(ctx, untouched.ID)
	require.NoError(t, e)
	require.True(t, ready)
	store := NewNextcloudPublicationRecoveryStore(db)
	inv, e := store.Capture(ctx, recoveryTestAnchor())
	require.NoError(t, e)
	require.Len(t, inv.Pairs, 2)
	plan := recoveryTestPlan(t, nil)
	missing := *inv
	missing.Pairs = inv.Pairs[:1]
	missingPin, _ := recoveryJSONSHA(&missing)
	_, e = store.Apply(ctx, plan, &missing, missingPin)
	require.ErrorIs(t, e, ErrNextcloudRecoveryDenied)
	pin, _ := recoveryJSONSHA(inv)
	receipt, e := store.Apply(ctx, plan, inv, pin)
	require.NoError(t, e)
	require.Equal(t, 2, receipt.PairCount)
	require.Empty(t, reloadedNextcloudETag(t, db, untouched.ID))
	_, _, e = r.ResolveNextcloudSourceLineage(ctx, 9, ds.KnowledgeBaseID, untouched.ID)
	require.ErrorIs(t, e, ErrNextcloudLineageSnapshotDenied)
	require.Error(t, db.Exec("DELETE FROM nextcloud_publication_recovery_scopes WHERE plan_sha256=?", receipt.PlanSHA256).Error)
	require.Error(t, db.Model(&types.DataSource{}).Where("id=?", ds.ID).Update("knowledge_base_id", "cross-pair-kb").Error)
}

func TestNextcloudRecoveryRejectsDuplicateSignedFieldsAndRestoredHistoryGap(t *testing.T) {
	key := []byte("0123456789abcdef0123456789abcdef")
	plan := recoveryTestPlan(t, nil)
	for _, raw := range []string{strings.Replace(plan.raw, `"instance_id":"instance"`, `"instance_id":"instance","instance_id":"instance"`, 1), strings.Replace(plan.raw, `"ingress_reopen_permitted":false`, `"ingress_reopen_permitted":null`, 1), strings.Replace(plan.raw, `,"ingress_reopen_permitted":false`, "", 1)} {
		mac := hmac.New(sha256.New, key)
		mac.Write([]byte(raw))
		envelope, _ := json.Marshal(map[string]string{"plan": raw, "hmac_sha256": hex.EncodeToString(mac.Sum(nil))})
		_, e := VerifyNextcloudRecoveryPlan(envelope, key)
		require.ErrorIs(t, e, ErrNextcloudRecoveryDenied)
	}
	_, db, _, _ := recoveryFixture(t, false)
	ctx := context.Background()
	store := NewNextcloudPublicationRecoveryStore(db)
	inv, e := store.Capture(ctx, recoveryTestAnchor())
	require.NoError(t, e)
	pin, _ := recoveryJSONSHA(inv)
	// A partial restore can omit an immutable history row; reproduce that missing
	// restore input in this disposable fixture, rather than accepting an assert.
	require.NoError(t, db.Exec("DROP TRIGGER nextcloud_source_revision_immutable_delete").Error)
	require.NoError(t, db.Exec("DELETE FROM nextcloud_source_revisions WHERE external_id='nextcloud:instance:77' AND revision=1").Error)
	_, e = store.Apply(ctx, plan, inv, pin)
	require.ErrorIs(t, e, ErrNextcloudRecoveryDenied)
	var receipts int64
	require.NoError(t, db.Model(&NextcloudRecoveryReceipt{}).Count(&receipts).Error)
	require.Zero(t, receipts)
}

func TestNextcloudPublicationRecoveryNewGenerationRequiresLiveStableManifest(t *testing.T) {
	recoveryNewGenerationContract(t, false)
}
func TestPostgresNextcloudPublicationRecoveryNewGenerationRequiresLiveStableManifest(t *testing.T) {
	recoveryNewGenerationContract(t, true)
}

func TestNextcloudRecoveryRejectsReceiptWithPartiallyRestoredOpenGeneration(t *testing.T) {
	_, db, old, _ := recoveryFixture(t, false)
	ctx := context.Background()
	store := NewNextcloudPublicationRecoveryStore(db)
	inv, e := store.Capture(ctx, recoveryTestAnchor())
	require.NoError(t, e)
	pin, _ := recoveryJSONSHA(inv)
	plan := recoveryTestPlan(t, nil)
	_, e = store.Apply(ctx, plan, inv, pin)
	require.NoError(t, e)
	// Reproduce a partial restore retaining the receipt but replacing the exact
	// old fence table rows. The existing receipt must not certify that branch.
	require.NoError(t, db.Exec("UPDATE nextcloud_content_fences SET state='open',epoch=1,retired_at_ms=NULL WHERE knowledge_id=?", old.ID).Error)
	_, e = store.Apply(ctx, plan, inv, pin)
	require.ErrorIs(t, e, ErrNextcloudRecoveryDenied)
}

func TestRecoveryAuthenticatedJournalEvents(t *testing.T) {
	p := recoveryTestPlan(t, func(p *NextcloudRecoveryPlan) {
		file := "77"
		p.JournalEvents = []NextcloudRecoveryJournalEvent{{"6", "binding", &file, "withdrawn", "1", 1, nil, nil}, {"7", "binding", nil, "binding_stop", "1", 2, nil, nil}}
		chain := p.CheckpointDatabaseChainSHA256
		for _, event := range p.JournalEvents {
			fid := "-"
			if event.FileID != nil {
				fid = *event.FileID
			}
			chain = recoverySHA([]byte(strings.Join([]string{chain, event.Sequence, event.BindingID, fid, event.Kind, event.SourceRevision, fmt.Sprint(event.CreatedAt)}, "\n")))
			p.DatabasePrefixHashes[event.Sequence] = chain
		}
		p.ThroughDatabaseChainSHA256 = chain
	})
	for _, name := range []string{"changed-event", "missing-event", "reordered", "missing-required-field"} {
		t.Run(name, func(t *testing.T) {
			var fields map[string]any
			require.NoError(t, json.Unmarshal([]byte(p.raw), &fields))
			events := fields["journal_events"].([]any)
			switch name {
			case "changed-event":
				events[0].(map[string]any)["kind"] = "eligible"
			case "missing-event":
				fields["journal_events"] = events[:1]
			case "reordered":
				events[0], events[1] = events[1], events[0]
			case "missing-required-field":
				delete(fields, "withdraw_files")
			}
			raw, err := json.Marshal(fields)
			require.NoError(t, err)
			key := []byte("0123456789abcdef0123456789abcdef")
			mac := hmac.New(sha256.New, key)
			mac.Write(raw)
			signed, err := json.Marshal(map[string]string{"plan": string(raw), "hmac_sha256": hex.EncodeToString(mac.Sum(nil))})
			require.NoError(t, err)
			_, err = VerifyNextcloudRecoveryPlan(signed, key)
			require.Error(t, err)
		})
	}
}
