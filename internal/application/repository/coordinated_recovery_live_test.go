package repository

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/datasource/connector/nextcloud"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
)

// The Python owner coordinates real NC and PostgreSQL dump/restore around this
// repository fixture. HTTP calls use the actual signed PHP endpoints.
func TestPostgresCoordinatedRecoveryLiveNextcloud(t *testing.T) {
	root := os.Getenv("WEKNORA_COORDINATED_RECOVERY_FIXTURE")
	if root == "" {
		t.Skip("owned live Nextcloud fixture required")
	}
	var f struct {
		Instance, Binding, Operation, KeyID, Token, KB, DS, Directory, AliceGUID, BobGUID string
		Epoch                                                                             int64
		Files                                                                             []struct {
			ID                   int64
			ETag, Path, Revision string
		}
		Anchor NextcloudRecoveryAnchor
	}
	read := func(name string, out any) {
		b, e := os.ReadFile(filepath.Join(root, name))
		require.NoError(t, e)
		require.NoError(t, json.Unmarshal(b, out))
	}
	read("fixture.json", &f)
	write := func(name string, v any) {
		b, e := json.Marshal(v)
		require.NoError(t, e)
		require.NoError(t, os.WriteFile(filepath.Join(root, name), b, 0o600))
	}
	wait := func(name string) {
		deadline := time.Now().Add(8 * time.Minute)
		for time.Now().Before(deadline) {
			if _, e := os.Stat(filepath.Join(root, name)); e == nil {
				return
			}
			time.Sleep(200 * time.Millisecond)
		}
		t.Fatalf("coordinator did not provide %s", name)
	}
	t.Setenv("SYSTEM_AES_KEY", "0123456789abcdef0123456789abcdef")
	t.Setenv("WEKNORA_NEXTCLOUD_DEV_HTTP", "1")
	db := leaseTestPostgres(t)
	require.NoError(
		t,
		db.AutoMigrate(
			&types.Knowledge{},
			&types.DataSource{},
			&NextcloudSourcePairing{},
			&types.KnowledgeBase{},
			&types.Chunk{},
			&nextcloudCandidateRetryJob{},
			&types.SyncLog{},
			&types.Directory{},
			&types.DirectoryIdentity{},
			&types.DirectoryGroup{},
			&types.DirectoryGroupMembership{},
		),
	)
	for _, name := range []string{"000112_nextcloud_source_versions", "000126_nextcloud_source_revisions", "000145_nextcloud_publication_recovery", "000158_nextcloud_content_revision"} {
		b, e := os.ReadFile("../../../migrations/versioned/" + name + ".up.sql")
		require.NoError(t, e)
		simplePGExec(t, db, string(b))
	}
	require.NoError(t, db.Create(&types.KnowledgeBase{ID: f.KB, TenantID: 7, Name: "owned-coordinated-recovery"}).Error)
	config, e := (&types.DataSourceConfig{Type: types.ConnectorTypeNextcloud, ResourceIDs: []string{f.Binding}, Settings: map[string]interface{}{"base_url": "http://nextcloud"}, Credentials: map[string]interface{}{"token": f.Token, "key_id": f.KeyID}}).ToJSON()
	require.NoError(t, e)
	ds := types.DataSource{
		ID:              f.DS,
		TenantID:        7,
		KnowledgeBaseID: f.KB,
		Type:            types.ConnectorTypeNextcloud,
		Status:          types.DataSourceStatusActive,
		Config:          config,
	}
	require.NoError(t, db.Create(&ds).Error)
	require.NoError(t, db.Where("id=?", ds.ID).Take(&ds).Error)
	base, hash, binding, e := NextcloudEventDataSourceIdentity(ds.Config)
	require.NoError(t, e)
	require.NoError(
		t,
		db.Create(
			&NextcloudSourcePairing{
				OperationID:      f.Operation,
				TenantID:         7,
				KnowledgeBaseID:  f.KB,
				DataSourceID:     f.DS,
				InstanceID:       f.Instance,
				BindingID:        binding,
				BaseURL:          base,
				ConfigSHA:        hash,
				PublicationEpoch: f.Epoch,
				KeyID:            f.KeyID,
				State:            "active",
			},
		).Error,
	)
	ctx := context.Background()
	repo := NewKnowledgeRepository(db).(*knowledgeRepository)
	makeKnowledge := func(id string, fid int64, etag, path, revision string) *types.Knowledge {
		metadata, e := json.Marshal(
			map[string]string{
				"datasource_id":               f.DS,
				"external_id":                 "nextcloud:" + f.Instance + ":" + strconv.FormatInt(fid, 10),
				"nextcloud_instance_id":       f.Instance,
				"nextcloud_binding_id":        f.Binding,
				"nextcloud_file_id":           strconv.FormatInt(fid, 10),
				"nextcloud_etag":              "",
				"source_resource_id":          f.Binding,
				"nextcloud_path":              path,
				"nextcloud_content_revision":  revision,
				"nextcloud_publication_epoch": strconv.FormatInt(f.Epoch, 10),
			},
		)
		require.NoError(t, e)
		k := &types.Knowledge{
			ID:              id,
			TenantID:        7,
			KnowledgeBaseID: f.KB,
			Channel:         types.ConnectorTypeNextcloud,
			Type:            "file",
			ParseStatus:     types.ParseStatusCompleted,
			EnableStatus:    "enabled",
			Metadata:        metadata,
		}
		require.NoError(t, db.Create(k).Error)
		require.NoError(
			t,
			repo.StageNextcloudVersion(
				ctx,
				7,
				f.KB,
				f.DS,
				"nextcloud:"+f.Instance+":"+strconv.FormatInt(fid, 10),
				etag,
				id,
			),
		)
		buildStore := NewNextcloudContentLeaseStore(db)
		buildScope := NextcloudContentScope{
			TenantID:        7,
			KnowledgeBaseID: f.KB,
			KnowledgeID:     id,
			DataSourceID:    f.DS,
			ExternalID:      "nextcloud:" + f.Instance + ":" + strconv.FormatInt(fid, 10),
		}
		lease, e := buildStore.AcquireKnowledge(
			ctx,
			buildScope,
			NextcloudContentBuildLease,
			"owned-importer",
			time.Minute,
		)
		require.NoError(t, e)
		buildCtx := WithNextcloudBuildLease(ctx, buildStore, lease, buildScope)
		ok, e := repo.PublishNextcloudVersion(buildCtx, id)
		require.NoError(t, e)
		require.True(t, ok)
		require.NoError(t, buildStore.ReleaseLease(ctx, lease.ID))
		require.NoError(
			t,
			db.Create(
				&types.Chunk{
					ID:              id + "-chunk",
					TenantID:        7,
					KnowledgeBaseID: f.KB,
					KnowledgeID:     id,
					Content:         "owned mock indexed bytes",
					IsEnabled:       true,
					Status:          int(types.ChunkStatusIndexed),
					IndexStatus:     "ready",
				},
			).Error,
		)
		require.NoError(t, db.Where("id=?", id).Take(k).Error)
		return k
	}
	old := makeKnowledge("old-withdrawn", f.Files[0].ID, f.Files[0].ETag, f.Files[0].Path, f.Files[0].Revision)
	oldLive := makeKnowledge("old-current", f.Files[1].ID, f.Files[1].ETag, f.Files[1].Path, f.Files[1].Revision)
	_, identity, e := repo.ResolveNextcloudSourceLineage(ctx, 7, f.KB, old.ID)
	require.NoError(t, e)
	lineage, e := types.NewCompleteSourceLineage(identity)
	require.NoError(t, e)
	require.NoError(
		t,
		db.Create(
			&types.SyncLog{
				ID:                 "old-worker",
				TenantID:           7,
				DataSourceID:       f.DS,
				Status:             types.SyncLogStatusRunning,
				WorkerActive:       true,
				WorkerAttemptToken: "old-worker",
				StartedAt:          time.Now().UTC(),
			},
		).Error,
	)
	// Actual DB directory lookup, with fixture snapshot rows linked to real LDAP
	// identities. NC performs a fresh LDAP objectGUID and user mount/ACL lookup.
	now := time.Now().UTC()
	require.NoError(
		t,
		db.Create(
			&types.Directory{
				ID:                   f.Directory,
				Name:                 "owned synthetic AD",
				Protocol:             types.DirectoryProtocolAD,
				Enabled:              true,
				SnapshotVersion:      1,
				LastSuccessfulSyncAt: &now,
				StaleAfterSeconds:    3600,
			},
		).Error,
	)
	for user, guid := range map[string]string{"alice": f.AliceGUID, "bob": f.BobGUID} {
		uid := user
		require.NoError(
			t,
			db.Create(
				&types.DirectoryIdentity{
					ID:              user + "-identity",
					DirectoryID:     f.Directory,
					ObjectGUID:      guid,
					UserID:          &uid,
					Status:          types.DirectoryObjectActive,
					SnapshotVersion: 1,
				},
			).Error,
		)
	}
	guard := access.NewNextcloudPublicationGuard(NewDirectoryRepository(db), recoveryGuardDS{db})
	actor := func(user string) context.Context {
		return types.WithPrincipal(
			types.WithCaller(ctx, types.Caller{TenantID: 7, UserID: user}),
			types.Principal{Type: types.PrincipalWebUser, ID: user},
		)
	}
	require.NoError(t, guard.CheckKnowledge(actor("alice"), oldLive))
	require.Error(t, guard.CheckKnowledge(actor("bob"), oldLive))
	require.Error(t, guard.CheckKnowledge(ctx, oldLive))
	private := t.TempDir()
	require.NoError(t, os.Chmod(private, 0o700))
	var schema string
	require.NoError(t, db.Raw("SELECT current_schema()").Scan(&schema).Error)
	dsnPath := filepath.Join(private, "dsn")
	require.NoError(
		t,
		os.WriteFile(dsnPath, []byte(os.Getenv("WEKNORA_LEASE_TEST_POSTGRES_DSN")+" search_path="+schema), 0o600),
	)
	cli := func(mode, out string, args ...string) error {
		command := exec.Command(
			os.Getenv("WEKNORA_RECOVERY_CLI"),
			append(
				[]string{
					"--mode=" + mode,
					"--postgres-connection-file=" + dsnPath,
					"--output=" + filepath.Join(private, out),
				},
				args...)...)
		output, e := command.CombinedOutput()
		if e != nil {
			t.Logf("maintenance %s refused (expected where asserted): %s", mode, output)
		}
		return e
	}
	_, e = NewNextcloudPublicationRecoveryStore(db).Capture(ctx, f.Anchor)
	require.NoError(t, e)
	require.NoError(
		t,
		cli(
			"capture",
			"inventory",
			"--instance-id="+f.Anchor.InstanceID,
			"--stream-id="+f.Anchor.StreamID,
			"--checkpoint-record-sha256="+f.Anchor.RecordSHA256,
			"--checkpoint-sequence="+f.Anchor.Sequence,
			"--checkpoint-database-chain-sha256="+f.Anchor.DatabaseChainSHA256,
		),
	)
	invRaw, e := os.ReadFile(filepath.Join(private, "inventory"))
	require.NoError(t, e)
	pin := recoverySHA(invRaw)
	require.NoError(t, os.WriteFile(filepath.Join(root, "inventory.json"), invRaw, 0o600))
	write("capture-ready.json", map[string]string{"schema": schema, "inventory_sha256": pin})
	wait("captured")
	require.NoError(
		t,
		repo.TombstoneNextcloudVersion(
			ctx,
			7,
			f.KB,
			f.DS,
			"nextcloud:"+f.Instance+":"+strconv.FormatInt(f.Files[0].ID, 10),
		),
	)
	write("tombstone-ready", map[string]bool{"tombstoned": true})
	wait("restore-ready")
	require.Equal(
		t,
		f.Files[0].ETag,
		reloadedNextcloudETag(t, db, old.ID),
		"actual pg_restore revived backup-era publication",
	)
	for _, name := range []string{"plan", "key"} {
		b, e := os.ReadFile(filepath.Join(root, name))
		require.NoError(t, e)
		require.NoError(t, os.WriteFile(filepath.Join(private, name), b, 0o600))
	}
	applyArgs := []string{
		"--plan=" + filepath.Join(private, "plan"),
		"--key-file=" + filepath.Join(private, "key"),
		"--inventory=" + filepath.Join(private, "inventory"),
		"--inventory-sha256=" + pin,
	}
	// Marker collision must refuse before DB closure; prove source still active.
	require.NoError(t, os.Mkdir(filepath.Join(private, "collision"), 0o700))
	require.Error(t, cli("apply", "collision", applyArgs...))
	var before types.DataSource
	require.NoError(t, db.Where("id=?", f.DS).Take(&before).Error)
	require.Equal(t, types.DataSourceStatusActive, before.Status)
	require.NoError(t, cli("apply", "closure", applyArgs...))
	require.NoError(t, cli("apply", "closure-again", applyArgs...))
	closure, e := os.ReadFile(filepath.Join(private, "closure"))
	require.NoError(t, e)
	again, e := os.ReadFile(filepath.Join(private, "closure-again"))
	require.NoError(t, e)
	require.Equal(t, closure, again)
	var receipt NextcloudRecoveryReceipt
	require.NoError(t, json.Unmarshal(closure, &receipt))
	require.NoError(t, os.WriteFile(filepath.Join(root, "weknora-closure.json"), closure, 0o600))
	for _, k := range []*types.Knowledge{old, oldLive} {
		require.NoError(t, db.Where("id=?", k.ID).Take(k).Error)
		require.Error(t, guard.CheckKnowledge(actor("alice"), k))
		_, _, e := repo.ResolveNextcloudSourceLineage(ctx, 7, f.KB, k.ID)
		require.ErrorIs(t, e, ErrNextcloudLineageSnapshotDenied)
	}
	matches, e := repo.RecheckNextcloudSourceLineage(ctx, lineage)
	require.NoError(t, e)
	require.False(t, matches)
	baseSnapshot := []string{
		"--plan-sha256=" + receipt.PlanSHA256,
		"--inventory=" + filepath.Join(private, "inventory"),
		"--inventory-sha256=" + pin,
	}
	require.NoError(t, cli("snapshot", "closed-state", baseSnapshot...))
	require.NoError(t, cli("snapshot", "closed-state-again", baseSnapshot...))
	closedAgain, e := os.ReadFile(filepath.Join(private, "closed-state-again"))
	require.NoError(t, e)
	closedFirst, e := os.ReadFile(filepath.Join(private, "closed-state"))
	require.NoError(t, e)
	require.Equal(t, closedFirst, closedAgain)
	raw, e := os.ReadFile(filepath.Join(private, "closed-state"))
	require.NoError(t, e)
	require.NoError(t, os.WriteFile(filepath.Join(root, "weknora-closed-state.json"), raw, 0o600))
	require.Error(
		t,
		cli("reconcile", "premature", "--plan-sha256="+receipt.PlanSHA256, "--pair-operation-id="+f.Operation),
	)
	// A receipt retained by a partial restore cannot conceal an opened old fence.
	var oldFence nextcloudContentFence
	require.NoError(
		t,
		db.Where("tenant_id=? AND knowledge_base_id=? AND knowledge_id=?", 7, f.KB, old.ID).Take(&oldFence).Error,
	)
	require.NoError(
		t,
		db.Exec(
			("UPDATE nextcloud_content_fences SET state='open' WHERE tenant_id=? AND kno"+
				"wledge_base_id=? AND knowledge_id=?"),
			7,
			f.KB,
			old.ID,
		).Error,
	)
	require.Error(t, cli("snapshot", "partial-state", baseSnapshot...))
	require.Error(t, cli("apply", "partial-retry", applyArgs...))
	require.NoError(
		t,
		db.Exec(
			"UPDATE nextcloud_content_fences SET state=? WHERE tenant_id=? AND knowledge_base_id=? AND knowledge_id=?",
			oldFence.State,
			7,
			f.KB,
			old.ID,
		).Error,
	)
	write("closed-ready", map[string]string{"plan_sha256": receipt.PlanSHA256})
	wait("resumed")
	reconcileArgs := []string{"--plan-sha256=" + receipt.PlanSHA256, "--pair-operation-id=" + f.Operation}
	require.NoError(t, cli("reconcile", "reconciled", reconcileArgs...))
	require.NoError(t, cli("reconcile", "reconciled-again", reconcileArgs...))
	var current struct {
		ID                   int64
		ETag, Path, Revision string
	}
	read("current-file.json", &current)
	newer := makeKnowledge("new-generation", current.ID, current.ETag, current.Path, current.Revision)
	require.NoError(t, guard.CheckKnowledge(actor("alice"), newer))
	require.Error(t, guard.CheckKnowledge(actor("bob"), newer))
	require.Error(t, guard.CheckKnowledge(ctx, newer))
	require.Error(
		t,
		repo.StageNextcloudVersion(
			ctx,
			7,
			f.KB,
			f.DS,
			"nextcloud:"+f.Instance+":"+strconv.FormatInt(current.ID, 10),
			current.ETag,
			oldLive.ID,
		),
	)
	require.NoError(t, cli("snapshot", "final-state", baseSnapshot...))
	require.NoError(t, cli("snapshot", "final-state-again", baseSnapshot...))
	finalAgain, e := os.ReadFile(filepath.Join(private, "final-state-again"))
	require.NoError(t, e)
	finalFirst, e := os.ReadFile(filepath.Join(private, "final-state"))
	require.NoError(t, e)
	require.Equal(t, finalFirst, finalAgain)
	raw, e = os.ReadFile(filepath.Join(private, "final-state"))
	require.NoError(t, e)
	require.NoError(t, os.WriteFile(filepath.Join(root, "weknora-final-state.json"), raw, 0o600))
	write(
		"final-ready",
		map[string]bool{
			"new_generation_published":     true,
			"history_old_lineage_denied":   true,
			"machine_without_actor_denied": true,
			"live_personal_acl":            true,
		},
	)
	wait("etag-changed")
	require.Error(
		t,
		guard.CheckKnowledge(actor("alice"), newer),
		"second live source ETag invalidates indexed generation",
	)
	require.Error(t, cli("snapshot", "changed-current", baseSnapshot...))
	var version struct {
		Revision string `json:"revision"`
	}
	read("current-revision.json", &version)
	configured, e := ds.ParseConfig()
	require.NoError(t, e)
	configured.Type = types.ConnectorTypeNextcloud
	aclContext := types.WithNextcloudContentRevision(ctx, version.Revision)
	decision, e := nextcloud.AuthorizeCurrentFile(
		aclContext,
		configured,
		f.Instance,
		f.Binding,
		f.Directory,
		f.AliceGUID,
		current.ID,
	)
	require.NoError(t, e)
	require.True(t, decision.Allow, "current source revision is readable before actual share revocation")
	write("etag-denied", map[string]bool{"second_etag_denied": true})
	wait("acl-revoked")
	// Independently probe the current source revision after actual ACL loss.
	require.Error(
		t,
		guard.CheckKnowledge(actor("alice"), newer),
		"current NC nested-department share revocation must deny",
	)
	decision, e = nextcloud.AuthorizeCurrentFile(
		aclContext,
		configured,
		f.Instance,
		f.Binding,
		f.Directory,
		f.AliceGUID,
		current.ID,
	)
	require.NoError(t, e)
	require.False(t, decision.Allow)
	require.Equal(
		t,
		"current_directory_membership_denied",
		decision.Reason,
		"revoked department share must fail the live audience gate independently of source revision",
	)
	write(
		"complete.json",
		map[string]bool{
			"actual_cli_capture_apply_snapshot_reconcile": true,
			"database_rollback_verified":                  true,
			"old_ids_retired":                             true,
			"new_id_positive":                             true,
			"source_etag_negative":                        true,
			"current_acl_negative":                        true,
			"no_actor_negative":                           true,
		},
	)
}
