package repository_test

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	repo "github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/datasource/connector/nextcloud"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func rootMigrationProof(op types.NextcloudOnboardingOperation, source string) nextcloud.RootMigration {
	return nextcloud.RootMigration{
		OperationID:        uuid.NewString(),
		PairOperationID:    op.OperationID,
		BindingID:          op.BindingID,
		InstanceID:         op.InstanceID,
		TenantID:           "7",
		KnowledgeBaseID:    op.KnowledgeBaseID,
		DataSourceID:       source,
		RootFileID:         "10",
		PhysicalRootKey:    strings.Repeat("a", 64),
		StorageIdentity:    strings.Repeat("b", 64),
		OldRootHash:        strings.Repeat("c", 64),
		NewRootHash:        strings.Repeat("d", 64),
		OldRootPath:        "/owner/files/Original",
		NewRootPath:        "/owner/files/Renamed",
		OldEpoch:           "1",
		PreparedEpoch:      "2",
		ResumeEpoch:        "3",
		JournalStreamID:    uuid.NewString(),
		JournalSequence:    "5",
		JournalChainSHA256: strings.Repeat("e", 64),
		State:              "observed",
	}
}

func TestNextcloudRootMigrationClosesLeaseIDsPreservesContentEpochAndRetries(t *testing.T) {
	// This lease-focused test uses the same SQLite dialect as the production
	// single writer path. Real PG two-writer admission is covered separately.
	t.Setenv("NEXTCLOUD_ONBOARDING_TEST_POSTGRES_DSN", "")
	db, store, c := onboardingDB(t)
	ctx := context.Background()
	kb := freshOnboardingKB("root-lease")
	createOnboardingTemplate(t, db, &kb)
	op, err := store.Reserve(ctx, c, onboardingRequest(c, uuid.NewString(), kb.ID))
	require.NoError(t, err)
	activateOnboarding(t, db, store, op)
	// Exact schema is installed by onboardingDB; never rebuild tables with immutable triggers.
	ddl, err := os.ReadFile("../../../migrations/sqlite/000046_nextcloud_content_leases.up.sql")
	require.NoError(t, err)
	if !db.Migrator().HasTable("nextcloud_content_fences") {
		require.NoError(t, db.Exec(string(ddl)).Error)
	}
	require.NoError(
		t,
		db.Exec(
			("CREATE TABLE nextcloud_graph_tasks(operation_id TEXT PRIMARY KEY,tenant_id" +
				" BIGINT,knowledge_base_id TEXT,state TEXT,error_code TEXT,completed_at BIG" +
				"INT)"),
		).Error,
	)
	require.NoError(
		t,
		db.Exec(
			"CREATE TABLE nextcloud_graph_records(operation_id TEXT PRIMARY KEY,body_json TEXT,generation BIGINT)",
		).Error,
	)
	require.NoError(
		t,
		db.Exec("INSERT INTO nextcloud_graph_records VALUES('persisted','immutable original graph receipt',1)").Error,
	)
	require.NoError(
		t,
		db.Exec(
			"INSERT INTO nextcloud_graph_tasks VALUES('persisted',7,?,'succeeded','',1),('late',7,?,'running','',NULL)",
			kb.ID,
			kb.ID,
		).Error,
	)
	var pair repo.NextcloudSourcePairing
	require.NoError(t, db.Where("operation_id=?", op.OperationID).Take(&pair).Error)
	leases := repo.NewNextcloudContentLeaseStore(db)
	scope := repo.NextcloudContentScope{
		TenantID:        7,
		KnowledgeBaseID: kb.ID,
		KnowledgeID:     "doc",
		DataSourceID:    pair.DataSourceID,
		ExternalID:      "nextcloud:" + c.InstanceID + ":11",
	}
	build, err := leases.AcquireKnowledge(ctx, scope, repo.NextcloudContentBuildLease, "old-worker", time.Minute)
	require.NoError(t, err)
	read, err := leases.AcquireKnowledge(ctx, scope, repo.NextcloudContentReadLease, "old-reader", time.Minute)
	require.NoError(t, err)
	proof := rootMigrationProof(op, pair.DataSourceID)
	prepared, err := store.PrepareRootMigration(ctx, op, proof)
	require.NoError(t, err)
	again, err := store.PrepareRootMigration(ctx, op, proof)
	require.NoError(t, err)
	require.Equal(t, prepared.OperationID, again.OperationID)
	_, err = leases.RenewLease(ctx, build.ID, time.Minute)
	require.Error(t, err)
	_, err = leases.RenewLease(ctx, read.ID, time.Minute)
	require.Error(t, err)
	require.Error(
		t,
		leases.ValidateBuildLeaseInTx(ctx, db, build, scope),
		"late SQL/provider write cannot revive its original lease token",
	)
	var before struct {
		Epoch int64
		State string
	}
	require.NoError(t, db.Table("nextcloud_content_fences").Where("knowledge_id='doc'").Take(&before).Error)
	require.Equal(t, build.Epoch, before.Epoch)
	require.Equal(t, "open", before.State)
	mapped, err := store.ResolveCurrentSourcePath(ctx, 7, kb.ID, pair.DataSourceID, "sub/file.txt")
	require.NoError(t, err)
	require.Equal(t, "sub/file.txt", mapped)
	allow, err := store.RootMigrationAuthorizesEpoch(ctx, 7, kb.ID, pair.DataSourceID, "sub/file.txt", 1, 3)
	require.NoError(t, err)
	require.False(t, allow, "unknown final ACK cannot authorize a new source publication epoch")
	bad := proof
	bad.KnowledgeBaseID = "wrong"
	require.ErrorIs(t, store.FinalizeRootMigration(ctx, op, prepared, bad), repo.ErrNextcloudOnboardingConflict)
	proof.State = "finalized"
	require.NoError(t, store.FinalizeRootMigration(ctx, op, prepared, proof))
	require.NoError(t, store.FinalizeRootMigration(ctx, op, prepared, proof))
	allow, err = store.RootMigrationAuthorizesEpoch(ctx, 7, kb.ID, pair.DataSourceID, "sub/file.txt", 1, 3)
	require.NoError(t, err)
	require.True(t, allow)
	mapped, err = store.ResolveCurrentSourcePath(ctx, 7, kb.ID, pair.DataSourceID, "/owner/files/Original/sub/file.txt")
	require.NoError(t, err)
	require.Equal(t, "/owner/files/Renamed/sub/file.txt", mapped)
	fresh, err := leases.AcquireKnowledge(ctx, scope, repo.NextcloudContentReadLease, "current-reader", time.Minute)
	require.NoError(t, err)
	require.Equal(t, build.Epoch, fresh.Epoch)
	require.NotEqual(t, read.ID, fresh.ID)
	var graph string
	require.NoError(
		t,
		db.Table("nextcloud_graph_records").Select("body_json").Where("operation_id='persisted'").Scan(&graph).Error,
	)
	require.Equal(t, "immutable original graph receipt", graph)
	var task string
	require.NoError(t, db.Table("nextcloud_graph_tasks").Select("state").Where("operation_id='late'").Scan(&task).Error)
	require.Equal(t, "cancelled", task)
	require.NoError(
		t,
		db.Table("nextcloud_graph_tasks").Select("state").Where("operation_id='persisted'").Scan(&task).Error,
	)
	require.Equal(t, "succeeded", task)
	enabled, err := store.SyncEnabled(ctx, op)
	require.NoError(t, err)
	require.True(t, enabled, "only unchanged operator sync revision may recover its prior flag")
}

func TestNextcloudTargetMigrationIsOneWayAndWaitsForCompleteManifest(t *testing.T) {
	t.Setenv("NEXTCLOUD_ONBOARDING_TEST_POSTGRES_DSN", "")
	db, store, c := onboardingDB(t)
	ctx := context.Background()
	// Exact schema is installed by onboardingDB; immutable history triggers stay installed.
	ops := []types.NextcloudOnboardingOperation{}
	for _, id := range []string{"old-target", "new-target"} {
		kb := freshOnboardingKB(id)
		createOnboardingTemplate(t, db, &kb)
		op, err := store.Reserve(ctx, c, onboardingRequest(c, uuid.NewString(), id))
		require.NoError(t, err)
		ops = append(ops, op)
	}
	activateOnboarding(t, db, store, ops[0])
	snapshot := types.NextcloudGrantSnapshot{
		PermissionVersion: 1,
		PublicationEpoch:  1,
		Complete:          true,
		Subjects: []types.NextcloudGrantSubject{
			{Kind: "user", DirectoryID: c.DirectoryID, ObjectGUID: "guid-reader", SourceRelationID: "share"},
		},
	}
	require.NoError(t, store.ApplyGrantSnapshot(ctx, ops[0], snapshot))
	migrationID := uuid.NewString()
	move, err := store.PrepareTargetMigration(ctx, ops[0], migrationID, ops[1].OperationID, c.DirectoryID, "guid-owner")
	require.NoError(t, err)
	require.Equal(t, "building", move.State)
	require.NotEqual(t, move.OldKnowledgeBaseID, move.NewKnowledgeBaseID)
	// Lost retirement ACK retries the same target and does not revoke another KB.
	again, err := store.PrepareTargetMigration(
		ctx,
		ops[0],
		migrationID,
		ops[1].OperationID,
		c.DirectoryID,
		"guid-owner",
	)
	require.NoError(t, err)
	require.Equal(t, move.OperationID, again.OperationID)
	old, err := store.Operation(ctx, c, ops[0].OperationID)
	require.NoError(t, err)
	require.Equal(t, "retired", old.State)
	var oldPair repo.NextcloudSourcePairing
	require.NoError(t, db.Where("operation_id=?", ops[0].OperationID).Take(&oldPair).Error)
	var ds types.DataSource
	require.NoError(t, db.Where("id=?", oldPair.DataSourceID).Take(&ds).Error)
	require.Equal(t, types.DataSourceStatusPaused, ds.Status)
	matches, err := store.ScopedMatches(ctx, "reader", 7, ops[0].KnowledgeBaseID, time.Now())
	require.NoError(t, err)
	require.Empty(t, matches)
	activateOnboarding(t, db, store, ops[1])
	require.NoError(t, store.ApplyGrantSnapshot(ctx, ops[1], snapshot))
	// A nonempty remote manifest cannot activate an empty relational build.
	require.NoError(
		t,
		db.Exec(
			("CREATE TABLE nextcloud_source_versions(tenant_id BIGINT,knowledge_base_id " +
				"TEXT,datasource_id TEXT,external_id TEXT,desired_etag TEXT,candidate_knowl" +
				"edge_id TEXT,state TEXT,updated_at DATETIME)"),
		).Error,
	)
	manifest := nextcloud.RecoveryManifest{
		Identity: nextcloud.PairingIdentity{
			InstanceID:       c.InstanceID,
			BindingID:        ops[1].BindingID,
			PublicationState: "active",
			PublicationEpoch: 1,
		},
		Generation: "complete-double-scan",
		Files:      []nextcloud.RecoveryManifestFile{{FileID: 11, ETag: "v1", Path: "file.txt"}},
	}
	_, err = store.FinalizeTargetMigration(ctx, ops[0], migrationID, manifest)
	require.ErrorIs(t, err, repo.ErrNextcloudOnboardingConflict)
	manifest.Files = nil
	done, err := store.FinalizeTargetMigration(ctx, ops[0], migrationID, manifest)
	require.NoError(t, err)
	require.Equal(t, "active", done.State)
	// Original KB and source identity survive retirement; their dedicated history
	// cannot be adopted as a new blank target after rows look empty.
	var retained types.KnowledgeBase
	require.NoError(t, db.Where("id=?", ops[0].KnowledgeBaseID).Take(&retained).Error)
	require.True(t, retained.EverHadNextcloudSource)
}
