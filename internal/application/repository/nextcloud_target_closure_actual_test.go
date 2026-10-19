package repository_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"strconv"
	"strings"
	"testing"
	"time"

	repo "github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func targetProofHash(target types.NextcloudTargetObservation) string {
	raw := strings.Join(
		[]string{
			"weknora-target-deleted-v1",
			target.OperationID,
			target.TenantID,
			target.KnowledgeBaseID,
			target.InstanceID,
			target.BindingID,
			target.OperationRevision,
			target.Reason,
			target.DeletionTimeMS,
		},
		"\n",
	)
	hash := sha256.Sum256([]byte(raw))
	return hex.EncodeToString(hash[:])
}

func TestNextcloudTargetDeletedTenantTransactionRetainsExitEvidence(t *testing.T) {
	db, store, c := onboardingDB(t)
	ctx := context.Background()
	require.NoError(t, db.Create(&types.Tenant{ID: 9, Name: "other"}).Error)
	require.NoError(
		t,
		db.Create(
			&types.TenantMember{
				TenantID: 9,
				UserID:   "outside",
				Role:     types.TenantRoleOwner,
				Status:   types.TenantMemberStatusActive,
			},
		).Error,
	)
	var operations []types.NextcloudOnboardingOperation
	for _, id := range []string{"delete-tenant-one", "delete-tenant-two"} {
		kb := freshOnboardingKB(id)
		createOnboardingTemplate(t, db, &kb)
		op, err := store.Reserve(ctx, c, onboardingRequest(c, uuid.NewString(), id))
		require.NoError(t, err)
		activateOnboarding(t, db, store, op)
		require.NoError(
			t,
			store.ApplyGrantSnapshot(
				ctx,
				op,
				types.NextcloudGrantSnapshot{
					PermissionVersion: 1,
					PublicationEpoch:  1,
					Complete:          true,
					Subjects: []types.NextcloudGrantSubject{
						{
							Kind:             "user",
							DirectoryID:      c.DirectoryID,
							ObjectGUID:       "guid-reader",
							SourceRelationID: "share",
						},
					},
				},
			),
		)
		operations = append(operations, op)
	}
	require.NoError(t, repo.NewTenantRepository(db).DeleteTenant(ctx, 7))
	for _, old := range operations {
		op, err := store.Operation(ctx, c, old.OperationID)
		require.NoError(t, err)
		target, err := store.TargetObservation(ctx, c, op, c.DirectoryID, "guid-owner")
		require.NoError(t, err)
		require.Equal(t, "target_deleted", target.State)
		require.Equal(t, "tenant_deleted", target.Reason)
		require.Equal(t, targetProofHash(target), target.ClosureID)
		revision, err := strconv.ParseUint(target.OperationRevision, 10, 64)
		require.NoError(t, err)
		require.Greater(t, revision, old.Revision)
		pair, ds, err := repo.NewNextcloudSourcePairingRepository(db).SourcePairing(ctx, 7, old.OperationID)
		require.NoError(t, err)
		require.Equal(t, "active", pair.State, "pair is retained, not a claimed withdrawal ACK")
		require.Equal(t, types.DataSourceStatusPaused, ds.Status)
		var enabled bool
		require.NoError(t, db.Table("data_sources").Select("sync_enabled").Where("id=?", ds.ID).Scan(&enabled).Error)
		require.False(t, enabled)
		require.NotEmpty(t, ds.Config, "cleanup credentials are preserved")
		matches, err := store.ScopedMatches(ctx, "reader", 7, old.KnowledgeBaseID, time.Now())
		require.NoError(t, err)
		require.Empty(t, matches)
		require.Error(t, store.MarkActive(ctx, op))
		require.Error(
			t,
			store.ApplyGrantSnapshot(
				ctx,
				op,
				types.NextcloudGrantSnapshot{PermissionVersion: 2, PublicationEpoch: 1, Complete: true},
			),
		)
		_, err = store.SetSyncEnabled(ctx, op, true, op.Revision)
		require.Error(t, err)
		_, _, err = repo.NewNextcloudSourcePairingRepository(db).PrepareSourcePairing(ctx, pair, "handoff-token")
		require.Error(t, err)
		_, err = repo.NewNextcloudContentLeaseStore(db).
			AcquireKBRead(ctx, 7, old.KnowledgeBaseID, "new-reader", time.Minute)
		require.Error(t, err)
		_, err = store.TargetObservation(ctx, c, op, c.DirectoryID, "guid-reader")
		require.ErrorIs(t, err, repo.ErrNextcloudOnboardingDenied)
	}
	var outside int64
	require.NoError(t, db.Model(&types.TenantMember{}).Where("tenant_id=9 AND user_id='outside'").Count(&outside).Error)
	require.EqualValues(t, 1, outside)
	// The server credential remains available only for authenticated safe closure
	// observation; its ordinary Actor and every scoped mutation are denied.
	_, err := store.Credential(ctx, "test-onboarding-server-credential-0123456789")
	require.NoError(t, err)
	_, err = store.Actor(ctx, c, c.DirectoryID, "guid-owner")
	require.Error(t, err)
	require.NoError(t, repo.NewTenantRepository(db).DeleteTenant(ctx, 7), "retry does not mint a new receipt")
	var rows int64
	require.NoError(t, db.Model(&repo.NextcloudTargetClosure{}).Count(&rows).Error)
	require.EqualValues(t, 2, rows)
	require.Error(t, db.Exec("UPDATE nextcloud_onboarding_target_closures SET reason='knowledge_base_deleted'").Error)
	require.Error(t, db.Exec("DELETE FROM nextcloud_onboarding_target_closures").Error)
}

func TestNextcloudTargetDeletedKBIsExactAndMissingIsNotDeletion(t *testing.T) {
	db, store, c := onboardingDB(t)
	ctx := context.Background()
	var ops []types.NextcloudOnboardingOperation
	for _, id := range []string{"delete-kb", "keep-kb"} {
		kb := freshOnboardingKB(id)
		createOnboardingTemplate(t, db, &kb)
		op, err := store.Reserve(ctx, c, onboardingRequest(c, uuid.NewString(), id))
		require.NoError(t, err)
		activateOnboarding(t, db, store, op)
		require.NoError(
			t,
			store.ApplyGrantSnapshot(
				ctx,
				op,
				types.NextcloudGrantSnapshot{
					PermissionVersion: 1,
					PublicationEpoch:  1,
					Complete:          true,
					Subjects: []types.NextcloudGrantSubject{
						{
							Kind:             "group",
							DirectoryID:      c.DirectoryID,
							ObjectGUID:       "guid-engineering",
							SourceRelationID: "same-group-share",
						},
					},
				},
			),
		)
		ops = append(ops, op)
	}
	// The existing service forbids generic deletion of a live source. This tests
	// the actual repository hook used after an authorized deletion transaction.
	require.NoError(t, repo.NewKnowledgeBaseRepository(db).DeleteKnowledgeBase(ctx, ops[0].KnowledgeBaseID))
	deleted, err := store.Operation(ctx, c, ops[0].OperationID)
	require.NoError(t, err)
	target, err := store.TargetObservation(ctx, c, deleted, c.DirectoryID, "guid-owner")
	require.NoError(t, err)
	require.Equal(t, "knowledge_base_deleted", target.Reason)
	require.NoError(t, store.RequireTargetOpen(ctx, ops[1]))
	matches, err := store.ScopedMatches(ctx, "reader", 7, ops[1].KnowledgeBaseID, time.Now())
	require.NoError(t, err)
	require.Len(t, matches, 1, "same directory group on another KB remains authorized")
	removed, err := store.ScopedMatches(ctx, "reader", 7, ops[0].KnowledgeBaseID, time.Now())
	require.NoError(t, err)
	require.Empty(t, removed)
	live, err := store.TargetObservation(ctx, c, ops[1], c.DirectoryID, "guid-owner")
	require.NoError(t, err)
	require.Equal(t, "present", live.State)
	require.Empty(t, live.ClosureID)
	require.NoError(t, db.Unscoped().Where("id=?", ops[1].KnowledgeBaseID).Delete(&types.KnowledgeBase{}).Error)
	unknown, err := store.TargetObservation(ctx, c, ops[1], c.DirectoryID, "guid-owner")
	require.Error(t, err)
	require.Equal(t, "unverified", unknown.State)
	require.Empty(t, unknown.ClosureID)
}

func TestNextcloudTargetDeleteUnknownLegacySourceRollsBack(t *testing.T) {
	db, _, _ := onboardingDB(t)
	ctx := context.Background()
	kb := freshOnboardingKB("legacy-source")
	createOnboardingTemplate(t, db, &kb)
	require.NoError(
		t,
		db.Create(
			&types.DataSource{
				ID:              "legacy",
				TenantID:        7,
				KnowledgeBaseID: kb.ID,
				Type:            types.ConnectorTypeNextcloud,
				Status:          types.DataSourceStatusPaused,
			},
		).Error,
	)
	require.Error(t, repo.NewTenantRepository(db).DeleteTenant(ctx, 7))
	var tenant types.Tenant
	require.NoError(t, db.Where("id=7").Take(&tenant).Error)
	require.False(t, tenant.DeletedAt.Valid)
	var members int64
	require.NoError(t, db.Model(&types.TenantMember{}).Where("tenant_id=7").Count(&members).Error)
	require.EqualValues(t, 1, members)
	var closed int64
	require.NoError(t, db.Model(&repo.NextcloudTargetClosure{}).Count(&closed).Error)
	require.Zero(t, closed)
}

func TestNextcloudTargetPositiveDeletedAtObservationRepairsPreHookFact(t *testing.T) {
	db, store, c := onboardingDB(t)
	ctx := context.Background()
	kb := freshOnboardingKB("positive-delete-observation")
	createOnboardingTemplate(t, db, &kb)
	op, err := store.Reserve(ctx, c, onboardingRequest(c, uuid.NewString(), kb.ID))
	require.NoError(t, err)
	activateOnboarding(t, db, store, op)
	require.NoError(t, db.Model(&types.Tenant{}).Where("id=7").Update("deleted_at", time.Now().UTC()).Error)
	_, err = store.TargetObservation(ctx, c, op, c.DirectoryID, "guid-reader")
	require.ErrorIs(t, err, repo.ErrNextcloudOnboardingDenied)
	var before int64
	require.NoError(t, db.Model(&repo.NextcloudTargetClosure{}).Count(&before).Error)
	require.Zero(t, before)
	target, err := store.TargetObservation(ctx, c, op, c.DirectoryID, "guid-owner")
	require.NoError(t, err)
	require.Equal(t, "target_deleted", target.State)
	require.Equal(t, "tenant_deleted", target.Reason)
	require.Equal(t, targetProofHash(target), target.ClosureID)
	current, err := store.Operation(ctx, c, op.OperationID)
	require.NoError(t, err)
	revision, err := strconv.ParseUint(target.OperationRevision, 10, 64)
	require.NoError(t, err)
	require.GreaterOrEqual(t, current.Revision, revision)
	again, err := store.TargetObservation(ctx, c, current, c.DirectoryID, "guid-owner")
	require.NoError(t, err)
	require.Equal(t, target, again)
}

func TestNextcloudTenantDeletedAfterKBClosureKeepsOriginalTargetReceipt(t *testing.T) {
	db, store, c := onboardingDB(t)
	ctx := context.Background()
	kb := freshOnboardingKB("kb-before-tenant")
	createOnboardingTemplate(t, db, &kb)
	op, err := store.Reserve(ctx, c, onboardingRequest(c, uuid.NewString(), kb.ID))
	require.NoError(t, err)
	activateOnboarding(t, db, store, op)
	require.NoError(t, repo.NewKnowledgeBaseRepository(db).DeleteKnowledgeBase(ctx, kb.ID))
	current, err := store.Operation(ctx, c, op.OperationID)
	require.NoError(t, err)
	original, err := store.TargetObservation(ctx, c, current, c.DirectoryID, "guid-owner")
	require.NoError(t, err)
	require.Equal(t, "knowledge_base_deleted", original.Reason)
	require.NoError(t, repo.NewTenantRepository(db).DeleteTenant(ctx, 7))
	current, err = store.Operation(ctx, c, op.OperationID)
	require.NoError(t, err)
	after, err := store.TargetObservation(ctx, c, current, c.DirectoryID, "guid-owner")
	require.NoError(t, err)
	require.Equal(t, original, after)
	owner, err := store.TenantDeletionObservation(ctx, c, current, c.DirectoryID, "guid-owner")
	require.NoError(t, err)
	require.NotNil(t, owner)
	require.Equal(t, "tenant_deleted", owner.State)
	require.Equal(t, "7", owner.TenantID)
	hash := sha256.Sum256([]byte("weknora-tenant-deleted-v1\n7\n" + owner.DeletionTimeMS))
	require.Equal(t, hex.EncodeToString(hash[:]), owner.DeletionID)
	require.Error(t, db.Unscoped().Model(&types.Tenant{}).Where("id=7").Update("deleted_at", nil).Error)
	fresh := freshOnboardingKB("new-kb-must-not-revive-owner")
	require.Error(t, db.Create(&fresh).Error)
	var tombstones int64
	require.NoError(t, db.Model(&repo.NextcloudTenantDeletion{}).Count(&tombstones).Error)
	require.EqualValues(t, 1, tombstones)
}

func TestNextcloudRetiredHistoricalOperationObservesOnlyDeletedOwner(t *testing.T) {
	db, store, c := onboardingDB(t)
	ctx := context.Background()
	kb := freshOnboardingKB("historical-retired")
	createOnboardingTemplate(t, db, &kb)
	op, err := store.Reserve(ctx, c, onboardingRequest(c, uuid.NewString(), kb.ID))
	require.NoError(t, err)
	require.NoError(
		t,
		db.Model(&types.NextcloudOnboardingOperation{}).
			Where("operation_id=?", op.OperationID).
			Update("state", "retired").
			Error,
	)
	require.NoError(t, repo.NewTenantRepository(db).DeleteTenant(ctx, 7))
	op, err = store.Operation(ctx, c, op.OperationID)
	require.NoError(t, err)
	owner, err := store.TenantDeletionObservation(ctx, c, op, c.DirectoryID, "guid-owner")
	require.NoError(t, err)
	require.NotNil(t, owner)
	target, err := store.TargetObservation(ctx, c, op, c.DirectoryID, "guid-owner")
	require.NoError(t, err)
	require.Equal(t, "unverified", target.State)
	require.Empty(t, target.ClosureID)
	var closures int64
	require.NoError(t, db.Model(&repo.NextcloudTargetClosure{}).Count(&closures).Error)
	require.Zero(t, closures)
	_, err = store.TenantDeletionObservation(ctx, c, op, c.DirectoryID, "guid-reader")
	require.ErrorIs(t, err, repo.ErrNextcloudOnboardingDenied)
}

// TestNextcloudTargetDeleteRetiresExistingFenceWithoutReadmission checks exact closure of a real open read fence.
func TestNextcloudTargetDeleteRetiresExistingFenceWithoutReadmission(t *testing.T) {
	for _, target := range []string{"knowledge_base", "tenant"} {
		t.Run(target, func(t *testing.T) {
			db, store, credential := onboardingDB(t)
			ctx := context.Background()
			kb := freshOnboardingKB("delete-held-fence")
			createOnboardingTemplate(t, db, &kb)
			operation, err := store.Reserve(ctx, credential, onboardingRequest(credential, uuid.NewString(), kb.ID))
			require.NoError(t, err)
			activateOnboarding(t, db, store, operation)
			leases := repo.NewNextcloudContentLeaseStore(db)
			read, err := leases.AcquireKBRead(ctx, 7, kb.ID, "held-before-target-delete", time.Minute)
			require.NoError(t, err)
			defer func() { require.NoError(t, leases.ReleaseLease(context.Background(), read.ID)) }()
			var before struct {
				Epoch int64
				State string
			}
			require.NoError(t, db.Table("nextcloud_content_fences").
				Where("tenant_id=? AND knowledge_base_id=? AND knowledge_id=''", 7, kb.ID).Take(&before).Error)
			require.Equal(t, "open", before.State)
			if target == "tenant" {
				require.NoError(t, repo.NewTenantRepository(db).DeleteTenant(ctx, 7))
			} else {
				require.NoError(t, repo.NewKnowledgeBaseRepository(db).DeleteKnowledgeBase(ctx, kb.ID))
			}
			after := before
			require.NoError(t, db.Table("nextcloud_content_fences").
				Where("tenant_id=? AND knowledge_base_id=? AND knowledge_id=''", 7, kb.ID).Take(&after).Error)
			require.Equal(t, "retired", after.State)
			require.Equal(t, before.Epoch+1, after.Epoch)
			_, err = leases.AcquireKBRead(ctx, 7, kb.ID, "new-reader-after-delete", time.Minute)
			require.Error(t, err)
			current, err := store.Operation(ctx, credential, operation.OperationID)
			require.NoError(t, err)
			_, err = store.TargetObservation(ctx, credential, current, credential.DirectoryID, "guid-owner")
			require.NoError(t, err)
			again := after
			require.NoError(t, db.Table("nextcloud_content_fences").
				Where("tenant_id=? AND knowledge_base_id=? AND knowledge_id=''", 7, kb.ID).Take(&again).Error)
			require.Equal(t, after, again, "closure retry cannot advance or reopen an already-closed fence")
			require.Error(t, db.Exec("UPDATE nextcloud_content_fences SET state='open',epoch=epoch+1 "+
				"WHERE tenant_id=? AND knowledge_base_id=? AND knowledge_id=''", 7, kb.ID).Error)
		})
	}
}
