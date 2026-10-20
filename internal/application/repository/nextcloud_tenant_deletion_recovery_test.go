package repository

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
)

func tenantRecoveryPlan(t *testing.T) (NextcloudRecoveryPlan, NextcloudRecoveryTenantDeletion) {
	t.Helper()
	p, _ := targetRecoveryPlan(t)
	now := time.Now().Unix()
	deletion := (now - 10) * 1000
	proof := NextcloudRecoveryTenantDeletion{
		OriginSHA256: strings.Repeat("e", 64),
		Tenant: types.NextcloudTenantDeletionObservation{
			Schema:         "weknora-nextcloud-tenant-deletion-v1",
			State:          "tenant_deleted",
			OperationID:    "7c0253d2-43bd-4fdb-b05f-048d52c2e766",
			InstanceID:     "instance",
			BindingID:      "binding",
			TenantID:       "7",
			DeletionID:     tenantDeletionID(7, deletion),
			DeletionTimeMS: fmt.Sprint(deletion),
			ClosedAt:       fmt.Sprint(now - 9),
		},
	}
	event := NextcloudRecoveryJournalEvent{
		Sequence:       "6",
		BindingID:      "binding",
		Kind:           "tenant_deleted",
		SourceRevision: "0",
		CreatedAt:      now,
		TenantDeletion: &proof,
	}
	lines := []string{p.CheckpointDatabaseChainSHA256, "6", "binding", "-", "tenant_deleted", "0", fmt.Sprint(now)}
	chain := recoverySHA([]byte(strings.Join(append(lines, tenantDeletionJournalLines(proof)...), "\n")))
	p.JournalEvents = []NextcloudRecoveryJournalEvent{event}
	p.TargetClosures = nil
	p.TenantDeletions = []NextcloudRecoveryTenantDeletion{proof}
	p.CloseBindings = []NextcloudRecoveryBinding{}
	p.ThroughDatabaseChainSHA256 = chain
	p.DatabasePrefixHashes["6"] = chain
	return p, proof
}

func TestNextcloudTenantRecoveryIndependentHistoryAndHash(t *testing.T) {
	key := []byte(strings.Repeat("k", 32))
	p, proof := tenantRecoveryPlan(t)
	verified, err := VerifyNextcloudRecoveryPlan(targetRecoverySigned(t, p, key), key)
	require.NoError(t, err)
	require.Equal(t, proof, verified.TenantDeletions[0])
	require.Empty(t, verified.CloseBindings)
	for _, name := range []string{
		"omit-history",
		"changed-owner",
		"changed-origin",
		"decimal-alias",
		"wrong-kind",
		"source-revision",
		"duplicate-namespace",
	} {
		t.Run(name, func(t *testing.T) {
			candidate, _ := tenantRecoveryPlan(t)
			switch name {
			case "omit-history":
				candidate.TenantDeletions = nil
			case "changed-owner":
				candidate.TenantDeletions[0].Tenant.TenantID = "8"
			case "changed-origin":
				candidate.TenantDeletions[0].OriginSHA256 = strings.Repeat("f", 64)
			case "decimal-alias":
				candidate.TenantDeletions[0].Tenant.DeletionTimeMS = "01"
			case "wrong-kind":
				candidate.JournalEvents[0].Kind = "binding_stop"
			case "source-revision":
				candidate.JournalEvents[0].SourceRevision = "1"
			case "duplicate-namespace":
				candidate.TenantDeletions = append(candidate.TenantDeletions, candidate.TenantDeletions[0])
			}
			_, err := VerifyNextcloudRecoveryPlan(targetRecoverySigned(t, candidate, key), key)
			require.Error(t, err)
		})
	}
	p.JournalEvents = nil
	p.CheckpointSequence = "6"
	p.CheckpointDatabaseChainSHA256 = p.ThroughDatabaseChainSHA256
	p.DatabasePrefixHashes = map[string]string{"6": p.ThroughDatabaseChainSHA256}
	verified, err = VerifyNextcloudRecoveryPlan(targetRecoverySigned(t, p, key), key)
	require.NoError(t, err)
	require.Equal(t, proof, verified.TenantDeletions[0])
	p.TenantDeletions = make([]NextcloudRecoveryTenantDeletion, 10001)
	_, err = VerifyNextcloudRecoveryPlan(targetRecoverySigned(t, p, key), key)
	require.Error(t, err)
}

func TestNextcloudRecoveryUnknownPermanentFactPersistsClosedGlobalGate(t *testing.T) {
	for _, kind := range []string{"target_deleted", "tenant_deleted"} {
		t.Run(kind, func(t *testing.T) {
			_, db, _, _ := recoveryFixture(t, false)
			ctx := context.Background()
			require.NoError(t, db.AutoMigrate(&nextcloudControlRecoveryGate{}))
			store := NewNextcloudPublicationRecoveryStore(db)
			inv, err := store.Capture(ctx, recoveryTestAnchor())
			require.NoError(t, err)
			pin, err := recoveryJSONSHA(inv)
			require.NoError(t, err)
			var plan NextcloudRecoveryPlan
			if kind == "target_deleted" {
				plan, _ = targetRecoveryPlan(t)
			} else {
				plan, _ = tenantRecoveryPlan(t)
			}
			key := []byte(strings.Repeat("k", 32))
			verified, err := VerifyNextcloudRecoveryPlan(targetRecoverySigned(t, plan, key), key)
			require.NoError(t, err)
			receipt, err := store.Apply(ctx, verified, inv, pin)
			require.ErrorIs(t, err, ErrNextcloudRecoveryDenied)
			require.ErrorIs(t, err, ErrNextcloudRecoveryUnknownOperation)
			require.Nil(t, receipt)
			var gate nextcloudControlRecoveryGate
			require.NoError(t, db.Where("instance_id='instance'").Take(&gate).Error)
			require.True(t, gate.Closed)
			require.Equal(t, verified.digest, gate.PlanSHA256)
			var receipts int64
			require.NoError(t, db.Model(&NextcloudRecoveryReceipt{}).Count(&receipts).Error)
			require.Zero(t, receipts)
		})
	}
}
