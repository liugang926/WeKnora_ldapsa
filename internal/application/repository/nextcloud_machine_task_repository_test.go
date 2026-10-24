//go:build sqlite_fts5

package repository

import (
	"context"
	"encoding/json"
	"strconv"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/datasource/connector/nextcloud"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// This adapter uses the real persisted source snapshot, SQL build lease,
// configuration/body reader and signed connector publication check. The
// fixture has one original source and independent configuration ancestry.
type machineTaskRepositorySources struct {
	machineFetchConfigurationBeginner
}

func (v *machineTaskRepositorySources) CheckKnowledgeOriginalInputSources(
	ctx context.Context, proof *types.KnowledgeBuildInputProof,
) error {
	if proof == nil || proof.SourceLineage.RequireComplete() != nil || len(proof.SourceLineage.Sources) != 1 {
		return types.ErrSourceLineageUnknown
	}
	k, err := v.store.ReadOriginalNextcloudBuildSource(ctx, proof)
	if err != nil {
		return err
	}
	source := proof.SourceLineage.Sources[0]
	if source.TenantID != k.TenantID || source.KnowledgeBaseID != k.KnowledgeBaseID || source.KnowledgeID != k.ID {
		return ErrKnowledgeInputChanged
	}
	var ds types.DataSource
	if err := v.store.db.Where("id = ? AND tenant_id = ? AND knowledge_base_id = ? AND status = ?",
		source.DataSourceID, k.TenantID, k.KnowledgeBaseID, types.DataSourceStatusActive).Take(&ds).Error; err != nil {
		return err
	}
	cfg, err := ds.ParseConfig()
	if err != nil {
		return err
	}
	fileID, err := strconv.ParseInt(source.FileID, 10, 64)
	if err != nil {
		return err
	}
	if err := nextcloud.CheckCurrentPublication(ctx, cfg, source.InstanceID, source.BindingID,
		fileID, source.ETag, k.GetMetadata()["nextcloud_path"]); err != nil {
		return err
	}
	remaining, err := types.NewCompleteSourceLineage()
	if err != nil {
		return err
	}
	handled, err := CheckScopedOnboardingConfigurationSources(ctx, k.TenantID, k.KnowledgeBaseID, remaining, true)
	if !handled && err == nil {
		return types.ErrSourceLineageUnknown
	}
	return err
}

func machineTaskRepositoryFixture(t *testing.T) (
	*onboardingOriginalActualFixture, context.Context, []byte, string,
) {
	t.Helper()
	return machineTaskRepositoryFixtureWithConfiguration(t, nil)
}

func machineTaskRepositoryFixtureWithConfiguration(t *testing.T,
	configure func(*onboardingOriginalActualFixture),
) (*onboardingOriginalActualFixture, context.Context, []byte, string) {
	t.Helper()
	f := newOnboardingOriginalActualFixture(t)
	if configure != nil {
		configure(f)
	}
	op, err := f.onboarding.Reserve(context.Background(), f.credential, f.request("Actual machine task receipt"))
	require.NoError(t, err)
	_, ds := f.pair(t, op)
	ConfigureKnowledgeOriginalInputSourceVerifier(f.store,
		&machineTaskRepositorySources{machineFetchConfigurationBeginner{store: f.store}})
	build, release := f.configurationBuildContext(t, op, ds)
	t.Cleanup(release)
	build, closeRead, err := f.store.BeginNextcloudBuildConfigurationRead(build)
	require.NoError(t, err)
	t.Cleanup(closeRead)
	proof, err := f.store.AdmitNextcloudBuildInputs(build, []byte("Actual immutable source artifact for machine work"))
	require.NoError(t, err)
	build = WithKnowledgeBuildInputProof(build, f.store, proof)
	require.NoError(t, f.store.RecheckKnowledgeBuildInputs(build, proof))
	raw, err := json.Marshal(types.KnowledgePostProcessPayload{
		TenantID: proof.TenantID, KnowledgeBaseID: proof.KnowledgeBaseID,
		KnowledgeID: proof.KnowledgeID, OriginalInput: proof, Attempt: 3, Language: "zh-CN",
	})
	require.NoError(t, err)
	raw, taskID, err := PrepareNextcloudMachineTask(build, types.TypeKnowledgePostProcess, raw, "")
	require.NoError(t, err)
	require.NotEmpty(t, taskID)
	var pair NextcloudSourcePairing
	require.NoError(t, f.db.Where("datasource_id = ? AND tenant_id = ?", ds.ID, proof.TenantID).
		Take(&pair).Error)
	var receipt nextcloudMachineTask
	require.NoError(t, f.db.Where("task_id = ?", taskID).Take(&receipt).Error)
	require.Equal(t, pair.PublicationEpoch+3, receipt.PairPublicationEpoch,
		"machine work uses the observed source generation rather than the initial pairing receipt")
	return f, build, raw, taskID
}

func machineTaskRepositoryWrite(ctx context.Context, t *testing.T, f *onboardingOriginalActualFixture) error {
	t.Helper()
	_, scope, err := NextcloudBuildInputLease(ctx)
	if err != nil {
		return err
	}
	return f.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		return ValidateNextcloudBuildWrite(ctx, tx, scope.TenantID, scope.KnowledgeBaseID, scope.KnowledgeID)
	})
}

func machineTaskRepositoryNextWorker(old context.Context, t *testing.T, f *onboardingOriginalActualFixture,
) context.Context {
	t.Helper()
	_, scope, err := NextcloudBuildInputLease(old)
	require.NoError(t, err)
	leases := NewNextcloudContentLeaseStore(f.db)
	lease, err := leases.AcquireKnowledge(context.Background(), scope, NextcloudContentBuildLease,
		"actual-retry-worker", time.Minute)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, leases.ReleaseLease(context.Background(), lease.ID)) })
	ctx := WithNextcloudBuildLease(context.Background(), leases, lease, scope)
	ctx, closeRead, err := f.store.BeginNextcloudBuildConfigurationRead(ctx)
	require.NoError(t, err)
	t.Cleanup(closeRead)
	return WithKnowledgeBuildInputProof(ctx, f.store, KnowledgeBuildInputProofFromContext(old))
}

func TestNextcloudMachineRepositoryReceiptIsImmutableAndSingleUse(t *testing.T) {
	f, build, raw, taskID := machineTaskRepositoryFixture(t)
	payload, _, err := machineTaskPayload(types.TypeKnowledgePostProcess, raw)
	require.NoError(t, err)
	require.Error(t, f.db.Model(&nextcloudMachineTask{}).Where("receipt_id = ?", payload.Receipt.ID).
		Update("attempt", 4).Error)
	require.Error(t, f.db.Where("receipt_id = ?", payload.Receipt.ID).Delete(&nextcloudMachineTask{}).Error)
	current, finish, err := f.store.BeginNextcloudMachineTask(build, types.TypeKnowledgePostProcess, raw, taskID)
	require.NoError(t, err)
	require.NoError(t, machineTaskRepositoryWrite(current, t, f))
	require.NoError(t, finish(true, false))
	require.Error(t, machineTaskRepositoryWrite(current, t, f), "completed work cannot keep writing")
	_, _, err = f.store.BeginNextcloudMachineTask(build, types.TypeKnowledgePostProcess, raw, taskID)
	require.Error(t, err, "a completed receipt cannot replay")
	require.Error(t, finish(true, false), "completion CAS is single use")
}

func TestNextcloudMachineRepositoryRejectsPayloadAndAuthorityChanges(t *testing.T) {
	f, build, raw, taskID := machineTaskRepositoryFixture(t)
	for _, mutation := range []string{
		"language",
		"attempt",
		"tenant_id",
		"knowledge_base_id",
		"knowledge_id",
		"initiator",
	} {
		t.Run(mutation, func(t *testing.T) {
			var fields map[string]any
			require.NoError(t, json.Unmarshal(raw, &fields))
			switch mutation {
			case "attempt":
				fields[mutation] = 4
			case "tenant_id":
				fields[mutation] = 8
			case "initiator":
				fields[mutation] = types.TaskInitiator{UserID: "forged-user"}
			default:
				fields[mutation] = "changed-value"
			}
			changed, err := json.Marshal(fields)
			require.NoError(t, err)
			_, _, err = f.store.BeginNextcloudMachineTask(build, types.TypeKnowledgePostProcess, changed, taskID)
			require.Error(t, err)
		})
	}
	_, _, err := f.store.BeginNextcloudMachineTask(build, types.TypeKnowledgePostProcess, raw, "different-task")
	require.Error(t, err)
	_, _, err = f.store.BeginNextcloudMachineTask(build, types.TypeSummaryGeneration, raw, taskID)
	require.Error(t, err)
	f.managerAllow.Store(false)
	_, _, err = f.store.BeginNextcloudMachineTask(build, types.TypeKnowledgePostProcess, raw, taskID)
	require.Error(t, err, "held configuration bytes cannot preserve revoked manager permission")
	var claim nextcloudMachineTaskClaim
	require.NoError(t, f.db.Take(&claim).Error)
	require.Equal(t, "ready", claim.State, "failed admission cannot consume a receipt")
}

func TestNextcloudMachineRepositoryRechecksManagerAfterBegin(t *testing.T) {
	f, build, raw, taskID := machineTaskRepositoryFixture(t)
	current, finish, err := f.store.BeginNextcloudMachineTask(build, types.TypeKnowledgePostProcess, raw, taskID)
	require.NoError(t, err)
	require.NoError(t, machineTaskRepositoryWrite(current, t, f))
	require.NoError(t, CheckNextcloudMachineTaskExecution(current))

	f.managerAllow.Store(false)
	require.Error(t, machineTaskRepositoryWrite(current, t, f),
		"a running claim and held original bytes cannot preserve revoked manager authority")
	require.Error(t, CheckNextcloudMachineTaskExecution(current),
		"the model/provider checkpoint must recheck manager authority after admission")
	var claim nextcloudMachineTaskClaim
	require.NoError(t, f.db.Take(&claim).Error)
	require.Equal(t, "running", claim.State, "an authority refusal cannot silently consume or reset the claim")

	f.managerAllow.Store(true)
	require.NoError(t, finish(false, false))
}

func TestNextcloudMachineRepositoryRetryFencesPreviousClaim(t *testing.T) {
	f, build, raw, taskID := machineTaskRepositoryFixture(t)
	old, finish, err := f.store.BeginNextcloudMachineTask(build, types.TypeKnowledgePostProcess, raw, taskID)
	require.NoError(t, err)
	next := machineTaskRepositoryNextWorker(build, t, f)
	_, _, err = f.store.BeginNextcloudMachineTask(next, types.TypeKnowledgePostProcess, raw, taskID)
	require.Error(t, err, "another worker cannot take a running claim with a live durable lease")
	require.NoError(t, finish(false, true))
	require.Error(t, machineTaskRepositoryWrite(old, t, f), "a retryable completion fences its old worker")
	current, finishCurrent, err := f.store.BeginNextcloudMachineTask(next, types.TypeKnowledgePostProcess, raw, taskID)
	require.NoError(t, err)
	require.NoError(t, machineTaskRepositoryWrite(current, t, f))
	require.Error(t, machineTaskRepositoryWrite(old, t, f), "the old live lease cannot reuse a different claim")
	require.NoError(t, finishCurrent(false, false))
	_, _, err = f.store.BeginNextcloudMachineTask(next, types.TypeKnowledgePostProcess, raw, taskID)
	require.Error(t, err, "terminal failure is not a retryable receipt")
}

func TestNextcloudMachineRepositoryMarksPermanentDocumentFailure(t *testing.T) {
	f, build, _, _ := machineTaskRepositoryFixture(t)
	proof := KnowledgeBuildInputProofFromContext(build)
	require.NoError(t, markNextcloudDocumentTerminalFailure(
		context.Background(), f.db, proof.TenantID, proof.KnowledgeBaseID, proof.KnowledgeID,
	))
	var knowledge types.Knowledge
	require.NoError(t, f.db.Where("id = ?", proof.KnowledgeID).Take(&knowledge).Error)
	require.Equal(t, types.ParseStatusFailed, knowledge.ParseStatus,
		"a permanent machine-task failure cannot leave the document in processing")
	require.Equal(t, "document_processing_failed", knowledge.ErrorMessage)
}

func TestNextcloudMachineRepositoryExpiredLeaseTakeoverUsesDatabaseClock(t *testing.T) {
	f, build, raw, taskID := machineTaskRepositoryFixture(t)
	old, finish, err := f.store.BeginNextcloudMachineTask(build, types.TypeKnowledgePostProcess, raw, taskID)
	require.NoError(t, err)
	next := machineTaskRepositoryNextWorker(build, t, f)
	lease, _, err := NextcloudBuildInputLease(build)
	require.NoError(t, err)
	now, err := nextcloudLeaseNowMS(f.db)
	require.NoError(t, err)
	// Advance only the actual lease's expiry past the SQL clock. The receipt,
	// proof, claim, original source and every authorization guard stay intact.
	require.NoError(t, f.db.Model(&nextcloudContentLeaseRow{}).Where("lease_id = ?", lease.ID).
		Update("expires_at_ms", now-1).Error)
	current, finishCurrent, err := f.store.BeginNextcloudMachineTask(next, types.TypeKnowledgePostProcess, raw, taskID)
	require.NoError(t, err)
	require.NoError(t, machineTaskRepositoryWrite(current, t, f))
	require.Error(t, machineTaskRepositoryWrite(old, t, f))
	require.Error(t, finish(true, false), "a previous claim cannot finish its successor")
	require.NoError(t, finishCurrent(true, false))
}

func TestNextcloudMachineRepositoryRecoveryInventoryIncludesClaims(t *testing.T) {
	f, build, raw, taskID := machineTaskRepositoryFixture(t)
	before, err := nextcloudControlMetadataInventory(f.db)
	require.NoError(t, err)
	require.Contains(t, before, "nextcloud_machine_tasks")
	require.Contains(t, before, "nextcloud_machine_task_claims")
	require.Empty(t, nextcloudMissingControlTables(f.db))
	_, finish, err := f.store.BeginNextcloudMachineTask(build, types.TypeKnowledgePostProcess, raw, taskID)
	require.NoError(t, err)
	require.NoError(t, finish(true, false))
	after, err := nextcloudControlMetadataInventory(f.db)
	require.NoError(t, err)
	require.Equal(t, before["nextcloud_machine_tasks"], after["nextcloud_machine_tasks"])
	require.NotEqual(t, before["nextcloud_machine_task_claims"], after["nextcloud_machine_task_claims"],
		"recovery must bind terminal consumption and reject an older ready claim")
	require.NoError(t, f.db.Exec("DROP TABLE nextcloud_machine_task_claims").Error)
	require.Contains(t, nextcloudMissingControlTables(f.db), "nextcloud_machine_task_claims",
		"a partially restored current schema cannot disguise missing anti-replay state")
}
