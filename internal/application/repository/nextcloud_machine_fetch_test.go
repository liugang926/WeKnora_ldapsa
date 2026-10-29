//go:build sqlite_fts5

package repository

import (
	"context"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
)

// Exercise the production configuration-reader ports with their real body
// plans and SQL leases. No actor, manager result or private purpose is forged.
type machineFetchConfigurationBeginner struct {
	store *KnowledgeInputProvenanceStore
}

func (b *machineFetchConfigurationBeginner) CheckKnowledgeOriginalInputSources(
	context.Context,
	*types.KnowledgeBuildInputProof,
) error {
	return types.ErrSourceLineageUnknown
}

func (b *machineFetchConfigurationBeginner) BeginKnowledgeConfigurationSources(
	ctx context.Context,
	tenant uint64,
	kb string,
	lineage *types.SourceLineage,
) (context.Context, func(), error) {
	noop := func() {}
	var read ScopedOnboardingConfigurationRead
	var err error
	if len(lineage.Sources) == 0 {
		read, err = b.store.BeginScopedIndependentConfigurationRead(ctx, tenant, kb, lineage)
	} else {
		read, err = b.store.NextcloudOnboardingConfigurationStore().
			BeginScopedOnboardingConfigurationRead(ctx, tenant, kb, lineage)
	}
	if err != nil {
		return ctx, noop, err
	}
	if err := HoldScopedOnboardingConfigurationRead(ctx, tenant, kb, lineage, read); err != nil {
		_ = read.Close()
		return ctx, noop, err
	}
	return read.Context(ctx), func() { CloseScopedOnboardingConfigurationReads(ctx) }, nil
}

func TestNextcloudAutomaticSyncManagerPurposeRequiresActualFetchScope(t *testing.T) {
	f := newOnboardingOriginalActualFixture(t)
	op, err := f.onboarding.Reserve(context.Background(), f.credential, f.request("Automatic manager scope"))
	require.NoError(t, err)
	_, ds := f.pair(t, op)
	ConfigureKnowledgeOriginalInputSourceVerifier(f.store, &machineFetchConfigurationBeginner{store: f.store})
	worker := types.WithTaskAuthorization(context.Background(), 7, types.TaskInitiator{})
	require.Error(
		t,
		f.store.CheckNextcloudAutomaticSyncManagerPurpose(worker, 7, op.KnowledgeBaseID, ds.ID),
		"source IDs and an empty task are not a private fetch scope",
	)
	owned, closeRead, err := f.store.BeginNextcloudOriginalFetchConfigurationRead(worker, ds)
	require.NoError(t, err)
	defer closeRead()
	// The template is independently configured. Its real body read alone is
	// insufficient: a signed current manager decision is separately required.
	slot := owned.Value(scopedOnboardingConfigurationReadKey{}).(*scopedOnboardingConfigurationReadSlot)
	slot.mu.Lock()
	_, independent := slot.read.(*NextcloudIndependentConfigurationRead)
	slot.mu.Unlock()
	require.True(t, independent)
	before := f.managerCalls.Load()
	require.NoError(t, f.store.CheckNextcloudAutomaticSyncManagerPurpose(owned, 7, op.KnowledgeBaseID, ds.ID))
	require.Greater(t, f.managerCalls.Load(), before)
	require.Equal(t, types.CallerFromContext(worker), types.CallerFromContext(owned))
	principal, present := types.PrincipalFromContext(worker)
	actual, actualPresent := types.PrincipalFromContext(owned)
	require.Equal(t, present, actualPresent)
	require.Equal(t, principal, actual, "manager authorization never supplies a browser principal")
	require.Error(t, f.store.CheckNextcloudAutomaticSyncManagerPurpose(owned, 8, op.KnowledgeBaseID, ds.ID))
	require.Error(t, f.store.CheckNextcloudAutomaticSyncManagerPurpose(owned, 7, f.credential.TemplateKBID, ds.ID))
	require.Error(t, f.store.CheckNextcloudAutomaticSyncManagerPurpose(owned, 7, op.KnowledgeBaseID, "other-source"))
	build, closeBuild := f.configurationBuildContext(t, op, ds)
	defer closeBuild()
	build, closeBuildRead, err := f.store.BeginNextcloudBuildConfigurationRead(build)
	require.NoError(t, err)
	defer closeBuildRead()
	require.Error(
		t,
		f.store.CheckNextcloudAutomaticSyncManagerPurpose(build, 7, op.KnowledgeBaseID, ds.ID),
		"a build configuration purpose is not an original-fetch purpose",
	)
	closeRead()
	require.Error(
		t,
		f.store.CheckNextcloudAutomaticSyncManagerPurpose(owned, 7, op.KnowledgeBaseID, ds.ID),
		"closed body leases cannot authorize an automatic sync",
	)
}

func TestNextcloudAutomaticSyncManagerPurposeRechecksAuthorityWithIndependentBody(t *testing.T) {
	for _, mode := range []string{
		"manager denied",
		"manager unavailable",
		"directory stale",
		"directory disabled",
		"delegation revoked",
		"credential disabled",
		"source paused",
	} {
		t.Run(mode, func(t *testing.T) {
			f := newOnboardingOriginalActualFixture(t)
			op, err := f.onboarding.Reserve(
				context.Background(),
				f.credential,
				f.request("Independent body manager revocation"),
			)
			require.NoError(t, err)
			_, ds := f.pair(t, op)
			ConfigureKnowledgeOriginalInputSourceVerifier(f.store, &machineFetchConfigurationBeginner{store: f.store})
			worker := types.WithTaskAuthorization(context.Background(), 7, types.TaskInitiator{})
			owned, closeRead, err := f.store.BeginNextcloudOriginalFetchConfigurationRead(worker, ds)
			require.NoError(t, err)
			defer closeRead()
			require.NoError(t, f.store.CheckNextcloudAutomaticSyncManagerPurpose(owned, 7, op.KnowledgeBaseID, ds.ID))
			switch mode {
			case "manager denied":
				f.managerAllow.Store(false)
			case "manager unavailable":
				f.managerUnknown.Store(true)
			case "directory stale":
				require.NoError(
					t,
					f.db.Model(&types.Directory{}).
						Where("id = ?", f.credential.DirectoryID).
						Update("last_successful_sync_at", time.Now().Add(-10*time.Minute)).
						Error,
				)
			case "directory disabled":
				require.NoError(
					t,
					f.db.Model(&types.Directory{}).
						Where("id = ?", f.credential.DirectoryID).
						Update("enabled", false).
						Error,
				)
			case "delegation revoked":
				require.NoError(
					t,
					f.db.Where(
						"tenant_id = ? AND directory_id = ? AND object_guid = ?",
						7,
						f.credential.DirectoryID,
						onboardingActualGUID,
					).
						Delete(&types.NextcloudOnboardingDelegation{}).
						Error,
				)
			case "credential disabled":
				require.NoError(
					t,
					f.db.Model(&types.NextcloudOnboardingCredential{}).
						Where("id = ?", f.credential.ID).
						Update("enabled", false).
						Error,
				)
			case "source paused":
				// This fixture runs the actual SQLite schema. Indexed-source
				// withdrawal is PostgreSQL-only and has its own actual PG tests.
				// A supported persisted pause must still revoke held sync purpose.
				require.NoError(t, f.db.Model(&types.DataSource{}).Where("id = ?", ds.ID).
					Update("status", types.DataSourceStatusPaused).Error)
			}
			require.Error(
				t,
				f.store.CheckNextcloudAutomaticSyncManagerPurpose(owned, 7, op.KnowledgeBaseID, ds.ID),
				"a held independent body lease cannot preserve revoked manager authority",
			)
			if mode == "manager unavailable" {
				f.managerUnknown.Store(false)
				require.NoError(
					t,
					f.store.CheckNextcloudAutomaticSyncManagerPurpose(owned, 7, op.KnowledgeBaseID, ds.ID),
					"a later retry must succeed after manager availability is restored",
				)
			}
		})
	}
}
