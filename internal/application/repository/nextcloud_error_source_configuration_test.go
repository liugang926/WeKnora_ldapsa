//go:build sqlite_fts5

package repository

import (
	"context"
	"testing"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
)

func TestNextcloudOriginalConfigurationReadAcceptsRecoverableErrorSource(t *testing.T) {
	f := newOnboardingOriginalActualFixture(t)
	op, err := f.onboarding.Reserve(
		context.Background(),
		f.credential,
		f.request("Recoverable error source"),
	)
	require.NoError(t, err)
	_, ds := f.pair(t, op)
	require.NoError(t, f.db.Model(&types.DataSource{}).Where("id = ?", ds.ID).
		Update("status", types.DataSourceStatusError).Error)
	ConfigureKnowledgeOriginalInputSourceVerifier(f.store,
		&machineFetchConfigurationBeginner{store: f.store})

	worker := types.WithTaskAuthorization(context.Background(), 7, types.TaskInitiator{})
	owned, closeRead, err := f.store.BeginNextcloudOriginalFetchConfigurationRead(worker, ds)
	require.NoError(t, err)
	defer closeRead()
	require.NotNil(t, owned.Value(scopedOnboardingConfigurationReadKey{}))
}
