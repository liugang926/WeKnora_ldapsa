package service

import (
	"context"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/models/chat"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/stretchr/testify/require"
)

func TestNextcloudIndependentConfigurationLegacyPairActualSQLite(t *testing.T) {
	for _, mode := range []string{"positive", "configuration ABA during model", "pair ABA during model", "body lease owner changed during model"} {
		t.Run(mode, func(t *testing.T) {
			m := newMachinePipelineFixture(t, false)
			var operations int64
			require.NoError(t, m.f.db.Model(&types.NextcloudOnboardingOperation{}).Count(&operations).Error)
			require.Zero(t, operations, "a legitimate generic pair has no fabricated onboarding operation")
			var config types.KnowledgeBase
			require.NoError(t, m.f.db.Where("id = ?", m.ds.KnowledgeBaseID).Take(&config).Error)
			_, lineage, err := m.f.store.ReadKnowledgeTemplateConfiguration(m.f.ctx, 7, config.ID)
			require.NoError(t, err)
			require.Empty(t, lineage.Sources)
			observed := false
			m.f.s.modelService = scopedConfigurationObservingModels{
				ModelService: m.f.s.modelService,
				summary: &scopedConfigurationObservingChat{
					delegate: m.f.model,
					before: func(ctx context.Context, _ []chat.Message) {
						observed = true
						var count int64
						require.NoError(
							t,
							m.f.db.Raw(("SELECT COUNT(*) FROM original_body_leases l JOIN original_body_field_heads"+
								" h ON h.body_id=l.body_id WHERE h.source_table='knowledge_bases' AND h.row"+
								"_key=? AND l.kind='read' AND l.released_at_ms IS NULL"), config.ID).
								Scan(&count).
								Error,
						)
						require.Positive(t, count, "independent config bytes use real body leases throughout the model")
						require.Empty(t, types.CallerFromContext(ctx).UserID)
					},
				},
			}
			if mode == "configuration ABA during model" {
				m.f.model.beforeReturn = func() {
					require.NoError(
						t,
						m.f.db.Model(&types.KnowledgeBase{}).
							Where("id = ?", config.ID).
							Update("summary_model_id", "temporary-altered-model").
							Error,
					)
					require.NoError(
						t,
						m.f.db.Model(&types.KnowledgeBase{}).
							Where("id = ?", config.ID).
							Update("summary_model_id", config.SummaryModelID).
							Error,
					)
				}
			}
			if mode == "pair ABA during model" {
				m.f.model.beforeReturn = func() {
					require.NoError(
						t,
						m.f.db.Exec(
							"UPDATE nextcloud_source_pairings SET state='pending' WHERE datasource_id=?",
							m.ds.ID,
						).Error,
					)
					require.NoError(
						t,
						m.f.db.Exec(
							"UPDATE nextcloud_source_pairings SET state='active' WHERE datasource_id=?",
							m.ds.ID,
						).Error,
					)
				}
			}
			if mode == "body lease owner changed during model" {
				m.f.model.beforeReturn = func() {
					require.NoError(
						t,
						m.f.db.Exec(
							("UPDATE original_body_leases SET owner_id=? WHERE lease_id IN (SELECT l.lea"+
								"se_id FROM original_body_leases l JOIN original_body_field_heads h ON h.bo"+
								"dy_id=l.body_id WHERE h.source_table='knowledge_bases' AND h.row_key=? AND"+
								" l.kind='read' AND l.released_at_ms IS NULL)"),
							"different-lease-owner",
							config.ID,
						).Error,
					)
				}
			}
			row := m.ingest(t)
			var taskErr error
			for i := 0; i < len(m.f.queue.tasks); i++ {
				taskErr = m.run(t, m.f.queue.tasks[i])
				if taskErr != nil {
					break
				}
			}
			require.True(t, observed)
			require.NoError(t, m.f.db.Where("id = ?", row.ID).Take(row).Error)
			if mode == "positive" {
				require.NoError(t, taskErr)
				require.Equal(t, types.ParseStatusCompleted, row.ParseStatus)
				require.NotEmpty(t, row.Description)
			} else {
				require.Error(t, taskErr)
				require.Empty(t, row.Description, "changed independent config/body authority cannot commit output")
			}
			var live int64
			require.NoError(
				t,
				m.f.db.Table("original_body_leases").Where("kind='read' AND released_at_ms IS NULL").Count(&live).Error,
			)
			require.Zero(t, live)
		})
	}
}

func TestNextcloudIndependentConfigurationOwnedScopeCancellationActualSQLite(t *testing.T) {
	m := newMachinePipelineFixture(t, false)
	parent, cancel := context.WithCancel(context.Background())
	ctx, closeOuter, err := m.f.store.BeginNextcloudOriginalFetchConfigurationRead(parent, m.ds)
	require.NoError(t, err)
	defer closeOuter()
	child, closeChild, err := m.f.store.BeginNextcloudOriginalFetchConfigurationRead(ctx, m.ds)
	require.NoError(t, err)
	require.NoError(t, repository.CheckScopedOnboardingConfigurationReads(child))
	closeChild()
	require.NoError(
		t,
		repository.CheckScopedOnboardingConfigurationReads(ctx),
		"closing a fresh read must preserve the caller's sibling",
	)
	cancel()
	require.ErrorIs(
		t,
		repository.CheckScopedOnboardingConfigurationReads(context.WithoutCancel(ctx)),
		context.Canceled,
		"detached contexts cannot revive canceled original worker authority",
	)
}

// An empty public manifest is still not a configuration-read purpose.
func TestNextcloudIndependentConfigurationPublicReaderNoWorkerPurposeActualSQLite(t *testing.T) {
	m := newMachinePipelineFixture(t, false)
	lineage, err := types.NewCompleteSourceLineage()
	require.NoError(t, err)
	read, err := m.f.store.BeginScopedIndependentConfigurationRead(
		context.Background(),
		7,
		m.ds.KnowledgeBaseID,
		lineage,
	)
	require.ErrorIs(t, err, types.ErrSourceLineageUnknown)
	require.Nil(t, read)
	var leases int64
	require.NoError(
		t,
		m.f.db.Table("original_body_leases").Where("kind='read' AND released_at_ms IS NULL").Count(&leases).Error,
	)
	require.Zero(t, leases)
}

func TestNextcloudIndependentConfigurationLeaseCheckpointRenewalActualSQLite(t *testing.T) {
	m := newMachinePipelineFixture(t, false)
	ctx, closeRead, err := m.f.store.BeginNextcloudOriginalFetchConfigurationRead(context.Background(), m.ds)
	require.NoError(t, err)
	defer closeRead()
	// The bounded ownership lifecycle is covered above. The actual lease
	// renewal port is exercised without a long sleep or a model invocation.
	require.NoError(t, repository.CheckScopedOnboardingConfigurationReads(ctx))
	var leases []struct {
		LeaseID     string
		ExpiresAtMS int64
	}
	require.NoError(
		t,
		m.f.db.Table("original_body_leases").Where("kind='read' AND released_at_ms IS NULL").Find(&leases).Error,
	)
	require.NotEmpty(t, leases)
	for _, lease := range leases {
		require.Greater(t, lease.ExpiresAtMS, time.Now().UnixMilli())
	}
}

var _ interfaces.ModelService = scopedConfigurationObservingModels{}
