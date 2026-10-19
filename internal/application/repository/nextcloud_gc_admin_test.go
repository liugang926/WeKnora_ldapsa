package repository

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	filesvc "github.com/Tencent/WeKnora/internal/application/service/file"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

func gcManualActual(t *testing.T, pg bool) {
	for _, mode := range []string{
		"collect",
		"retention",
		"no-coverage",
		"broad-reader",
		"disabled",
		"viewer",
		"api-key",
		"late-disabled",
		"cross-tenant",
		"missing-factory",
	} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			store, db, jobID, _, path := gcActualObjectFixture(t, pg)
			require.NoError(
				t,
				db.AutoMigrate(
					&types.Tenant{},
					&types.User{},
					&types.TenantMember{},
					&types.ResourceAccessPolicy{},
					&types.AuditLog{},
				),
			)
			require.NoError(t, db.Create(&types.Tenant{ID: 7, Name: "owned GC tenant", Status: "active"}).Error)
			require.NoError(
				t,
				db.Create(
					&types.User{
						ID:       "actual-admin",
						TenantID: 7,
						Username: "actual-admin",
						Email:    "gc@example.test",
						IsActive: true,
					},
				).Error,
			)
			require.NoError(
				t,
				db.Create(
					&types.TenantMember{
						UserID:   "actual-admin",
						TenantID: 7,
						Role:     types.TenantRoleAdmin,
						Status:   types.TenantMemberStatusActive,
					},
				).Error,
			)
			factory := func(tx *gorm.DB) *KnowledgeInputProvenanceStore {
				actual := NewKnowledgeInputProvenanceStore(tx)
				ConfigureKnowledgeInputActorGroupAccess(actual, displayTestGroups{db: tx})
				return actual
			}
			actor := factory(db)
			ConfigureKnowledgeDisplayInputWriters(actor, nil, nil, factory)
			ctx = types.WithExecutionTenant(ctx, 7)
			ctx = types.WithCaller(ctx, types.Caller{TenantID: 7, UserID: "actual-admin", Role: types.TenantRoleAdmin})
			ctx = types.WithPrincipal(ctx, types.Principal{Type: types.PrincipalWebUser, ID: "actual-admin"})
			if mode == "disabled" {
				require.NoError(t, db.Model(&types.User{}).Where("id='actual-admin'").Update("is_active", false).Error)
			}
			if mode == "viewer" {
				require.NoError(
					t,
					db.Model(&types.TenantMember{}).
						Where("user_id='actual-admin'").
						Update("role", types.TenantRoleViewer).
						Error,
				)
			}
			if mode == "api-key" {
				ctx = types.WithTenantAPIKeyScope(ctx, types.TenantAPIKeyScope{KeyID: 1, FullAccess: true})
			}
			if mode == "retention" {
				require.NoError(
					t,
					db.Model(&nextcloudGCJob{}).
						Where("id=?", jobID).
						Update("not_before", time.Now().Add(time.Hour)).
						Error,
				)
			}
			if mode == "no-coverage" {
				require.NoError(
					t,
					db.Exec(
						"DELETE FROM nextcloud_content_lease_coverage WHERE tenant_id=7 AND knowledge_base_id='kb'",
					).Error,
				)
			}
			if mode == "broad-reader" {
				lease, e := NewNextcloudContentLeaseStore(db).AcquireKBRead(ctx, 7, "kb", "actual-reader", time.Minute)
				require.NoError(t, e)
				defer func() { require.NoError(t, NewNextcloudContentLeaseStore(db).ReleaseLease(ctx, lease.ID)) }()
			}
			if mode == "missing-factory" {
				actor.displayAuthorityFactory = nil
			}
			calls := 0
			provider := filesvc.NewBackendScopedFileService(
				"backend",
				filesvc.NewLocalFileService(filepath.Dir(filepath.Dir(filepath.Dir(path))), ""),
			)
			deleter := provider.(interfaces.NextcloudPhysicalGCDeleter)
			store.ConfigureLocalDeletePreparation(
				func(context.Context, *types.StoredResource) (func(context.Context) (bool, error), error) {
					if mode == "late-disabled" {
						require.NoError(
							t,
							db.Model(&types.User{}).Where("id='actual-admin'").Update("is_active", false).Error,
						)
					}
					return func(ioCtx context.Context) (bool, error) {
						calls++
						return deleter.DeleteNextcloudObject(ioCtx, "storage://backend/local://7/old/source.txt")
					}, nil
				},
			)
			tenant := uint64(7)
			if mode == "cross-tenant" {
				tenant = 8
			}
			preview, e := store.PreviewJob(ctx, 7, jobID)
			require.NoError(t, e)
			raw, e := json.Marshal(preview)
			require.NoError(t, e)
			require.NotContains(t, string(raw), "storage://")
			require.NotContains(t, string(raw), path)
			var kbQueries []string
			require.NoError(
				t,
				db.Callback().Query().After("gorm:query").Register("gc_admin_scope_projection", func(tx *gorm.DB) {
					if tx.Statement.Table == "knowledge_bases" {
						kbQueries = append(kbQueries, tx.Statement.SQL.String())
					}
				}),
			)
			result, e := store.CollectJob(ctx, actor, tenant, jobID)
			switch mode {
			case "retention":
				require.ErrorIs(t, e, ErrNextcloudGCRetention)
			case "disabled", "viewer", "api-key", "missing-factory":
				require.ErrorIs(t, e, ErrNextcloudGCAdmin)
			case "cross-tenant":
				require.ErrorIs(t, e, ErrNextcloudGCJobUnavailable)
			default:
				require.NoError(t, e)
			}
			if mode == "collect" {
				require.Equal(t, 1, calls)
				_, e = os.Stat(path)
				require.ErrorIs(t, e, os.ErrNotExist)
				require.Positive(t, result.Status.ConfirmedReleasedBytes)
			} else {
				require.Zero(t, calls)
				_, e = os.Stat(path)
				require.NoError(t, e)
			}
			for _, q := range kbQueries {
				require.NotContains(t, q, "SELECT *")
				require.NotContains(t, q, "enrichment_templates")
			}
			var logs []types.AuditLog
			require.NoError(t, db.Where("target_id=?", jobID).Find(&logs).Error)
			if mode == "collect" || mode == "no-coverage" || mode == "broad-reader" || mode == "late-disabled" {
				require.Len(t, logs, 2)
				require.Equal(t, "actual-admin", logs[0].ActorUserID)
				require.Equal(t, types.AuditOutcomeAccepted, logs[0].Outcome)
			} else {
				require.Empty(t, logs)
			}
		})
	}
}
func TestNextcloudGCManualActualSQLite(t *testing.T)   { gcManualActual(t, false) }
func TestPostgresNextcloudGCManualActual(t *testing.T) { gcManualActual(t, true) }

func TestNextcloudGCAdminPaginationKeepsAllJobsAndScope(t *testing.T) {
	store, db := nextcloudGCTestDB(t)
	ctx := context.Background()
	now := time.Now().UTC()
	for i := 0; i < 110; i++ {
		id := fmt.Sprintf("job-%03d", i)
		require.NoError(
			t,
			db.Create(
				&nextcloudGCJob{
					ID:              id,
					TenantID:        7,
					KnowledgeBaseID: "kb",
					KnowledgeID:     id,
					CreatedAt:       now,
					UpdatedAt:       now,
					State:           "blocked",
				},
			).Error,
		)
	}
	require.NoError(
		t,
		db.Create(
			&nextcloudGCJob{
				ID:              "foreign-anchor",
				TenantID:        8,
				KnowledgeBaseID: "foreign",
				KnowledgeID:     "foreign",
				CreatedAt:       now,
				State:           "blocked",
			},
		).Error,
	)
	first, e := store.ListStatus(ctx, 7, 100, "kb", "")
	require.NoError(t, e)
	require.Len(t, first, 100)
	second, e := store.ListStatus(ctx, 7, 100, "kb", first[99].ID)
	require.NoError(t, e)
	require.Len(t, second, 10)
	ids := map[string]bool{}
	for _, row := range append(first, second...) {
		require.False(t, ids[row.ID])
		ids[row.ID] = true
	}
	require.Len(t, ids, 110)
	_, e = store.ListStatus(ctx, 7, 100, "kb", "foreign-anchor")
	require.ErrorIs(t, e, ErrNextcloudGCJobUnavailable)
	_, e = store.ListStatus(ctx, 7, 100, "different", first[99].ID)
	require.ErrorIs(t, e, ErrNextcloudGCJobUnavailable)
}
