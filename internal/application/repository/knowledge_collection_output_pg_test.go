package repository

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func collectionPostgresFixture(t *testing.T) (*KnowledgeInputProvenanceStore, *gorm.DB) {
	t.Helper()
	store, db := knowledgeInputPostgresFixture(t)
	require.NoError(t, db.AutoMigrate(&types.KnowledgeTag{}, &types.KnowledgeTagRelation{}))
	db.DisableForeignKeyConstraintWhenMigrating = true
	require.NoError(t, db.AutoMigrate(&types.User{}, &types.TenantMember{}, &types.ResourceAccessPolicy{}))
	db.DisableForeignKeyConstraintWhenMigrating = false
	sqlDB, err := db.DB()
	require.NoError(t, err)
	migration, err := gorm.Open(postgres.New(postgres.Config{Conn: sqlDB, PreferSimpleProtocol: true}), &gorm.Config{})
	require.NoError(t, err)
	for _, name := range []string{"000139_knowledge_collection_checkpoints", "000140_knowledge_collection_display_inputs"} {
		script, err := os.ReadFile("../../../migrations/versioned/" + name + ".up.sql")
		require.NoError(t, err)
		require.NoError(t, migration.Exec(string(script)).Error)
	}
	kb := &types.KnowledgeBase{
		ID:               "kb",
		TenantID:         7,
		Name:             "original collection KB",
		Type:             types.KnowledgeBaseTypeDocument,
		EmbeddingModelID: "embed",
		SummaryModelID:   "summary",
	}
	require.NoError(
		t,
		store.ReplaceKnowledgeBaseConfigurationWithInput(WithIndependentKnowledgeInput(context.Background()), kb),
	)
	require.NoError(
		t,
		db.Create(
			&types.User{
				ID:       "collection-user",
				TenantID: 7,
				Username: "collection-user",
				Email:    "collection@example.test",
				IsActive: true,
			},
		).Error,
	)
	require.NoError(
		t,
		db.Create(
			&types.TenantMember{
				UserID:   "collection-user",
				TenantID: 7,
				Role:     types.TenantRoleAdmin,
				Status:   types.TenantMemberStatusActive,
			},
		).Error,
	)
	ctx := types.WithCaller(
		context.Background(),
		types.Caller{TenantID: 7, UserID: "collection-user", Role: types.TenantRoleAdmin},
	)
	ctx = types.WithPrincipal(ctx, types.Principal{Type: types.PrincipalWebUser, ID: "collection-user"})
	factory := func(tx *gorm.DB) *KnowledgeInputProvenanceStore {
		actual := NewKnowledgeInputProvenanceStore(tx)
		ConfigureKnowledgeInputActorGroupAccess(actual, displayTestGroups{db: tx})
		return actual
	}
	ConfigureKnowledgeDisplayInputWriters(store, NewKnowledgeTagRepository(db), NewKnowledgeRepository(db), factory)
	kb.Name = "actual original human collection KB"
	require.NoError(
		t,
		store.UpdateKnowledgeBaseWithInput(
			WithIndependentKnowledgeBaseNameInput(WithIndependentKnowledgeInput(ctx), kb.ID, kb.Name),
			kb,
		),
	)
	for _, id := range []string{"doc-a", "doc-b"} {
		var k types.Knowledge
		require.NoError(t, db.Where("id = ?", id).Take(&k).Error)
		meta := k.GetMetadata()
		scope := NextcloudContentScope{
			TenantID:        7,
			KnowledgeBaseID: "kb",
			KnowledgeID:     id,
			DataSourceID:    "ds",
			ExternalID:      meta["external_id"],
		}
		leases := NewNextcloudContentLeaseStore(db)
		lease, err := leases.AcquireKnowledge(
			context.Background(),
			scope,
			NextcloudContentBuildLease,
			"collection-build-"+id,
			time.Minute,
		)
		require.NoError(t, err)
		ctx := WithNextcloudBuildLease(context.Background(), leases, lease, scope)
		_, err = store.AdmitNextcloudBuildInputs(ctx, []byte("actual original collection artifact "+id))
		require.NoError(t, err)
	}
	return store, db
}

func TestPostgresKnowledgeCollectionOriginalInputsAndProjection(t *testing.T) {
	store, _ := collectionPostgresFixture(t)
	ctx := context.Background()
	snapshot, err := store.CaptureKnowledgeCollectionInputs(
		ctx,
		types.KnowledgeCollectionReadRequest{Scopes: []types.KnowledgeSearchScope{{TenantID: 7, KBID: "kb"}}},
	)
	require.NoError(t, err)
	require.Len(t, snapshot.Candidates, 2)
	for _, candidate := range snapshot.Candidates {
		require.Len(t, candidate.Proof.SourceLineage.Sources, 1)
		require.Equal(t, "nextcloud", candidate.Proof.SourceLineage.Sources[0].Provider)
	}
	for _, mode := range []string{"list", "search", "folders"} {
		selection, err := store.SelectKnowledgeCollectionInputs(
			ctx,
			snapshot,
			types.KnowledgeCollectionSelectionRequest{
				Mode:  mode,
				Page:  types.Pagination{Page: 1, PageSize: 1},
				Limit: 1,
			},
		)
		require.NoError(t, err)
		if mode == "folders" {
			require.Equal(t, int64(2), selection.Folders.TotalDocumentCount)
		} else {
			require.Equal(t, int64(2), selection.Total)
			require.Len(t, selection.Knowledge, 1)
		}
		require.NoError(t, store.RecheckKnowledgeCollectionInputs(ctx, snapshot, selection))
	}
}

type collectionPostgresPauseKey struct{}

func TestPostgresKnowledgeCollectionRRWholeSetAndAbsenceABA(t *testing.T) {
	for _, change := range []string{"non_returned_body", "body_aba", "membership_aba", "config_aba", "source_withdraw"} {
		t.Run(change, func(t *testing.T) {
			store, db := collectionPostgresFixture(t)
			ctx := context.Background()
			baseline, err := store.CaptureKnowledgeCollectionInputs(
				ctx,
				types.KnowledgeCollectionReadRequest{Scopes: []types.KnowledgeSearchScope{{TenantID: 7, KBID: "kb"}}},
			)
			require.NoError(t, err)
			fired := false
			name := "collection_whole_set_changed"
			require.NoError(t, db.Callback().Query().After("gorm:query").Register(name, func(tx *gorm.DB) {
				if fired || tx.Statement.Context.Value(collectionPostgresPauseKey{}) != true ||
					tx.Statement.Table != "knowledges" {
					return
				}
				fired = true
				switch change {
				case "non_returned_body", "body_aba":
					require.NoError(t, db.Exec("UPDATE knowledges SET title='different' WHERE id='doc-b'").Error)
					if change == "body_aba" {
						require.NoError(t, db.Exec("UPDATE knowledges SET title='' WHERE id='doc-b'").Error)
					}
				case "membership_aba":
					require.NoError(
						t,
						db.Create(
							&types.Knowledge{
								ID:              "transient",
								TenantID:        7,
								KnowledgeBaseID: "kb",
								Type:            "manual",
								Channel:         "web",
								CustomMetadata:  types.JSON(`{}`),
							},
						).Error,
					)
					require.NoError(t, db.Unscoped().Where("id='transient'").Delete(&types.Knowledge{}).Error)
				case "config_aba":
					require.NoError(t, db.Exec("UPDATE knowledge_bases SET summary_model_id='new' WHERE id='kb'").Error)
					require.NoError(
						t,
						db.Exec("UPDATE knowledge_bases SET summary_model_id='summary' WHERE id='kb'").Error,
					)
				case "source_withdraw":
					require.NoError(
						t,
						NewNextcloudContentLeaseStore(
							db,
						).RetireKnowledge(context.Background(), NextcloudContentScope{TenantID: 7, KnowledgeBaseID: "kb", KnowledgeID: "doc-b", DataSourceID: "ds", ExternalID: "nextcloud:instance:78"}),
					)
				}
			}))
			t.Cleanup(func() { _ = db.Callback().Query().Remove(name) })
			captured, err := store.CaptureKnowledgeCollectionInputs(
				context.WithValue(ctx, collectionPostgresPauseKey{}, true),
				types.KnowledgeCollectionReadRequest{Scopes: []types.KnowledgeSearchScope{{TenantID: 7, KBID: "kb"}}},
			)
			require.NoError(t, err)
			require.True(t, fired)
			require.Equal(t, baseline, captured, "the full original set is read coherently under PostgreSQL RR")
			require.Error(
				t,
				store.RecheckKnowledgeCollectionInputs(ctx, captured, nil),
				"release must reject committed change, including a non-returned candidate or transient membership ABA",
			)
		})
	}
}
