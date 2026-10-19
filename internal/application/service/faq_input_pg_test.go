package service

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func faqPostgresActualFixture(
	t *testing.T,
) (*knowledgeService, *repository.KnowledgeInputProvenanceStore, *gorm.DB, context.Context) {
	t.Helper()
	dsn := os.Getenv("WEKNORA_FAQ_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("owned FAQ PostgreSQL fixture DSN is not configured")
	}
	admin, err := gorm.Open(
		postgres.New(postgres.Config{DSN: dsn, PreferSimpleProtocol: true}),
		&gorm.Config{DisableForeignKeyConstraintWhenMigrating: true},
	)
	require.NoError(t, err)
	schema := "faq_original_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	require.NoError(t, admin.Exec("CREATE SCHEMA "+schema).Error)
	t.Cleanup(func() {
		require.NoError(t, admin.Exec("DROP SCHEMA "+schema+" CASCADE").Error)
		sqlDB, _ := admin.DB()
		_ = sqlDB.Close()
	})
	db, err := gorm.Open(
		postgres.New(postgres.Config{DSN: dsn + " search_path=" + schema + ",public"}),
		&gorm.Config{DisableForeignKeyConstraintWhenMigrating: true},
	)
	require.NoError(t, err)
	t.Cleanup(func() { sqlDB, _ := db.DB(); _ = sqlDB.Close() })
	require.NoError(
		t,
		db.AutoMigrate(
			&types.KnowledgeBase{},
			&types.Knowledge{},
			&types.Chunk{},
			&types.KnowledgeTag{},
			&types.KnowledgeTagRelation{},
			&types.Session{},
			&types.Message{},
			&types.User{},
			&types.TenantMember{},
			&types.TenantAPIKey{},
			&types.Directory{},
			&types.DirectoryIdentity{},
			&types.DirectoryGroup{},
			&types.DirectoryGroupMembership{},
			&types.TenantGroupRoleGrant{},
			&types.ResourceAccessPolicy{},
			&types.ResourceGroupGrant{},
		),
	)
	// Production's agent migration defines FAQ metadata as JSONB. AutoMigrate
	// otherwise uses the cross-dialect struct's JSON column type.
	require.NoError(t, db.Exec("ALTER TABLE chunks ALTER COLUMN metadata TYPE JSONB USING metadata::jsonb").Error)
	require.NoError(
		t,
		db.Exec(
			("CREATE TABLE nextcloud_source_versions(candidate_knowledge_id TEXT); CREAT" +
				"E TABLE nextcloud_source_revisions(candidate_knowledge_id TEXT)"),
		).Error,
	)
	for _, file := range []string{"000134_knowledge_input_provenance", "000135_knowledge_configuration_history_inputs", "000136_knowledge_actual_config_controls", "000137_knowledge_original_worker_actor", "000139_knowledge_collection_checkpoints", "000140_knowledge_collection_display_inputs", "000141_tag_statistics_checkpoints", "000144_faq_original_entry_inputs"} {
		ddl, err := os.ReadFile("../../../migrations/versioned/" + file + ".up.sql")
		require.NoError(t, err)
		require.NoError(t, db.Exec(string(ddl)).Error)
	}
	store := repository.NewKnowledgeInputProvenanceStore(db)
	groups := NewGroupAccessService(repository.NewGroupAccessRepository(db))
	repository.ConfigureKnowledgeInputActorGroupAccess(store, groups)
	factory := func(tx *gorm.DB) *repository.KnowledgeInputProvenanceStore {
		actual := repository.NewKnowledgeInputProvenanceStore(tx)
		repository.ConfigureKnowledgeInputActorGroupAccess(
			actual,
			NewGroupAccessService(repository.NewGroupAccessRepository(tx)),
		)
		return actual
	}
	repository.ConfigureKnowledgeDisplayInputWriters(
		store,
		repository.NewKnowledgeTagRepository(db),
		repository.NewKnowledgeRepository(db),
		factory,
	)
	user := &types.User{
		ID:       "actual-faq-author",
		TenantID: 7,
		Username: "faq-author",
		Email:    "faq-author@isolated.test",
		IsActive: true,
	}
	require.NoError(t, db.Create(user).Error)
	require.NoError(
		t,
		db.Create(
			&types.TenantMember{
				UserID:   user.ID,
				TenantID: 7,
				Role:     types.TenantRoleAdmin,
				Status:   types.TenantMemberStatusActive,
			},
		).Error,
	)
	ctx := types.WithExecutionTenant(
		types.WithPrincipal(
			types.WithCaller(
				context.Background(),
				types.Caller{TenantID: 7, UserID: user.ID, Role: types.TenantRoleAdmin},
			),
			types.Principal{Type: types.PrincipalWebUser, ID: user.ID},
		),
		7,
	)
	kb := &types.KnowledgeBase{
		ID:               "kb",
		TenantID:         7,
		Name:             "actual FAQ PostgreSQL config",
		Type:             types.KnowledgeBaseTypeFAQ,
		EmbeddingModelID: "embed",
	}
	require.NoError(
		t,
		store.CreateKnowledgeBaseWithInput(
			repository.WithIndependentKnowledgeBaseNameInput(
				repository.WithIndependentKnowledgeInput(ctx),
				"",
				kb.Name,
			),
			kb,
		),
	)
	tenant := &types.Tenant{
		ID: 7,
		RetrieverEngines: types.RetrieverEngines{
			Engines: []types.RetrieverEngineParams{
				{RetrieverType: types.VectorRetrieverType, RetrieverEngineType: types.PostgresRetrieverEngineType},
			},
		},
	}
	ctx = context.WithValue(ctx, types.TenantInfoContextKey, tenant)
	grant, err := access.ResolveKB(
		ctx,
		access.KBRequest{Caller: types.CallerFromContext(ctx)},
		kb,
		types.OrgRoleEditor,
		nil,
		nil,
	)
	require.NoError(t, err)
	ctx = grant.Context(ctx)
	s := &knowledgeService{
		repo:                 repository.NewKnowledgeRepository(db),
		chunkRepo:            repository.NewChunkRepository(db),
		tagRepo:              repository.NewKnowledgeTagRepository(db),
		kbService:            &createKnowledgeFileKBServiceStub{kb: kb},
		modelService:         parentChildModelService{embedder: parentChildEmbedder{}},
		retrieveEngine:       parentChildRetrieveRegistry{engine: &faqInputEngine{}},
		tenantRepo:           knowledgeInputTenantRepo{tenant: tenant},
		task:                 &knowledgeInputQueue{},
		knowledgeInputs:      store,
		knowledgeInputWriter: store,
	}
	guard := access.NewNextcloudSourceLineageGuard(
		nil,
		&knowledgeBaseService{repo: repository.NewKnowledgeBaseRepository(db)},
		nil,
		nil,
		groups,
		nil,
		store,
	)
	ConfigureFAQInputProvenance(s, store, guard)
	return s, store, db, ctx
}

func TestFAQPostgresActualProducerPerEntryAndControlsABA(t *testing.T) {
	s, store, db, ctx := faqPostgresActualFixture(t)
	first := &types.FAQEntryPayload{StandardQuestion: "first PostgreSQL FAQ", Answers: []string{"first original body"}}
	a, err := s.CreateFAQEntry(
		faqInputContext(ctx, t, types.FAQInputOperation{Kind: "create", Entry: first}),
		"kb",
		first,
	)
	require.NoError(t, err)
	second := &types.FAQEntryPayload{
		StandardQuestion: "second PostgreSQL FAQ",
		Answers:          []string{"second original body"},
	}
	b, err := s.CreateFAQEntry(
		faqInputContext(ctx, t, types.FAQInputOperation{Kind: "create", Entry: second}),
		"kb",
		second,
	)
	require.NoError(t, err)
	other, err := store.CaptureFAQInputs(
		ctx,
		types.FAQInputReadRequest{TenantID: 7, KnowledgeBaseID: "kb", ChunkIDs: []string{b.ChunkID}},
	)
	require.NoError(t, err)
	first.Answers = []string{"edited first body"}
	_, err = s.UpdateFAQEntry(
		faqInputContext(ctx, t, types.FAQInputOperation{Kind: "update", Entry: first, EntrySeqID: a.ID}),
		"kb",
		a.ID,
		first,
	)
	require.NoError(t, err)
	require.NoError(t, store.RecheckFAQInputs(ctx, other))
	all, err := store.CaptureFAQInputs(
		ctx,
		types.FAQInputReadRequest{TenantID: 7, KnowledgeBaseID: "kb", AllEntries: true},
	)
	require.NoError(t, err)
	require.NoError(t, db.Model(&types.Chunk{}).Where("id=?", a.ChunkID).Update("is_enabled", false).Error)
	require.NoError(t, db.Model(&types.Chunk{}).Where("id=?", a.ChunkID).Update("is_enabled", true).Error)
	require.Error(t, store.RecheckFAQInputs(ctx, all))
	_, err = s.ListFAQEntries(ctx, "kb", &types.Pagination{Page: 1, PageSize: 10}, nil, 0, "", "", "", nil)
	require.Error(t, err)
}
