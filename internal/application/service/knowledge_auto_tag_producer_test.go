package service

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/models/chat"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/google/uuid"
	"github.com/hibiken/asynq"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

type autoTagActualModel struct {
	*autoTagChatModel
	beforeReturn func()
}

func (m *autoTagActualModel) Chat(ctx context.Context, messages []chat.Message, options *chat.ChatOptions) (*types.ChatResponse, error) {
	response, err := m.autoTagChatModel.Chat(ctx, messages, options)
	if m.beforeReturn != nil {
		m.beforeReturn()
	}
	return response, err
}

type autoTagActualLineageChecker struct{ calls int }

func (c *autoTagActualLineageChecker) Check(ctx context.Context, lineage *types.SourceLineage, scope access.LineageReadScope) error {
	c.calls++
	return lineage.RequireComplete()
}

func autoTagActualFixture(t *testing.T, pg bool) (*repository.KnowledgeInputProvenanceStore, *gorm.DB, context.Context) {
	t.Helper()
	if !pg {
		_, store, db, _, ctx := knowledgeInputServiceFixture(t)
		return store, db, ctx
	}
	dsn := os.Getenv("WEKNORA_AUTO_TAG_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("set WEKNORA_AUTO_TAG_TEST_POSTGRES_DSN for disposable PostgreSQL")
	}
	admin, err := gorm.Open(postgres.Open(dsn), &gorm.Config{DisableForeignKeyConstraintWhenMigrating: true})
	require.NoError(t, err)
	schema := "auto_tag_" + uuid.NewString()[:8]
	require.NoError(t, admin.Exec("CREATE SCHEMA "+schema).Error)
	searchPath := schema
	if os.Getenv("WEKNORA_LOCAL_DERIVED_NATIVE_VECTOR") == "1" {
		searchPath += ",public"
	}
	db, err := gorm.Open(postgres.Open(dsn+" search_path="+searchPath), &gorm.Config{DisableForeignKeyConstraintWhenMigrating: true})
	require.NoError(t, err)
	t.Cleanup(func() {
		sqlDB, _ := db.DB()
		_ = sqlDB.Close()
		_ = admin.Exec("DROP SCHEMA " + schema + " CASCADE").Error
		adminSQL, _ := admin.DB()
		_ = adminSQL.Close()
	})
	require.NoError(t, db.AutoMigrate(&types.KnowledgeBase{}, &types.Knowledge{}, &types.Chunk{}, &types.Session{}, &types.Message{}, &types.KnowledgeTag{}, &types.KnowledgeTagRelation{}))
	require.NoError(t, db.Exec("CREATE TABLE nextcloud_source_versions(candidate_knowledge_id TEXT); CREATE TABLE nextcloud_source_revisions(candidate_knowledge_id TEXT)").Error)
	sqlDB, err := db.DB()
	require.NoError(t, err)
	migration, err := gorm.Open(postgres.New(postgres.Config{Conn: sqlDB, PreferSimpleProtocol: true}), &gorm.Config{})
	require.NoError(t, err)
	for _, file := range []string{"000134_knowledge_input_provenance.up.sql", "000135_knowledge_configuration_history_inputs.up.sql", "000136_knowledge_actual_config_controls.up.sql", "000137_knowledge_original_worker_actor.up.sql"} {
		script, err := os.ReadFile("../../../migrations/versioned/" + file)
		require.NoError(t, err)
		require.NoError(t, migration.Exec(string(script)).Error)
	}
	store := repository.NewKnowledgeInputProvenanceStore(db)
	kb := &types.KnowledgeBase{ID: "kb", TenantID: 7, Name: "explicit human config", ChunkingConfig: types.ChunkingConfig{ChunkSize: 512}, EmbeddingModelID: "embed"}
	require.NoError(t, store.CreateKnowledgeBaseWithInput(repository.WithIndependentKnowledgeInput(context.Background()), kb))
	return store, db, types.WithExecutionTenant(context.Background(), 7)
}

func runKnowledgeAutoTagActualEnqueueOriginalActorManifestAndSQLWriter(t *testing.T, pg bool) {
	for _, mutation := range []string{"none", "candidate ABA", "role revoked", "legacy unknown", "payload forge", "API revoked"} {
		t.Run(mutation, func(t *testing.T) {
			store, db, ctx := autoTagActualFixture(t, pg)
			require.NoError(t, db.AutoMigrate(&types.User{}, &types.TenantMember{}, &types.TenantAPIKey{}, &types.KnowledgeTagRelation{}, &types.Directory{}, &types.DirectoryIdentity{}, &types.DirectoryGroup{}, &types.DirectoryGroupMembership{}, &types.TenantGroupRoleGrant{}, &types.ResourceAccessPolicy{}, &types.ResourceGroupGrant{}))
			path := "../../../migrations/sqlite/000059_knowledge_collection_display_inputs.up.sql"
			if pg {
				path = "../../../migrations/versioned/000140_knowledge_collection_display_inputs.up.sql"
			}
			script, err := os.ReadFile(path)
			require.NoError(t, err)
			migration := db
			if pg {
				sqlDB, err := db.DB()
				require.NoError(t, err)
				migration, err = gorm.Open(postgres.New(postgres.Config{Conn: sqlDB, PreferSimpleProtocol: true}), &gorm.Config{})
				require.NoError(t, err)
			}
			require.NoError(t, migration.Exec(string(script)).Error)
			require.NoError(t, db.AutoMigrate(&types.KnowledgeProcessingSpan{}))
			require.NoError(t, db.Exec("CREATE UNIQUE INDEX IF NOT EXISTS auto_span_producer_unique ON knowledge_processing_spans(knowledge_id,attempt,span_id)").Error)
			spanPath := "../../../migrations/sqlite/000057_processing_span_original_inputs.up.sql"
			if pg {
				spanPath = "../../../migrations/versioned/000138_processing_span_original_inputs.up.sql"
			}
			spanScript, err := os.ReadFile(spanPath)
			require.NoError(t, err)
			require.NoError(t, migration.Exec(string(spanScript)).Error)

			require.NoError(t, db.Create(&types.User{ID: "auto-user", Username: "auto-user", Email: "auto@example.test", IsActive: true}).Error)
			require.NoError(t, db.Create(&types.TenantMember{TenantID: 7, UserID: "auto-user", Role: types.TenantRoleAdmin, Status: types.TenantMemberStatusActive}).Error)
			ctx = types.WithCaller(ctx, types.Caller{TenantID: 7, UserID: "auto-user", Role: types.TenantRoleAdmin})
			ctx = types.WithPrincipal(ctx, types.Principal{Type: types.PrincipalWebUser, ID: "auto-user"})
			tags := repository.NewKnowledgeTagRepository(db)
			knowledge := repository.NewKnowledgeRepository(db)
			factory := func(tx *gorm.DB) *repository.KnowledgeInputProvenanceStore {
				actual := repository.NewKnowledgeInputProvenanceStore(tx)
				repository.ConfigureKnowledgeInputActorGroupAccess(actual, NewGroupAccessService(repository.NewGroupAccessRepository(tx)))
				return actual
			}
			repository.ConfigureKnowledgeDisplayInputWriters(store, tags, knowledge, factory)
			repository.ConfigureKnowledgeInputActorGroupAccess(store, NewGroupAccessService(repository.NewGroupAccessRepository(db)))
			var kb types.KnowledgeBase
			require.NoError(t, db.Where("id='kb'").Take(&kb).Error)
			kb.Type = types.KnowledgeBaseTypeDocument
			kb.AutoTagConfig = &types.AutoTagConfig{Enabled: true, ModelID: "mock", MaxTags: 2}
			require.NoError(t, store.UpdateKnowledgeBaseWithInput(repository.WithIndependentKnowledgeInput(ctx), &kb))
			var apiKey *types.TenantAPIKey
			if mutation == "API revoked" {
				tenant := uint64(7)
				apiKey = &types.TenantAPIKey{ID: 41, TenantID: &tenant, Name: "actual-span-worker", ScopeType: types.APIKeyScopeTenant, KeyHash: "actual-span-worker-key", KnowledgeBaseIDs: types.StringArray{"kb"}, Capabilities: types.StringArray{string(types.APIKeyCapabilityIngest), string(types.APIKeyCapabilityRetrieve)}}
				require.NoError(t, db.Create(apiKey).Error)
				ctx = types.WithCaller(ctx, types.Caller{TenantID: 7, Role: types.TenantRoleViewer})
				ctx = types.WithPrincipal(ctx, types.Principal{Type: types.PrincipalAPITenant, ID: "7"})
				ctx = types.WithTenantAPIKeyScope(ctx, types.TenantAPIKeyScope{KeyID: apiKey.ID, KnowledgeBaseIDs: apiKey.KnowledgeBaseIDs, Capabilities: apiKey.Capabilities})
			}
			k := &types.Knowledge{ID: "auto-actual", TenantID: 7, KnowledgeBaseID: "kb", Type: types.KnowledgeTypeManual, Source: "manual", Channel: "web", FileName: "human document", Metadata: types.JSON(`{"manual":{"content":"original human body"}}`), CustomMetadata: types.JSON(`{}`), ParseStatus: types.ParseStatusFinalizing, EnableStatus: "enabled"}
			original, err := store.CreateKnowledgeWithInput(repository.WithIndependentKnowledgeInput(ctx), k, "original human body")
			require.NoError(t, err)
			build := repository.WithKnowledgeBuildInputProof(ctx, store, original)
			chunk := &types.Chunk{ID: "auto-actual-chunk", TenantID: 7, KnowledgeBaseID: "kb", KnowledgeID: k.ID, Content: "actual classification body", ChunkType: types.ChunkTypeText, IsEnabled: true}
			require.NoError(t, repository.NewChunkRepository(db).CreateChunks(build, []*types.Chunk{chunk}))
			for i, id := range []string{"candidate-a", "candidate-b"} {
				tag := &types.KnowledgeTag{ID: id, TenantID: 7, KnowledgeBaseID: "kb", Name: id, SortOrder: i}
				tagCtx := repository.WithIndependentKnowledgeTagCreateInput(ctx, "kb", tag.Name, "", i)
				if mutation == "legacy unknown" && i == 1 {
					tagCtx = ctx
				}
				require.NoError(t, tags.Create(tagCtx, tag))
			}
			tracker := NewSpanTracker(repository.NewKnowledgeSpanRepository(db), nil)
			root, attempt, err := tracker.OpenAttempt(build, k.ID, "actual-auto-trace")
			require.NoError(t, err)
			require.NotNil(t, root)
			stage := tracker.BeginStage(build, k.ID, attempt, types.StagePostProcess, types.JSONMap{"phase": "actual original postprocess"})
			require.NotNil(t, stage)
			queue := &autoTagRecordingQueue{}
			post := &KnowledgePostProcessService{taskEnqueuer: queue, chunkRepo: repository.NewChunkRepository(db), knowledgeInputs: store}
			require.True(t, post.enqueueAutoTagTask(build, types.KnowledgePostProcessPayload{TenantID: 7, KnowledgeBaseID: "kb", KnowledgeID: k.ID}, attempt))
			require.NotNil(t, queue.task)
			var payload types.KnowledgeAutoTagPayload
			require.NoError(t, json.Unmarshal(queue.task.Payload(), &payload))
			require.NotEmpty(t, payload.OriginalInput.BodyDigest)
			require.Equal(t, []string{chunk.ID}, payload.OriginalInput.ChunkIDs)
			model := &autoTagActualModel{autoTagChatModel: &autoTagChatModel{response: `{"matches":[{"index":1,"confidence":0.9}]}`}}
			if mutation == "candidate ABA" {
				model.beforeReturn = func() {
					require.NoError(t, db.Model(&types.KnowledgeTag{}).Where("id='candidate-b'").Update("name", "mutated unselected").Error)
					require.NoError(t, db.Model(&types.KnowledgeTag{}).Where("id='candidate-b'").Update("name", "candidate-b").Error)
				}
			}
			if mutation == "role revoked" {
				model.beforeReturn = func() {
					require.NoError(t, db.Model(&types.TenantMember{}).Where("user_id='auto-user'").Update("role", types.TenantRoleViewer).Error)
				}
			}
			if mutation == "API revoked" {
				model.beforeReturn = func() { now := time.Now(); require.NoError(t, db.Model(apiKey).Update("revoked_at", &now).Error) }
			}
			if mutation == "payload forge" {
				payload.OriginalInput.AdmissionID = "forged-original"
				data, err := json.Marshal(payload)
				require.NoError(t, err)
				queue.task = asynq.NewTask(types.TypeKnowledgeAutoTag, data)
			}
			checker := &autoTagActualLineageChecker{}
			handler := &KnowledgeAutoTagService{knowledgeRepo: knowledge, tagRepo: tags, kbService: &autoTagKBService{kb: &kb}, chunkService: &autoTagChunkService{chunks: []*types.Chunk{chunk}}, modelService: &stubModelService{chatModel: model}, knowledgeInputs: store, lineageChecker: checker, spanTracker: tracker}
			err = handler.Handle(context.Background(), queue.task)
			var relations []types.KnowledgeTagRelation
			require.NoError(t, db.Where("knowledge_id=?", k.ID).Find(&relations).Error)
			if mutation == "none" {
				require.NoError(t, err)
				require.Len(t, relations, 1)
				snapshot, err := store.CaptureSpanOutputInputs(ctx, types.SpanOutputReadRequest{TenantID: 7, KnowledgeBaseID: "kb", KnowledgeID: k.ID, Attempt: attempt})
				require.NoError(t, err)
				found := false
				for _, row := range snapshot.Rows {
					if row.Name == autoTagSpanName {
						require.Equal(t, types.SpanStatusDone, row.Status)
						require.Equal(t, "complete", row.ContentProvenance)
						found = true
					}
				}
				require.True(t, found)

				require.Equal(t, "candidate-a", relations[0].TagID)
				require.NotEmpty(t, model.messages)
				require.Greater(t, checker.calls, 0)
				var manifests int64
				require.NoError(t, db.Table("knowledge_auto_tag_input_manifests").Count(&manifests).Error)
				require.EqualValues(t, 1, manifests)
			} else {
				require.Error(t, err)
				require.Empty(t, relations)
				if mutation == "legacy unknown" || mutation == "payload forge" {
					require.Empty(t, model.messages)
				}
			}
		})
	}
}

func TestKnowledgeAutoTagSQLiteActualEnqueueOriginalActorManifestAndSQLWriter(t *testing.T) {
	runKnowledgeAutoTagActualEnqueueOriginalActorManifestAndSQLWriter(t, false)
}
func TestKnowledgeAutoTagPostgresActualEnqueueOriginalActorManifestAndSQLWriter(t *testing.T) {
	runKnowledgeAutoTagActualEnqueueOriginalActorManifestAndSQLWriter(t, true)
}
