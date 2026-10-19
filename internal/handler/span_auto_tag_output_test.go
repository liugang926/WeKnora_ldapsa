package handler

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/application/service"
	"github.com/Tencent/WeKnora/internal/models/chat"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/gin-gonic/gin"
	"github.com/hibiken/asynq"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

type autoSpanHTTPModel struct {
	calls int
	after func()
}

func (m *autoSpanHTTPModel) Chat(context.Context, []chat.Message, *chat.ChatOptions) (*types.ChatResponse, error) {
	m.calls++
	if m.after != nil {
		m.after()
	}
	return &types.ChatResponse{Content: `{"matches":[{"index":1,"confidence":0.9}]}`}, nil
}

func (m *autoSpanHTTPModel) ChatStream(
	context.Context,
	[]chat.Message,
	*chat.ChatOptions,
) (<-chan types.StreamResponse, error) {
	return nil, nil
}
func (m *autoSpanHTTPModel) GetModelName() string { return "isolated-auto" }
func (m *autoSpanHTTPModel) GetModelID() string   { return "isolated-auto" }

type autoSpanHTTPModels struct {
	interfaces.ModelService
	model *autoSpanHTTPModel
}

func (m autoSpanHTTPModels) GetChatModel(context.Context, string) (chat.Chat, error) {
	return m.model, nil
}

type autoSpanHTTPChunks struct {
	interfaces.ChunkService
	rows []*types.Chunk
}

func (s autoSpanHTTPChunks) ListChunksByKnowledgeID(context.Context, string) ([]*types.Chunk, error) {
	return s.rows, nil
}

func TestAutoTagActualAsynqBeginModelEndOriginalSpanHTTP(t *testing.T) {
	for _, mutation := range []string{
		"none",
		"unselected name ABA",
		"relationship ABA",
		"source withdrawal after model",
		"source withdrawal before HTTP",
		"remote revoke before HTTP",
		"source withdraw during HTTP remote",
		"API revoked after model",
		"API revoked before HTTP",
	} {
		t.Run(mutation, func(t *testing.T) {
			change := "auto-source"
			if mutation == "source withdraw during HTTP remote" {
				change = "source_withdraw_after_auto_final_remote"
			}
			if mutation == "remote revoke before HTTP" {
				change = "personal_revoke_after_auto"
			}
			h, db, store, sourceGuard, calls := collectionNextcloudHTTPStateFixture(
				t,
				change,
				func(db *gorm.DB, store *repository.KnowledgeInputProvenanceStore) {
					require.NoError(t, db.AutoMigrate(&types.Tenant{}, &types.Model{}))
					for _, name := range []string{
						"000056_knowledge_original_worker_actor",
						"000057_processing_span_original_inputs",
						"000062_knowledge_enrichment_inputs",
					} {
						if name == "000057_processing_span_original_inputs" {
							require.NoError(t, db.AutoMigrate(&types.KnowledgeProcessingSpan{}))
							require.NoError(
								t,
								db.Exec(
									("CREATE UNIQUE INDEX IF NOT EXISTS auto_span_http_unique ON knowledge_proce" +
										"ssing_spans(knowledge_id,attempt,span_id)"),
								).Error,
							)
						}
						data, err := os.ReadFile("../../migrations/sqlite/" + name + ".up.sql")
						require.NoError(t, err)
						require.NoError(t, db.Exec(string(data)).Error)
					}
					require.NoError(
						t,
						db.Create(&types.Tenant{ID: 7, Name: "actual auto HTTP tenant", Status: "active"}).Error,
					)
					require.NoError(
						t,
						db.Create(
							&types.Model{
								ID:       "isolated-model",
								TenantID: 7,
								Name:     "actual auto HTTP model",
								Source:   types.ModelSourceRemote,
								Status:   types.ModelStatusActive,
							},
						).Error,
					)
					repository.ConfigureKnowledgeInputActorGroupAccess(
						store,
						service.NewGroupAccessService(repository.NewGroupAccessRepository(db)),
					)
					var kb types.KnowledgeBase
					require.NoError(t, db.Where("id='kb'").Take(&kb).Error)
					kb.AutoTagConfig = &types.AutoTagConfig{Enabled: true, ModelID: "isolated-model", MaxTags: 2}
					require.NoError(
						t,
						store.UpdateKnowledgeBaseWithInput(
							repository.WithIndependentKnowledgeInput(collectionHumanInputContext(7)),
							&kb,
						),
					)
				},
			)
			ctx := collectionHumanInputContext(7)
			machine := mutation == "API revoked after model" || mutation == "API revoked before HTTP"
			tags := repository.NewKnowledgeTagRepository(db)
			knowledge := repository.NewKnowledgeRepository(db)
			factory := func(tx *gorm.DB) *repository.KnowledgeInputProvenanceStore {
				actual := repository.NewKnowledgeInputProvenanceStore(tx)
				repository.ConfigureKnowledgeInputActorGroupAccess(
					actual,
					service.NewGroupAccessService(repository.NewGroupAccessRepository(tx)),
				)
				return actual
			}
			repository.ConfigureKnowledgeDisplayInputWriters(store, tags, knowledge, factory)
			match := &types.KnowledgeTag{
				ID:              "auto-span-match",
				TenantID:        7,
				KnowledgeBaseID: "kb",
				Name:            "ordinary selected candidate",
				SortOrder:       0,
			}
			require.NoError(
				t,
				tags.Create(repository.WithIndependentKnowledgeTagCreateInput(ctx, "kb", match.Name, "", 0), match),
			)
			sourced, err := store.CaptureKnowledgeOutputInputs(
				ctx,
				types.KnowledgeBuildInputCaptureRequest{TenantID: 7, KnowledgeBaseID: "kb", KnowledgeID: "source"},
			)
			require.NoError(t, err)
			derived, err := repository.WithDerivedKnowledgeDisplayInput(ctx, sourced.Proof)
			require.NoError(t, err)
			nonmatch := &types.KnowledgeTag{
				ID:              "auto-span-nonmatch",
				TenantID:        7,
				KnowledgeBaseID: "kb",
				Name:            "unselected derived candidate",
				SortOrder:       1,
			}
			if machine {
				derived = repository.WithIndependentKnowledgeTagCreateInput(ctx, "kb", nonmatch.Name, "", 1)
			}
			require.NoError(t, tags.Create(derived, nonmatch))
			var apiKey *types.TenantAPIKey
			if machine {
				tenant := uint64(7)
				apiKey = &types.TenantAPIKey{
					ID:               41,
					TenantID:         &tenant,
					Name:             "actual-auto-api",
					ScopeType:        types.APIKeyScopeTenant,
					KeyHash:          "actual-auto-api-key",
					KnowledgeBaseIDs: types.StringArray{"kb"},
					Capabilities: types.StringArray{
						string(types.APIKeyCapabilityIngest),
						string(types.APIKeyCapabilityRetrieve),
					},
				}
				require.NoError(t, db.Create(apiKey).Error)
				ctx = types.WithCaller(context.Background(), types.Caller{TenantID: 7, Role: types.TenantRoleViewer})
				ctx = types.WithPrincipal(ctx, types.Principal{Type: types.PrincipalAPITenant, ID: "7"})
				ctx = types.WithTenantAPIKeyScope(
					ctx,
					types.TenantAPIKeyScope{
						KeyID:            apiKey.ID,
						KnowledgeBaseIDs: apiKey.KnowledgeBaseIDs,
						Capabilities:     apiKey.Capabilities,
					},
				)
				ctx = types.WithExecutionTenant(ctx, 7)
			}
			k := &types.Knowledge{
				ID:              "auto-span-document",
				TenantID:        7,
				KnowledgeBaseID: "kb",
				Type:            types.KnowledgeTypeManual,
				Source:          "manual",
				Channel:         "web",
				FileName:        "ordinary original input",
				ParseStatus:     types.ParseStatusFinalizing,
				EnableStatus:    "enabled",
				Metadata:        types.JSON(`{"manual":{"content":"ordinary actual input"}}`),
				CustomMetadata:  types.JSON(`{}`),
			}
			original, err := store.CreateKnowledgeWithInput(
				repository.WithIndependentKnowledgeInput(ctx),
				k,
				"ordinary actual input",
			)
			require.NoError(t, err)
			build := repository.WithKnowledgeBuildInputProof(ctx, store, original)
			chunk := &types.Chunk{
				ID:              "auto-span-chunk",
				TenantID:        7,
				KnowledgeBaseID: "kb",
				KnowledgeID:     k.ID,
				Content:         "actual classification body",
				ChunkType:       types.ChunkTypeText,
				IsEnabled:       true,
			}
			require.NoError(t, repository.NewChunkRepository(db).CreateChunks(build, []*types.Chunk{chunk}))
			tracker := service.NewSpanTracker(repository.NewKnowledgeSpanRepository(db), nil)
			root, attempt, err := tracker.OpenAttempt(build, k.ID, "actual-auto-http-trace")
			require.NoError(t, err)
			require.NotNil(t, root)
			require.NotNil(
				t,
				tracker.BeginStage(
					build,
					k.ID,
					attempt,
					types.StagePostProcess,
					types.JSONMap{"stage": "actual source-independent pipeline"},
				),
			)
			actual, err := store.CaptureKnowledgeBuildInputs(
				build,
				types.KnowledgeBuildInputCaptureRequest{
					TenantID:        7,
					KnowledgeBaseID: "kb",
					KnowledgeID:     k.ID,
					ChunkIDs:        []string{chunk.ID},
					Expected:        original,
				},
			)
			require.NoError(t, err)
			queued := types.KnowledgeAutoTagPayload{
				TenantID:        7,
				KnowledgeBaseID: "kb",
				KnowledgeID:     k.ID,
				Attempt:         attempt,
				OriginalInput:   actual.Proof,
			}
			queued.TaskInput, err = store.AdmitKnowledgeAutoTagTaskInput(build, queued)
			require.NoError(t, err)
			payload, err := json.Marshal(queued)
			require.NoError(t, err)
			model := &autoSpanHTTPModel{}
			if mutation == "unselected name ABA" {
				model.after = func() {
					require.NoError(
						t,
						db.Model(&types.KnowledgeTag{}).
							Where("id=?", nonmatch.ID).
							Update("name", "changed unselected").
							Error,
					)
					require.NoError(
						t,
						db.Model(&types.KnowledgeTag{}).Where("id=?", nonmatch.ID).Update("name", nonmatch.Name).Error,
					)
				}
			}
			if mutation == "relationship ABA" {
				model.after = func() {
					require.NoError(
						t,
						db.Create(&types.KnowledgeTagRelation{KnowledgeID: k.ID, TagID: nonmatch.ID}).Error,
					)
					require.NoError(
						t,
						db.Where("knowledge_id=? AND tag_id=?", k.ID, nonmatch.ID).
							Delete(&types.KnowledgeTagRelation{}).
							Error,
					)
				}
			}
			withdraw := func() {
				require.NoError(
					t,
					repository.NewNextcloudContentLeaseStore(db).
						RetireKnowledge(context.Background(), repository.NextcloudContentScope{
							TenantID:        7,
							KnowledgeBaseID: "kb",
							KnowledgeID:     "source",
							DataSourceID:    "ds",
							ExternalID:      "nextcloud:instance:77",
						}),
				)
			}
			revokeAPI := func() {
				now := time.Now()
				require.NoError(t, db.Model(
					apiKey,
				).Update("revoked_at", &now).Error)
			}
			if mutation == "API revoked after model" {
				model.after = revokeAPI
			}
			if mutation == "source withdrawal after model" {
				model.after = withdraw
			}

			taskHandler := service.NewKnowledgeAutoTagService(
				knowledge,
				tags,
				collectionHTTPKB{db: db},
				autoSpanHTTPChunks{rows: []*types.Chunk{chunk}},
				autoSpanHTTPModels{model: model},
				tracker,
			)
			service.ConfigureKnowledgeAutoTagInputProvenance(taskHandler, store, sourceGuard)
			err = taskHandler.Handle(context.Background(), asynq.NewTask(types.TypeKnowledgeAutoTag, payload))
			if mutation == "none" || mutation == "source withdrawal before HTTP" ||
				mutation == "remote revoke before HTTP" ||
				mutation == "API revoked before HTTP" ||
				mutation == "source withdraw during HTTP remote" {
				require.NoError(t, err)
				require.Equal(t, 1, model.calls)
			} else if mutation != "remote revoke before HTTP" {
				require.Error(t, err)
				require.Equal(t, 1, model.calls)
			}
			if mutation == "API revoked before HTTP" {
				revokeAPI()
			}
			if mutation == "source withdrawal before HTTP" {
				withdraw()
			}
			h.kgService = publicationHandlerKnowledge{row: k}
			h.spanRepo = repository.NewKnowledgeSpanRepository(db)
			ConfigureKnowledgeHandlerSpanOutputLineage(h, service.NewSpanOutputAuthorizer(store, sourceGuard))
			router := documentHandlerRouter()
			router.Use(func(c *gin.Context) {
				readCtx := collectionHumanInputContext(7)
				if machine {
					readCtx = ctx
				}
				c.Request = c.Request.WithContext(readCtx)
			})
			router.GET("/knowledge/:id/spans", h.GetKnowledgeSpans)
			w := httptest.NewRecorder()
			router.ServeHTTP(w, httptest.NewRequest("GET", "/knowledge/auto-span-document/spans?attempt=1", nil))
			if mutation == "none" {
				require.Equal(t, 200, w.Code, w.Body.String())
				require.Contains(t, w.Body.String(), "postprocess.auto_tag")
				require.Contains(t, w.Body.String(), `"matched_tag_count":1`)
				require.Contains(t, w.Body.String(), `"content_provenance":"complete"`)
				require.NotContains(t, w.Body.String(), "manifest_id")
				require.NotContains(t, w.Body.String(), "actual_inputs")
				require.Greater(t, calls.Load(), int64(0))
			} else {
				require.NotEqual(t, 200, w.Code, w.Body.String())
				require.NotContains(t, w.Body.String(), "matched_tag_count")
				require.NotContains(t, w.Body.String(), "actual-auto-http-trace")
			}
		})
	}
}
