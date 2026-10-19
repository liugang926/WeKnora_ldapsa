package handler

import (
	"context"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/application/service"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

func spanHTTPFixture(
	t *testing.T,
) (*KnowledgeHandler, *service.SpanOutputAuthorizer, *gorm.DB, *types.Knowledge, *outputHTTPGroups, service.SpanTracker, context.Context) {
	t.Helper()
	h, db, k, groups := outputHTTPFixture(t, true)
	require.NoError(t, db.AutoMigrate(&types.KnowledgeProcessingSpan{}))
	require.NoError(
		t,
		db.Exec(
			"CREATE UNIQUE INDEX span_http_unique_tuple ON knowledge_processing_spans(knowledge_id,attempt,span_id)",
		).Error,
	)
	script, err := os.ReadFile("../../migrations/sqlite/000057_processing_span_original_inputs.up.sql")
	require.NoError(t, err)
	require.NoError(t, db.Exec(string(script)).Error)
	store := repository.NewKnowledgeInputProvenanceStore(db)
	repository.ConfigureKnowledgeInputActorGroupAccess(
		store,
		service.NewGroupAccessService(repository.NewGroupAccessRepository(db)),
	)
	actual, err := store.CaptureKnowledgeOutputInputs(
		context.Background(),
		types.KnowledgeBuildInputCaptureRequest{
			TenantID:        k.TenantID,
			KnowledgeBaseID: k.KnowledgeBaseID,
			KnowledgeID:     k.ID,
		},
	)
	require.NoError(t, err)
	ctx := repository.WithKnowledgeBuildInputProof(context.Background(), store, actual.Proof)
	var kb types.KnowledgeBase
	require.NoError(t, db.Where("id = ?", k.KnowledgeBaseID).Take(&kb).Error)
	guard := access.NewNextcloudSourceLineageGuard(nil, outputHTTPKB{row: &kb}, nil, nil, groups, nil, store)
	tracker := service.NewSpanTracker(repository.NewKnowledgeSpanRepository(db), nil)
	return h, service.NewSpanOutputAuthorizer(store, guard), db, k, groups, tracker, ctx
}

func serveSpanHTTP(h *KnowledgeHandler, a *service.SpanOutputAuthorizer, path string) *httptest.ResponseRecorder {
	gin.SetMode(gin.TestMode)
	router := documentHandlerRouter()
	router.Use(func(c *gin.Context) {
		ctx := types.WithCaller(c.Request.Context(), types.Caller{TenantID: 7, UserID: "user"})
		ctx = types.WithPrincipal(ctx, types.Principal{Type: types.PrincipalWebUser, ID: "user"})
		ctx = types.WithExecutionTenant(ctx, 7)
		c.Request = c.Request.WithContext(ctx)
	})
	ConfigureKnowledgeHandlerSpanOutputLineage(h, a)
	router.GET("/knowledge/:id/spans", h.GetKnowledgeSpans)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, httptest.NewRequest("GET", path, nil))
	return w
}

func recordActualSpanHTTP(
	ctx context.Context, t *testing.T,
	tracker service.SpanTracker,

	kid string,
) (*service.Span, *service.Span, int) {
	t.Helper()
	root, attempt, err := tracker.OpenAttempt(ctx, kid, "original-langfuse-trace")
	require.NoError(t, err)
	require.NotNil(t, root)
	stage := tracker.BeginStage(
		ctx,
		kid,
		attempt,
		types.StagePostProcess,
		types.JSONMap{"body": "original Start input"},
	)
	require.NotNil(t, stage)
	child := tracker.BeginSubSpan(
		ctx,
		stage,
		"summary.actual",
		types.SpanKindGeneration,
		types.JSONMap{"body": "actual summary input"},
	)
	require.NotNil(t, child)
	tracker.FailSpan(ctx, child, "MODEL_FAILED", "actual original model error", nil)
	tracker.EndSpan(ctx, stage, types.JSONMap{"body": "actual original stage output"})
	tracker.EndSpan(ctx, root, types.JSONMap{"body": "actual original attempt output"})
	return stage, child, attempt
}

func TestSpanOutputHTTPActualTrackerStartWorkEndRESTOriginalAttempt(t *testing.T) {
	h, a, _, k, _, tracker, ctx := spanHTTPFixture(t)
	_, _, attempt := recordActualSpanHTTP(ctx, t, tracker, k.ID)
	// The current document's recovery message has a different producer and
	// cannot authenticate the previous attempt's diagnostic body.
	k.ErrorMessage = "CURRENT recovery body must never certify old attempt"
	w := serveSpanHTTP(h, a, "/knowledge/document/spans?attempt=1")
	require.Equal(t, 200, w.Code, w.Body.String())
	require.Equal(t, 1, attempt)
	for _, body := range []string{"original Start input", "actual summary input", "actual original model error", "actual original stage output", "actual original attempt output", "original-langfuse-trace"} {
		require.Contains(t, w.Body.String(), body)
	}
	require.NotContains(t, w.Body.String(), "CURRENT recovery")
	require.NotContains(t, w.Body.String(), "admission_id")
	require.NotContains(t, w.Body.String(), "run_token")
	require.NotContains(t, w.Body.String(), "actual_inputs")
	require.Contains(t, w.Body.String(), `"content_provenance":"complete"`)
}

func TestSpanOutputHTTPUnknownLegacyAndUnknownBeginThenAdmittedEndStatusOnly(t *testing.T) {
	h, a, db, k, _, tracker, ctx := spanHTTPFixture(t)
	root, attempt, err := tracker.OpenAttempt(context.Background(), k.ID, "SECRET legacy trace link")
	require.NoError(t, err)
	stage := tracker.BeginStage(
		context.Background(),
		k.ID,
		attempt,
		types.StagePostProcess,
		types.JSONMap{"body": "SECRET legacy input"},
	)
	child := tracker.BeginSubSpan(
		context.Background(),
		stage,
		"SECRET dynamic span name",
		types.SpanKindGeneration,
		types.JSONMap{"body": "SECRET legacy child input"},
	)
	tracker.FailSpan(ctx, child, "SECRET legacy error code", "SECRET legacy error message", nil)
	tracker.EndSpan(ctx, stage, types.JSONMap{"body": "SECRET late output"})
	tracker.EndSpan(ctx, root, types.JSONMap{"body": "SECRET legacy root output"})
	require.NoError(
		t,
		db.Exec("UPDATE knowledge_processing_spans SET metadata='{\"trace_id\":\"SECRET raw link\"}'").Error,
	)
	w := serveSpanHTTP(h, a, "/knowledge/document/spans")
	require.Equal(t, 200, w.Code, w.Body.String())
	require.NotContains(t, w.Body.String(), "SECRET")
	require.Contains(t, w.Body.String(), `"content_provenance":"unknown"`)
	require.NotContains(t, w.Body.String(), `"last_error"`)
}

func TestSpanOutputHTTPFinalSetRecheckClosesWithdrawBodyABAInputConfigAndMembership(t *testing.T) {
	for _, mode := range []string{"grant", "span body ABA", "config ABA", "config afterremote", "input ABA", "membership"} {
		t.Run(mode, func(t *testing.T) {
			h, a, db, k, groups, tracker, ctx := spanHTTPFixture(t)
			_, _, _ = recordActualSpanHTTP(ctx, t, tracker, k.ID)
			groups.hook = func(n int) {
				boundary := 2
				if mode == "config afterremote" {
					boundary = 4
				}
				if n != boundary {
					return
				}
				switch mode {
				case "grant":
					groups.allowed = false
				case "span body ABA":
					require.NoError(
						t,
						db.Exec(
							"UPDATE knowledge_processing_spans SET error_message='changed' WHERE name='summary.actual'",
						).Error,
					)
					require.NoError(
						t,
						db.Exec(
							("UPDATE knowledge_processing_spans SET error_message='actual original model" +
								" error' WHERE name='summary.actual'"),
						).Error,
					)
				case "config ABA", "config afterremote":
					require.NoError(
						t,
						db.Exec("UPDATE knowledge_bases SET summary_model_id='changed' WHERE id='kb'").Error,
					)
					require.NoError(t, db.Exec("UPDATE knowledge_bases SET summary_model_id='' WHERE id='kb'").Error)
				case "input ABA":
					require.NoError(t, db.Exec("UPDATE knowledges SET metadata='{}' WHERE id='document'").Error)
					require.NoError(
						t,
						db.Model(&types.Knowledge{}).Where("id='document'").Update("metadata", k.Metadata).Error,
					)
				case "membership":
					require.NoError(
						t,
						db.Create(
							&types.KnowledgeProcessingSpan{
								KnowledgeID: k.ID,
								Attempt:     1,
								SpanID:      "late-unknown",
								Name:        "SECRET new unknown input",
								Kind:        types.SpanKindSubSpan,
								Status:      types.SpanStatusDone,
							},
						).Error,
					)
				}
			}
			w := serveSpanHTTP(h, a, "/knowledge/document/spans")
			require.Equal(t, 403, w.Code, w.Body.String())
			require.NotContains(t, w.Body.String(), "original Start input")
			require.NotContains(t, w.Body.String(), "actual original model error")
			require.NotContains(t, w.Body.String(), "SECRET")
		})
	}
}

func TestSpanOutputHTTPUnknownStatusStillRequiresCurrentKBGrant(t *testing.T) {
	h, a, _, k, groups, tracker, _ := spanHTTPFixture(t)
	_, _, err := tracker.OpenAttempt(context.Background(), k.ID, "SECRET trace")
	require.NoError(t, err)
	groups.allowed = false
	w := serveSpanHTTP(h, a, "/knowledge/document/spans")
	require.Equal(t, 403, w.Code, w.Body.String())
	require.NotContains(t, w.Body.String(), "SECRET")
	require.NotContains(t, w.Body.String(), `"trace"`)
}
