package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/application/service"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
)

func TestFAQImportActualHTTPProgressResultAndDisplayToggle(t *testing.T) {
	h, _, db, store, queue, _, ctx := faqActualHTTPFixture(t)
	w := serveFAQHTTP(
		h,
		ctx,
		http.MethodPost,
		"/knowledge-bases/kb/faq/entries",
		`{"mode":"append","entries":[{"standard_question":"actual progress source question","answers":["actual imported answer"]}]}`,
	)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	require.Len(t, queue.tasks, 1)
	var task types.FAQImportPayload
	require.NoError(t, json.Unmarshal(queue.tasks[0].Payload(), &task))
	w = serveFAQHTTP(h, ctx, http.MethodGet, "/faq/import/progress/"+task.TaskID, "")
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	require.Contains(t, w.Body.String(), "pending")
	require.NoError(t, h.knowledgeService.ProcessFAQImport(context.Background(), queue.tasks[0]))
	w = serveFAQHTTP(h, ctx, http.MethodGet, "/faq/import/progress/"+task.TaskID, "")
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	require.Contains(t, w.Body.String(), `"success_count":1`)
	require.Contains(t, w.Body.String(), "actual progress source question")
	result, err := store.CaptureFAQImportOutputInputs(
		ctx,
		types.FAQImportOutputReadRequest{
			TenantID:        7,
			KnowledgeBaseID: "kb",
			KnowledgeID:     task.KnowledgeID,
			Kind:            "result",
		},
	)
	require.NoError(t, err)
	require.Equal(t, 1, result.Result.SuccessCount)
	w = serveFAQHTTP(
		h,
		ctx,
		http.MethodPut,
		"/knowledge-bases/kb/faq/import/last-result/display",
		`{"display_status":"close"}`,
	)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	w = serveFAQHTTP(h, ctx, http.MethodGet, "/faq/import/progress/"+task.TaskID, "")
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	require.Contains(t, w.Body.String(), `"display_status":"close"`)
	require.Contains(t, w.Body.String(), `"success_count":1`)
	current, err := store.CaptureFAQImportOutputInputs(
		ctx,
		types.FAQImportOutputReadRequest{
			TenantID:        7,
			KnowledgeBaseID: "kb",
			KnowledgeID:     task.KnowledgeID,
			Kind:            "result",
		},
	)
	require.NoError(t, err)
	var knowledge types.Knowledge
	require.NoError(t, db.Where("id=?", task.KnowledgeID).Take(&knowledge).Error)
	legacy := knowledge.LastFAQImportResult
	require.NoError(
		t,
		db.Model(&knowledge).
			Update("last_faq_import_result", types.JSON(`{"task_id":"foreign","success_count":999,"display_status":"open"}`)).
			Error,
	)
	require.NoError(t, db.Model(&knowledge).Update("last_faq_import_result", legacy).Error)
	require.Error(t, store.RecheckFAQImportOutputInputs(ctx, current))
	w = serveFAQHTTP(
		h,
		ctx,
		http.MethodPut,
		"/knowledge-bases/kb/faq/import/last-result/display",
		`{"display_status":"open"}`,
	)
	require.NotEqual(t, http.StatusOK, w.Code, w.Body.String())
}

func TestFAQImportActualHTTPCurrentKeyAndKBReadWithdraw(t *testing.T) {
	for _, change := range []string{"key-revoked", "key-capability", "key-kb-scope", "kb-policy", "membership"} {
		t.Run(change, func(t *testing.T) {
			h, _, db, _, queue, _, ctx := faqActualHTTPFixture(t)
			w := serveFAQHTTP(
				h,
				ctx,
				http.MethodPost,
				"/knowledge-bases/kb/faq/entries",
				`{"mode":"append","entries":[{"standard_question":"current read protected question","answers":["original read protected answer"]}]}`,
			)
			require.Equal(t, http.StatusOK, w.Code, w.Body.String())
			var task types.FAQImportPayload
			require.NoError(t, json.Unmarshal(queue.tasks[0].Payload(), &task))
			require.NoError(t, h.knowledgeService.ProcessFAQImport(context.Background(), queue.tasks[0]))
			var key *types.TenantAPIKey
			if change != "membership" {
				tenant := uint64(7)
				key = &types.TenantAPIKey{
					TenantID:         &tenant,
					ScopeType:        types.APIKeyScopeTenant,
					Name:             "progress read key",
					KeyHash:          "progress-key",
					Capabilities:     types.StringArray{string(types.APIKeyCapabilityRetrieve)},
					KnowledgeBaseIDs: types.StringArray{"kb"},
				}
				require.NoError(t, db.Create(key).Error)
				ctx = types.WithCaller(ctx, types.Caller{TenantID: 7, Role: types.TenantRoleViewer})
				ctx = types.WithPrincipal(ctx, types.Principal{Type: types.PrincipalAPITenant, ID: "7"})
				ctx = types.WithTenantAPIKeyScope(
					ctx,
					types.TenantAPIKeyScope{
						KeyID:            key.ID,
						ScopeType:        key.ScopeType,
						Capabilities:     key.Capabilities,
						KnowledgeBaseIDs: key.KnowledgeBaseIDs,
					},
				)
			}
			w = serveFAQHTTP(h, ctx, http.MethodGet, "/faq/import/progress/"+task.TaskID, "")
			require.Equal(t, http.StatusOK, w.Code, w.Body.String())
			require.Contains(t, w.Body.String(), "current read protected question")
			switch change {
			case "key-revoked":
				require.NoError(t, db.Model(key).Update("revoked_at", time.Now()).Error)
			case "key-capability":
				require.NoError(
					t,
					db.Model(key).Update("capabilities", types.StringArray{string(types.APIKeyCapabilityIngest)}).Error,
				)
			case "key-kb-scope":
				require.NoError(t, db.Model(key).Update("knowledge_base_ids", types.StringArray{"elsewhere"}).Error)
			case "kb-policy":
				require.NoError(
					t,
					db.Create(
						&types.ResourceAccessPolicy{
							ID:           "progress-restricted",
							TenantID:     7,
							ResourceType: types.GroupResourceTypeKnowledgeBase,
							ResourceID:   "kb",
							Mode:         types.ResourceAccessRestricted,
						},
					).Error,
				)
			case "membership":
				require.NoError(
					t,
					db.Model(&types.TenantMember{}).
						Where("user_id='user' AND tenant_id=7").
						Update("status", types.TenantMemberStatusSuspended).
						Error,
				)
			}
			w = serveFAQHTTP(h, ctx, http.MethodGet, "/faq/import/progress/"+task.TaskID, "")
			require.NotEqual(t, http.StatusOK, w.Code, w.Body.String())
			require.NotContains(t, w.Body.String(), "current read protected question")
		})
	}
}

func TestFAQImportActualKnowledgeCollectionLastResultReceipt(t *testing.T) {
	h, _, db, store, queue, _, ctx := faqActualHTTPFixture(t)
	w := serveFAQHTTP(
		h,
		ctx,
		http.MethodPost,
		"/knowledge-bases/kb/faq/entries",
		`{"mode":"append","entries":[{"standard_question":"last result original question","answers":["original"]}]}`,
	)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	var task types.FAQImportPayload
	require.NoError(t, json.Unmarshal(queue.tasks[0].Payload(), &task))
	require.NoError(t, h.knowledgeService.ProcessFAQImport(context.Background(), queue.tasks[0]))
	// The reused fixture's old document admissions belong to its previous KB
	// config and must not contaminate this new FAQ collection positive.
	require.NoError(t, db.Where("tenant_id=7 AND type<>?", types.KnowledgeTypeFAQ).Delete(&types.Knowledge{}).Error)
	_, err := store.CaptureKnowledgeCollectionInputs(
		ctx,
		types.KnowledgeCollectionReadRequest{Scopes: []types.KnowledgeSearchScope{{TenantID: 7, KBID: "kb"}}},
	)
	require.NoError(t, err)
	kb := collectionHTTPKB{db: db}
	groups := service.NewGroupAccessService(repository.NewGroupAccessRepository(db))
	guard := access.NewNextcloudSourceLineageGuard(nil, kb, nil, nil, groups, nil, store)
	kg := &KnowledgeHandler{kgService: h.knowledgeService, kbService: kb}
	ConfigureKnowledgeHandlerContentLeaseStore(kg, store.ContentLeaseStore())
	ConfigureKnowledgeHandlerCollectionOutput(kg, service.NewKnowledgeCollectionOutputAuthorizer(store, guard))
	w = serveCollectionHTTP(kg, "/knowledge-bases/kb/knowledge?page_size=20", false)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	require.Contains(t, w.Body.String(), `"last_faq_import_result"`)
	require.Contains(t, w.Body.String(), task.TaskID)
	require.Contains(t, w.Body.String(), `"success_count":1`)
	require.NoError(
		t,
		db.Exec("DELETE FROM faq_import_output_heads WHERE output_key=?", "result:7:"+task.KnowledgeID).Error,
	)
	w = serveCollectionHTTP(kg, "/knowledge-bases/kb/knowledge?page_size=20", false)
	require.NotEqual(t, http.StatusOK, w.Code, w.Body.String())
	require.NotContains(t, w.Body.String(), task.TaskID)
}

func TestFAQImportActualHTTPCurrentActorAndMissingLegacyReceiptReject(t *testing.T) {
	h, _, db, _, queue, _, ctx := faqActualHTTPFixture(t)
	w := serveFAQHTTP(
		h,
		ctx,
		http.MethodPost,
		"/knowledge-bases/kb/faq/entries",
		`{"mode":"append","entries":[{"standard_question":"protected progress question","answers":["known original"]}]}`,
	)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	var task types.FAQImportPayload
	require.NoError(t, json.Unmarshal(queue.tasks[0].Payload(), &task))
	require.NoError(t, h.knowledgeService.ProcessFAQImport(context.Background(), queue.tasks[0]))
	require.NoError(t, db.Model(&types.User{}).Where("id='user'").Update("is_active", false).Error)
	w = serveFAQHTTP(h, ctx, http.MethodGet, "/faq/import/progress/"+task.TaskID, "")
	require.NotEqual(t, http.StatusOK, w.Code, w.Body.String())
	require.NotContains(t, w.Body.String(), "protected progress question")
	require.NoError(t, db.Model(&types.User{}).Where("id='user'").Update("is_active", true).Error)
	// A current metadata object has no right to create its missing original
	// result fact, even when the actor supplies a valid new display edit.
	require.NoError(
		t,
		db.Exec("DELETE FROM faq_import_output_heads WHERE output_key=?", "result:7:"+task.KnowledgeID).Error,
	)
	w = serveFAQHTTP(
		h,
		ctx,
		http.MethodPut,
		"/knowledge-bases/kb/faq/import/last-result/display",
		`{"display_status":"close"}`,
	)
	require.NotEqual(t, http.StatusOK, w.Code, w.Body.String())
}

func TestFAQImportActualMetadataPlanPinsHeadBeforeBodyCapture(t *testing.T) {
	h, _, _, store, queue, _, ctx := faqActualHTTPFixture(t)
	w := serveFAQHTTP(
		h,
		ctx,
		http.MethodPost,
		"/knowledge-bases/kb/faq/entries",
		`{"mode":"append","entries":[{"standard_question":"plan original question","answers":["original"]}]}`,
	)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	var task types.FAQImportPayload
	require.NoError(t, json.Unmarshal(queue.tasks[0].Payload(), &task))
	plan, err := h.outputAuthorizer.ProgressReadPlan(ctx, task.TaskID)
	require.NoError(t, err)
	require.Equal(t, []types.KnowledgeSearchScope{{TenantID: 7, KBID: "kb"}}, plan.Scopes)
	unsafe, err := repository.WithFAQImportOutputReadPlan(ctx, plan)
	require.NoError(t, err)
	_, err = store.CaptureFAQImportOutputInputs(
		unsafe,
		types.FAQImportOutputReadRequest{TenantID: 7, TaskID: task.TaskID, Kind: "progress"},
	)
	require.Error(t, err)
	actualRead, err := acquireFAQReadPlanLeases(ctx, store.ContentLeaseStore(), plan.Scopes, plan.SourceLineage, 0)
	require.NoError(t, err)
	defer actualRead.Close()
	marked, err := repository.WithFAQImportOutputReadPlan(actualRead.Context(ctx), plan)
	require.NoError(t, err)
	_, err = store.CaptureFAQImportOutputInputs(
		marked,
		types.FAQImportOutputReadRequest{TenantID: 7, TaskID: task.TaskID, Kind: "progress"},
	)
	require.NoError(t, err)
	require.NoError(t, h.knowledgeService.ProcessFAQImport(context.Background(), queue.tasks[0]))
	_, err = store.CaptureFAQImportOutputInputs(
		marked,
		types.FAQImportOutputReadRequest{TenantID: 7, TaskID: task.TaskID, Kind: "progress"},
	)
	require.Error(t, err)
}

func TestFAQImportActualHTTPDryRunAndPartialFailureProgress(t *testing.T) {
	for _, dryRun := range []bool{false, true} {
		t.Run(fmt.Sprintf("dryrun=%v", dryRun), func(t *testing.T) {
			h, _, _, store, queue, _, ctx := faqActualHTTPFixture(t)
			batch := types.FAQBatchUpsertPayload{
				Mode:   types.FAQBatchModeAppend,
				DryRun: dryRun,
				Entries: []types.FAQEntryPayload{
					{StandardQuestion: "actual validation success", Answers: []string{"valid original"}},
					{StandardQuestion: "", Answers: []string{"failed original entry"}},
				},
			}
			body, err := json.Marshal(batch)
			require.NoError(t, err)
			w := serveFAQHTTP(h, ctx, http.MethodPost, "/knowledge-bases/kb/faq/entries", string(body))
			require.Equal(t, http.StatusOK, w.Code, w.Body.String())
			var task types.FAQImportPayload
			require.NoError(t, json.Unmarshal(queue.tasks[0].Payload(), &task))
			require.NoError(t, h.knowledgeService.ProcessFAQImport(context.Background(), queue.tasks[0]))
			w = serveFAQHTTP(h, ctx, http.MethodGet, "/faq/import/progress/"+task.TaskID, "")
			require.Equal(t, http.StatusOK, w.Code, w.Body.String())
			require.Contains(t, w.Body.String(), `"total":2`)
			require.Contains(t, w.Body.String(), `"failed_count":1`)
			require.Contains(t, w.Body.String(), `"status":"completed"`)
			result, err := store.CaptureFAQImportOutputInputs(
				ctx,
				types.FAQImportOutputReadRequest{
					TenantID:        7,
					KnowledgeBaseID: "kb",
					KnowledgeID:     task.KnowledgeID,
					Kind:            "result",
				},
			)
			if dryRun {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, 1, result.Result.FailedCount)
			}
		})
	}
}
