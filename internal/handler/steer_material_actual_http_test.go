package handler

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestSteerOriginalMaterialActualHTTPAgentPromoteCanonicalAndRedisForgery(t *testing.T) {
	for _, controlled := range []bool{false, true} {
		name := "ordinary_new_legal_flow"
		if controlled {
			name = "retained_current_source"
		}
		t.Run(name, func(t *testing.T) { runActualSteerHTTPAgent(t, controlled, "positive") })
	}
}

func TestSteerOriginalMaterialActualHTTPAgentRevocationBeforeConsumeAndBeforeNextModel(t *testing.T) {
	for _, mode := range []string{"actor_before_consume", "control_aba_before_consume", "source_before_consume", "actor_after_persist", "control_aba_after_persist", "source_after_persist"} {
		t.Run(mode, func(t *testing.T) { runActualSteerHTTPAgent(t, strings.HasPrefix(mode, "source_"), mode) })
	}
}

func runActualSteerHTTPAgent(t *testing.T, controlled bool, mode string) {
	t.Helper()
	f := newActualSteerHTTPFixture(t, controlled)
	steerID := uuid.NewString()
	model := &actualSteerHTTPModel{t: t, f: f, steerID: steerID, ready: make(chan struct{}), resume: make(chan struct{})}
	t.Cleanup(model.release)
	h, observed, redis, raw := newActualSteerHTTPHandler(t, f, model)
	router := actualSteerHTTPRouter(f.ctx, h)
	w := &actualSteerHTTPWriter{ResponseRecorder: httptest.NewRecorder(), t: t, f: f, steerID: steerID}
	done := make(chan struct{})
	go func() {
		defer close(done)
		request := httptest.NewRequest(http.MethodPost, "/agent-chat/"+f.parent.SessionID, strings.NewReader(`{"query":"actual current user task","agent_id":"actual-steer-agent","agent_enabled":true,"summary_model_id":"actual-http-tail-model","disable_title":true}`))
		request.Header.Set("Content-Type", "application/json")
		router.ServeHTTP(w, request)
	}()
	select {
	case <-model.ready:
	case <-done:
		t.Fatalf("actual public AgentQA ended before its first local model call: %s", w.Body.String())
	case <-time.After(8 * time.Second):
		t.Fatal("actual public AgentQA did not reach the paused local model")
	}
	var run types.Message
	require.NoError(t, f.db.Where("session_id=? AND role='assistant' AND request_id<>?", f.parent.SessionID, f.parent.RequestID).Take(&run).Error)
	require.False(t, run.IsCompleted)
	mentions := types.MentionedItems{{ID: "human-literal-name", Type: "file", Name: "actual original typed mention", KBID: "kb", KBName: "actual original typed KB name"}}
	queued := actualSteerHTTPRequest(router, http.MethodPost, "/sessions/"+f.parent.SessionID+"/steer", map[string]interface{}{
		"query":                         "  original steer guidance\r\nsecond original line\rthird original line  ",
		"steer_id":                      steerID,
		"expected_assistant_message_id": run.ID,
		"mentioned_items":               mentions,
		"channel":                       "api",
		"delivery":                      "after",
	})
	require.Equal(t, http.StatusOK, queued.Code, queued.Body.String())
	require.Contains(t, queued.Body.String(), `"delivery":"after"`)
	var origin struct{ ID, InputBody string }
	require.NoError(t, f.db.Raw("SELECT id,input_body FROM steer_authored_origins WHERE steer_id=?", steerID).Scan(&origin).Error)
	require.Contains(t, origin.InputBody, "_original_body_version", "actual HTTP enqueue uses the body vault, including no-KB human input")
	require.NotContains(t, origin.InputBody, "original steer guidance")
	var scopeCount int64
	require.NoError(t, f.db.Table("original_body_lease_scopes s").Joins("JOIN original_body_field_heads h ON h.body_id=s.body_id AND h.source_table='steer_authored_origins'").Where("h.row_key=? AND s.tenant_id=7 AND s.knowledge_base_id=''", origin.ID).Count(&scopeCount).Error)
	require.EqualValues(t, 1, scopeCount, "human authored steer originals have a physical no-KB retention scope")
	var initialHead string
	require.NoError(t, f.db.Table("steer_control_heads").Select("head_id").Where("origin_id=?", origin.ID).Scan(&initialHead).Error)
	forgeActualSteerHTTPMirror(t, f, redis, raw, run.ID, steerID)
	pending := actualSteerHTTPRequest(router, http.MethodGet, "/sessions/"+f.parent.SessionID+"/steer", nil)
	require.Equal(t, http.StatusOK, pending.Code, pending.Body.String())
	var listed struct {
		Items []struct {
			Content, Delivery string
			MentionedItems    types.MentionedItems `json:"mentioned_items"`
		}
	}
	require.NoError(t, json.Unmarshal(pending.Body.Bytes(), &listed))
	require.Len(t, listed.Items, 1)
	require.Equal(t, actualSteerHTTPQuery, listed.Items[0].Content)
	require.Equal(t, "after", listed.Items[0].Delivery)
	require.Equal(t, mentions, listed.Items[0].MentionedItems)
	require.NotContains(t, pending.Body.String(), "FORGED REDIS")
	// Promotion changes only the exact SQL control head. A forged Redis query,
	// mention, channel and consumed user ID remain transport-only copies.
	promoted := actualSteerHTTPRequest(router, http.MethodPost, "/sessions/"+f.parent.SessionID+"/steer/"+steerID+"/inject", nil)
	require.Equal(t, http.StatusOK, promoted.Code, promoted.Body.String())
	require.Contains(t, promoted.Body.String(), `"delivery":"inject"`)
	var promotedHead string
	require.NoError(t, f.db.Table("steer_control_heads").Select("head_id").Where("origin_id=?", origin.ID).Scan(&promotedHead).Error)
	require.NotEqual(t, initialHead, promotedHead)
	forgeActualSteerHTTPMirror(t, f, redis, raw, run.ID, steerID)
	mutate := func(proof *types.SteerConsumptionProof) {
		switch {
		case strings.HasPrefix(mode, "actor_"):
			require.NoError(t, f.db.Model(&types.User{}).Where("id='user'").Update("is_active", false).Error)
		case strings.HasPrefix(mode, "source_"):
			f.revokeSource()
		case strings.HasPrefix(mode, "control_aba_"):
			current := promotedHead
			if proof != nil {
				current = proof.ResultHeadID
			}
			require.NoError(t, f.db.Table("steer_control_heads").Where("origin_id=?", origin.ID).Update("head_id", initialHead).Error)
			require.NoError(t, f.db.Table("steer_control_heads").Where("origin_id=?", origin.ID).Update("head_id", current).Error)
		}
	}
	if strings.HasSuffix(mode, "before_consume") {
		observed.beforeConsume = func(_ *types.SteerMaterialInput) { mutate(nil) }
	}
	if strings.HasSuffix(mode, "after_persist") {
		observed.afterConsume = func(_ *types.Message, proof *types.SteerConsumptionProof) { mutate(proof) }
	}
	model.release()
	select {
	case <-done:
	case <-time.After(8 * time.Second):
		t.Fatal("actual Agent producer or its final HTTP sender did not finish")
	}
	require.EqualValues(t, 1, observed.consumes.Load(), "one actual round drain consumes one exact admitted original")
	require.NotContains(t, w.Body.String(), "FORGED REDIS")
	var users []types.Message
	require.NoError(t, f.db.Where("session_id=? AND request_id=? AND role='user'", f.parent.SessionID, run.RequestID).Order("created_at").Find(&users).Error)
	var consumed int64
	require.NoError(t, f.db.Table("steer_consumption_origins").Where("origin_id=?", origin.ID).Count(&consumed).Error)
	if mode == "positive" {
		require.EqualValues(t, 2, model.calls.Load())
		require.Len(t, users, 2)
		require.EqualValues(t, 1, consumed)
		require.Equal(t, actualSteerHTTPQuery, users[1].Content)
		require.Equal(t, mentions, users[1].MentionedItems)
		require.Equal(t, "api", users[1].Channel)
		require.NotEqual(t, "FORGED REDIS USER ID", users[1].ID)
		require.NoError(t, users[1].SourceLineage.RequireComplete())
		require.Positive(t, w.finalWrites)
		require.Equal(t, 1, w.completeWrites)
		require.Contains(t, w.Body.String(), actualSteerHTTPAnswer)
		var final types.Message
		require.NoError(t, f.db.Where("id=?", run.ID).Take(&final).Error)
		require.True(t, final.IsCompleted)
		require.Equal(t, actualSteerHTTPAnswer, final.Content)
		require.Equal(t, f.parent.SourceLineage, final.SourceLineage, "original steer insertion retains the complete prior source union")
	} else {
		require.EqualValues(t, 1, model.calls.Load(), "the rejected exact consume capability poisons the next actual model round")
		require.NotContains(t, w.Body.String(), "original steer guidance", "rejected original input is never published as an injection")
		require.NotContains(t, w.Body.String(), actualSteerHTTPAnswer)
		require.Zero(t, w.finalWrites)
		if strings.HasSuffix(mode, "before_consume") {
			require.Len(t, users, 1, "revocation before consume cannot manufacture a user row from Redis")
			require.Zero(t, consumed)
		} else {
			require.Len(t, users, 2, "one actual atomic consume may exist before revocation, with no duplicate or forged user")
			require.EqualValues(t, 1, consumed)
			require.Equal(t, actualSteerHTTPQuery, users[1].Content)
			require.Equal(t, mentions, users[1].MentionedItems)
			require.Equal(t, "api", users[1].Channel)
		}
	}
	require.Eventually(t, func() bool {
		var physical, sources int64
		return f.db.Table("original_body_leases").Where("kind='read' AND released_at_ms IS NULL").Count(&physical).Error == nil && physical == 0 && f.db.Table("nextcloud_content_leases").Where("kind='read' AND released_at_ms IS NULL").Count(&sources).Error == nil && sources == 0
	}, 3*time.Second, 10*time.Millisecond, "actual producer and HTTP ownership close every source and original body read lease")
	assertActualSteerBodyLease(t, f.db, "", false)
	var completed types.Message
	require.NoError(t, f.db.Where("id=?", run.ID).Take(&completed).Error)
	var completedInputs int64
	require.NoError(t, f.db.Table("message_completion_input_manifests m").
		Joins("JOIN message_material_admissions a ON a.id=m.assistant_admission_id").
		Where("a.message_id=?", run.ID).Count(&completedInputs).Error)
	if mode == "positive" {
		require.True(t, completed.IsCompleted)
		require.EqualValues(t, 1, completedInputs, "the actual completed answer retains its exact initial and consumed original inputs")
	} else {
		require.False(t, completed.IsCompleted, "safe refusal must not manufacture a completed assistant")
		require.Zero(t, completedInputs, "refused original input cannot receive completion provenance")
	}
}

func TestSteerOriginalMaterialActualHTTPUnknownCacheCannotBecomeCurrentBody(t *testing.T) {
	f := newActualSteerHTTPFixture(t, false)
	model := &actualSteerHTTPModel{t: t, f: f, ready: make(chan struct{}), resume: make(chan struct{})}
	h, observed, redis, _ := newActualSteerHTTPHandler(t, f, model)
	router := actualSteerHTTPRouter(f.ctx, h)
	user, err := f.messages.CreateMessage(f.ctx, &types.Message{SessionID: f.parent.SessionID, RequestID: "actual-cache-only-running", Role: "user", Content: "actual current request", IsCompleted: true})
	require.NoError(t, err)
	run, err := f.messages.CreateMessage(f.ctx, &types.Message{SessionID: user.SessionID, RequestID: user.RequestID, Role: "assistant"})
	require.NoError(t, err)
	require.NoError(t, redis.SetLiveRun(f.ctx, run.SessionID, run.ID, run.RequestID))
	unknownID := uuid.NewString()
	lineage, _ := types.NewCompleteSourceLineage()
	require.NoError(t, redis.AppendSteerEvents(f.ctx, run.SessionID, run.ID, []interfaces.StreamEvent{{ID: unknownID, Type: types.ResponseTypeSteer, Content: "UNKNOWN CACHED STEER SECRET", SourceLineage: lineage, Data: map[string]interface{}{"delivery": "inject", "channel": "api", "user_message_id": user.ID, "original_admission_id": "copied-nonexistent-original"}}}))
	listed := actualSteerHTTPRequest(router, http.MethodGet, "/sessions/"+run.SessionID+"/steer", nil)
	require.Equal(t, http.StatusOK, listed.Code, listed.Body.String())
	var listedItems struct {
		Items []json.RawMessage `json:"items"`
	}
	require.NoError(t, json.Unmarshal(listed.Body.Bytes(), &listedItems))
	require.Empty(t, listedItems.Items, "unknown Redis input is not a current SQL-authored item")
	require.NotContains(t, listed.Body.String(), "UNKNOWN CACHED STEER SECRET")
	for _, request := range []struct{ Method, Path string }{{http.MethodPost, "/inject"}, {http.MethodDelete, ""}} {
		response := actualSteerHTTPRequest(router, request.Method, "/sessions/"+run.SessionID+"/steer/"+unknownID+request.Path, nil)
		require.NotEqual(t, http.StatusOK, response.Code, "unknown cache ID cannot receive a fresh SQL control admission")
		require.NotContains(t, response.Body.String(), "UNKNOWN CACHED STEER SECRET")
	}
	_, generation, err := observed.MessageNativeLiveIdentity(f.ctx, run.SessionID, run.ID)
	require.NoError(t, err)
	_, err = observed.AdmitActualSteerInput(f.ctx, &types.SteerAuthoredRequest{TenantID: 7, SessionID: run.SessionID, RunMessageID: run.ID, RunGenerationID: generation, RunRequestID: run.RequestID, ClientSteerID: unknownID, Query: "UNKNOWN CACHED STEER SECRET", SourceContext: f.ctx, ActorContext: f.ctx})
	require.Error(t, err, "cached bytes lack the exact actual HTTP input boundary even with a complete source envelope")
	var originals, consumed, fakeUsers int64
	require.NoError(t, f.db.Table("steer_authored_origins").Count(&originals).Error)
	require.NoError(t, f.db.Table("steer_consumption_origins").Count(&consumed).Error)
	require.NoError(t, f.db.Model(&types.Message{}).Where("content=?", "UNKNOWN CACHED STEER SECRET").Count(&fakeUsers).Error)
	require.Zero(t, originals)
	require.Zero(t, consumed)
	require.Zero(t, fakeUsers)
	require.Zero(t, model.calls.Load())
	assertActualSteerBodyLease(t, f.db, "", false)
	assertActualHTTPTailLeases(t, f.db, false, "")
}

func TestSteerOriginalMaterialActualHTTPDeleteUsesExactOriginalControl(t *testing.T) {
	f := newActualSteerHTTPFixture(t, false)
	model := &actualSteerHTTPModel{t: t, f: f, ready: make(chan struct{}), resume: make(chan struct{})}
	h, _, redis, raw := newActualSteerHTTPHandler(t, f, model)
	router := actualSteerHTTPRouter(f.ctx, h)
	user, err := f.messages.CreateMessage(f.ctx, &types.Message{SessionID: f.parent.SessionID, RequestID: "actual-deletion-running", Role: "user", Content: "actual current deletion request", IsCompleted: true})
	require.NoError(t, err)
	run, err := f.messages.CreateMessage(f.ctx, &types.Message{SessionID: user.SessionID, RequestID: user.RequestID, Role: "assistant"})
	require.NoError(t, err)
	require.NoError(t, redis.SetLiveRun(f.ctx, run.SessionID, run.ID, run.RequestID))
	steerID := uuid.NewString()
	queued := actualSteerHTTPRequest(router, http.MethodPost, "/sessions/"+run.SessionID+"/steer", map[string]interface{}{"query": "actual deletable original", "steer_id": steerID, "expected_assistant_message_id": run.ID, "delivery": "after"})
	require.Equal(t, http.StatusOK, queued.Code, queued.Body.String())
	forgeActualSteerHTTPMirror(t, f, redis, raw, run.ID, steerID)
	deleted := actualSteerHTTPRequest(router, http.MethodDelete, "/sessions/"+run.SessionID+"/steer/"+steerID, nil)
	require.Equal(t, http.StatusOK, deleted.Code, deleted.Body.String())
	require.Contains(t, deleted.Body.String(), `"removed":true`)
	require.NotContains(t, deleted.Body.String(), "FORGED REDIS")
	listed := actualSteerHTTPRequest(router, http.MethodGet, "/sessions/"+run.SessionID+"/steer", nil)
	require.Equal(t, http.StatusOK, listed.Code, listed.Body.String())
	require.Contains(t, listed.Body.String(), `"items":[]`)
	var control struct {
		Operation string
		Deleted   bool
		Revision  uint64
	}
	require.NoError(t, f.db.Table("steer_control_admissions c").Select("c.operation,c.deleted,c.revision").Joins("JOIN steer_control_heads h ON h.head_id=c.id").Where("c.steer_id=?", steerID).Take(&control).Error)
	require.Equal(t, "delete", control.Operation)
	require.True(t, control.Deleted)
	require.EqualValues(t, 2, control.Revision)
	var originals, consumed, users int64
	require.NoError(t, f.db.Table("steer_authored_origins").Where("steer_id=?", steerID).Count(&originals).Error)
	require.NoError(t, f.db.Table("steer_consumption_origins").Count(&consumed).Error)
	require.NoError(t, f.db.Model(&types.Message{}).Where("request_id=? AND role='user'", run.RequestID).Count(&users).Error)
	require.EqualValues(t, 1, originals)
	require.Zero(t, consumed)
	require.EqualValues(t, 1, users)
	require.Zero(t, model.calls.Load())
	assertActualSteerBodyLease(t, f.db, "", false)
	assertActualHTTPTailLeases(t, f.db, false, "")
}
