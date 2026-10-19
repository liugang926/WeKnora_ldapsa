package handler

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/application/service"
	sessionhandler "github.com/Tencent/WeKnora/internal/handler/session"
	"github.com/Tencent/WeKnora/internal/middleware"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

type actualRewindHTTPWriter struct {
	*httptest.ResponseRecorder
	t           *testing.T
	db          *gorm.DB
	session     string
	source      string
	beforeJSON  func()
	observed    bool
	countWrites int
}

// JSON rendering asks for Header before its actual guarded body Write. This
// hook fires only after the production delete committed, while the original
// output guard still owns its leases; it cannot retract already written bytes.
func (w *actualRewindHTTPWriter) Header() http.Header {
	if w.beforeJSON != nil && !w.observed {
		var deleted int64
		require.NoError(w.t, w.db.Unscoped().Model(&types.Message{}).Where("session_id=? AND deleted_at IS NOT NULL", w.session).Count(&deleted).Error)
		if deleted == 2 {
			w.observed = true
			assertActualHTTPTailLeases(w.t, w.db, true, w.source)
			w.beforeJSON()
		}
	}
	return w.ResponseRecorder.Header()
}
func (w *actualRewindHTTPWriter) Write(body []byte) (int, error) {
	if bytes.Contains(body, []byte(`"deleted_messages":2`)) {
		w.countWrites++
		assertActualHTTPTailLeases(w.t, w.db, true, w.source)
	}
	return w.ResponseRecorder.Write(body)
}

func actualRewindHTTPMessages(t *testing.T, f *actualHTTPTailFixture) (*types.Message, *types.Message, *types.Message) {
	t.Helper()
	// Use the authoritative persisted timestamp for a reproducible composite
	// boundary; callers never supply a cached clock to the delete selector.
	var first types.Message
	require.NoError(t, f.db.Where("id=?", f.parent.ID).Take(&first).Error)
	user, err := f.messages.CreateMessage(f.ctx, &types.Message{SessionID: first.SessionID, RequestID: "actual-http-rewind-later", Role: "user", Content: "actual later original rewind question", IsCompleted: true, CreatedAt: first.CreatedAt.Add(time.Second), ExecutionContext: types.MessageExecutionContext{KnowledgeBaseIDs: []string{"kb"}}})
	require.NoError(t, err)
	answer, err := f.messages.CreateMessage(f.ctx, &types.Message{SessionID: first.SessionID, RequestID: user.RequestID, Role: "assistant", CreatedAt: first.CreatedAt.Add(2 * time.Second), ExecutionContext: user.ExecutionContext})
	require.NoError(t, err)
	answer.Content = "actual later original rewind answer"
	answer.SourceLineage, _ = types.NewCompleteSourceLineage()
	answer.IsCompleted = true
	require.NoError(t, f.messages.UpdateMessage(f.ctx, answer))
	return &first, user, answer
}

func serveActualRewindHTTP(t *testing.T, f *actualHTTPTailFixture, point string, beforeJSON func()) *actualRewindHTTPWriter {
	t.Helper()
	sessions := repository.NewSessionRepository(f.db)
	rows := repository.NewMessageRepository(f.db)
	repository.ConfigureMessageMaterialWriters(rows, f.store)
	rewind := service.NewSessionRewindServiceFromRepos(sessions, rows, nil, f.messages, nil, nil, nil, nil, service.NewSessionBusyGate())
	h := sessionhandler.NewHandler(nativeSavedReplaySessions{repo: sessions}, f.messages, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, rewind, nil)
	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.Use(middleware.ErrorHandler())
	router.Use(func(c *gin.Context) {
		c.Request = c.Request.WithContext(f.ctx)
		c.Set(types.TenantIDContextKey.String(), uint64(7))
		c.Set(types.RequestIDContextKey.String(), "actual-rewind-http-request")
		c.Next()
	})
	router.POST("/sessions/:session_id/rewind", h.RewindSession)
	request, err := json.Marshal(map[string]string{"message_id": point})
	require.NoError(t, err)
	w := &actualRewindHTTPWriter{ResponseRecorder: httptest.NewRecorder(), t: t, db: f.db, session: f.parent.SessionID, source: f.source, beforeJSON: beforeJSON}
	router.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/sessions/"+f.parent.SessionID+"/rewind", bytes.NewReader(request)))
	return w
}

func TestMessageRewindActualHTTPOriginalUserAssistantCutsAndFinalLeaseCleanup(t *testing.T) {
	for _, controlled := range []bool{false, true} {
		for _, role := range []string{"user", "assistant"} {
			name := role + "/plain"
			if controlled {
				name = role + "/signed_source"
			}
			t.Run(name, func(t *testing.T) {
				f := newActualHTTPTailFixture(t, controlled)
				first, user, answer := actualRewindHTTPMessages(t, f)
				point := first.ID
				if role == "user" {
					point = user.ID
				}
				w := serveActualRewindHTTP(t, f, point, nil)
				require.Equal(t, http.StatusOK, w.Code, w.Body.String())
				require.Equal(t, 1, w.countWrites, "actual final JSON counter body must be written with original leases held")
				require.Contains(t, w.Body.String(), `"deleted_messages":2`)
				require.Contains(t, w.Body.String(), `"workspace_reset":false`)
				require.Contains(t, w.Body.String(), `"reason":"NO_SANDBOX"`)
				require.NotContains(t, w.Body.String(), "CheckOriginalOutput")
				require.NotContains(t, w.Body.String(), "CloseOriginalOutput")
				var deleted int64
				require.NoError(t, f.db.Unscoped().Model(&types.Message{}).Where("id IN ? AND deleted_at IS NOT NULL", []string{user.ID, answer.ID}).Count(&deleted).Error)
				require.EqualValues(t, 2, deleted)
				var remaining int64
				require.NoError(t, f.db.Model(&types.Message{}).Where("session_id=?", first.SessionID).Count(&remaining).Error)
				require.EqualValues(t, 2, remaining)
				assertActualHTTPTailLeases(t, f.db, false, "")
			})
		}
	}
}

func TestMessageRewindActualHTTPFinalWriterRejectsActorSourceBodyAndControlABA(t *testing.T) {
	for _, mode := range []string{"actor_revoke", "source_revoke", "remaining_body_aba", "remaining_control_aba", "owner_aba", "deleted_body_aba", "deleted_control_aba", "http_cancel", "lease_expired"} {
		t.Run(mode, func(t *testing.T) {
			f := newActualHTTPTailFixture(t, mode == "source_revoke")
			first, user, answer := actualRewindHTTPMessages(t, f)
			var cancelHTTP context.CancelFunc
			f.ctx, cancelHTTP = context.WithCancel(f.ctx)
			defer cancelHTTP()
			w := serveActualRewindHTTP(t, f, user.ID, func() {
				switch mode {
				case "http_cancel":
					cancelHTTP()
				case "lease_expired":
					require.NoError(t, f.db.Table("nextcloud_content_leases").Where("kind='read' AND released_at_ms IS NULL").Update("expires_at_ms", int64(1)).Error)
				case "actor_revoke":
					require.NoError(t, f.db.Model(&types.User{}).Where("id='user'").Update("is_active", false).Error)
				case "source_revoke":
					f.revokeSource()
				case "remaining_body_aba":
					require.NoError(t, f.db.Model(&types.Message{}).Where("id=?", first.ID).Update("content", "foreign transient remaining body").Error)
					require.NoError(t, f.db.Model(&types.Message{}).Where("id=?", first.ID).Update("content", first.Content).Error)
				case "remaining_control_aba":
					require.NoError(t, f.db.Model(&types.Message{}).Where("id=?", first.ID).Update("created_at", first.CreatedAt.Add(time.Millisecond)).Error)
					require.NoError(t, f.db.Model(&types.Message{}).Where("id=?", first.ID).Update("created_at", first.CreatedAt).Error)
				case "owner_aba":
					require.NoError(t, f.db.Model(&types.Session{}).Where("id=?", first.SessionID).Update("user_id", "foreign transient output owner").Error)
					require.NoError(t, f.db.Model(&types.Session{}).Where("id=?", first.SessionID).Update("user_id", "user").Error)
				case "deleted_body_aba":
					require.NoError(t, f.db.Unscoped().Model(&types.Message{}).Where("id=?", answer.ID).Update("content", "foreign transient deleted body").Error)
					require.NoError(t, f.db.Unscoped().Model(&types.Message{}).Where("id=?", answer.ID).Update("content", answer.Content).Error)
				case "deleted_control_aba":
					require.NoError(t, f.db.Unscoped().Model(&types.Message{}).Where("id=?", answer.ID).Update("created_at", answer.CreatedAt.Add(time.Millisecond)).Error)
					require.NoError(t, f.db.Unscoped().Model(&types.Message{}).Where("id=?", answer.ID).Update("created_at", answer.CreatedAt).Error)
				}
			})
			require.True(t, w.observed, "race must occur after actual delete and before guarded JSON serialization")
			require.Zero(t, w.countWrites)
			require.NotContains(t, w.Body.String(), "deleted_messages")
			require.NotContains(t, w.Body.String(), `"success":true`)
			assertActualHTTPTailLeases(t, f.db, false, "")
		})
	}
}

func TestMessageRewindActualHTTPUnknownCheckpointRejectsBeforeDelete(t *testing.T) {
	f := newActualHTTPTailFixture(t, false)
	first, user, _ := actualRewindHTTPMessages(t, f)
	checkpoint, err := json.Marshal(&types.SandboxCheckpoint{SandboxID: "unknown-current-sandbox", CommitSHA: strings.Repeat("a", 40)})
	require.NoError(t, err)
	require.NoError(t, f.db.Model(&types.Message{}).Where("id=?", first.ID).Update("sandbox_checkpoint", string(checkpoint)).Error)
	w := serveActualRewindHTTP(t, f, user.ID, nil)
	require.NotEqual(t, http.StatusOK, w.Code, w.Body.String())
	require.NotContains(t, w.Body.String(), "deleted_messages")
	var deleted int64
	require.NoError(t, f.db.Unscoped().Model(&types.Message{}).Where("session_id=? AND deleted_at IS NOT NULL", first.SessionID).Count(&deleted).Error)
	require.Zero(t, deleted)
	assertActualHTTPTailLeases(t, f.db, false, "")
}
