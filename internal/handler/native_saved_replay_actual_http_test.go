package handler

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/application/service"
	chatpipeline "github.com/Tencent/WeKnora/internal/application/service/chat_pipeline"
	sessionhandler "github.com/Tencent/WeKnora/internal/handler/session"
	"github.com/Tencent/WeKnora/internal/middleware"
	"github.com/Tencent/WeKnora/internal/stream"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/alicebob/miniredis/v2"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

type nativeSavedReplaySessions struct {
	interfaces.SessionService
	repo interfaces.SessionRepository
}

func (s nativeSavedReplaySessions) GetSession(ctx context.Context, id string) (*types.Session, error) {
	return s.repo.Get(ctx, types.MustTenantIDFromContext(ctx), types.SessionOwnerIDFromContext(ctx), id)
}

type nativeSavedReplayRedis struct {
	interfaces.StreamManager
	reads int
}

func (s *nativeSavedReplayRedis) GetEvents(ctx context.Context, sessionID, messageID string, offset int) ([]interfaces.StreamEvent, int, error) {
	s.reads++
	return s.StreamManager.GetEvents(ctx, sessionID, messageID, offset)
}

type nativeSavedReplayFiles struct {
	interfaces.FileService
	beforeURL func()
}

type nativeSavedReplayFinalWriter struct {
	*httptest.ResponseRecorder
	t      *testing.T
	db     *gorm.DB
	source string
}

func (w nativeSavedReplayFinalWriter) Write(body []byte) (int, error) {
	if bytes.Contains(body, []byte(`"response_type":"complete"`)) {
		var live int64
		query := w.db.Table("nextcloud_content_leases").Where("kind='read' AND released_at_ms IS NULL")
		if w.source != "" {
			query = query.Where("knowledge_id=?", w.source)
		}
		require.NoError(w.t, query.Count(&live).Error)
		require.Positive(w.t, live, "actual final completion write must still own its material leases")
	}
	return w.ResponseRecorder.Write(body)
}

func (s nativeSavedReplayFiles) GetFileURL(context.Context, string) (string, error) {
	if s.beforeURL != nil {
		s.beforeURL()
	}
	return "https://isolated.example.test/replay-image.png", nil
}

func configureNativeSavedReplayMessages(t *testing.T, db *gorm.DB, store *repository.KnowledgeInputProvenanceStore, kbs interfaces.KnowledgeBaseService, knowledge interfaces.KnowledgeService, guard *access.NextcloudSourceLineageGuard) interfaces.MessageService {
	t.Helper()
	require.NoError(t, db.Exec("ALTER TABLE messages ADD COLUMN artifacts TEXT").Error)
	for _, name := range []string{"000023_message_artifacts_table.up.sql", "000026_message_artifacts_deleted_at.up.sql", "000070_message_original_materials.up.sql"} {
		ddl, err := os.ReadFile("../../migrations/sqlite/" + name)
		require.NoError(t, err)
		require.NoError(t, db.Exec(string(ddl)).Error)
	}
	require.NoError(t, db.Exec("CREATE TABLE im_channel_sessions(id TEXT,session_id TEXT,platform TEXT)").Error)
	sqlDB, err := db.DB()
	require.NoError(t, err)
	sqlDB.SetMaxOpenConns(1)
	sqlDB.SetMaxIdleConns(1)
	messages := service.NewMessageService(repository.NewMessageRepository(db), repository.NewSessionRepository(db), nil, kbs, knowledge, nil, nil, nil, nil)
	service.ConfigureMessageSourceLineage(messages, nil, &service.SessionForkService{}, &chatpipeline.EventManager{}, guard)
	service.ConfigureMessageMaterialProvenance(messages, store)
	return messages
}

func produceNativeSavedReplayMessage(t *testing.T, db *gorm.DB, messages interfaces.MessageService, ctx context.Context, content string, lineage *types.SourceLineage) *types.Message {
	t.Helper()
	session := &types.Session{TenantID: 7, UserID: types.CallerFromContext(ctx).UserID}
	require.NoError(t, db.Create(session).Error)
	question, err := messages.CreateMessage(ctx, &types.Message{SessionID: session.ID, RequestID: "native-original-turn", Role: "user", Content: "actual original native question", IsCompleted: true, ExecutionContext: types.MessageExecutionContext{KnowledgeBaseIDs: []string{"kb"}}})
	require.NoError(t, err)
	answer, err := messages.CreateMessage(ctx, &types.Message{SessionID: session.ID, RequestID: question.RequestID, Role: "assistant", ExecutionContext: question.ExecutionContext})
	require.NoError(t, err)
	answer.Content, answer.SourceLineage, answer.IsCompleted = content, lineage, true
	answer.AgentSteps = types.AgentSteps{{Thought: "actual original saved thought", ToolCalls: []types.ToolCall{{ID: "actual-tool", Name: "think", Args: map[string]interface{}{"thought": "actual original saved argument"}, Result: &types.ToolResult{Success: true, Output: "actual original saved result"}}}}}
	answer.KnowledgeReferences = types.References{{ID: "original-ref", KnowledgeID: "match", KnowledgeBaseID: "kb", Content: "actual original saved reference"}}
	require.NoError(t, messages.UpdateMessage(ctx, answer))
	return answer
}

func serveNativeSavedReplay(t *testing.T, db *gorm.DB, messages interfaces.MessageService, ctx context.Context, answer *types.Message, fileService interfaces.FileService, public bool) (*httptest.ResponseRecorder, *nativeSavedReplayRedis) {
	t.Helper()
	mini := miniredis.RunT(t)
	redis, err := stream.NewRedisStreamManager(mini.Addr(), "", "", 0, "native-replay-test", time.Minute)
	require.NoError(t, err)
	t.Cleanup(func() { _ = redis.Close() })
	// Neither unknown old frames nor a complete current source envelope can
	// authenticate these unrelated cached bytes. Actual replay must ignore both.
	for _, frame := range []interfaces.StreamEvent{
		{Type: types.ResponseTypeAnswer, Content: "UNKNOWN CACHED SECRET"},
		{Type: types.ResponseTypeToolResult, Content: "SOURCE ONLY CACHED SECRET", SourceLineage: answer.SourceLineage},
		{Type: types.ResponseTypeComplete, Done: true, SourceLineage: answer.SourceLineage, Data: map[string]interface{}{"final_content": "CACHED FINAL SECRET", "artifacts": []map[string]interface{}{{"file_name": "cached-unsafe.pdf"}}}},
	} {
		require.NoError(t, redis.AppendEvent(ctx, answer.SessionID, answer.ID, frame))
	}
	streams := &nativeSavedReplayRedis{StreamManager: redis}
	h := sessionhandler.NewHandler(
		nativeSavedReplaySessions{repo: repository.NewSessionRepository(db)},
		messages,
		nil, // suggestions
		streams,
		nil, // config
		nil, // knowledge bases
		nil, // agents
		nil, // tenants
		nil, // agent shares
		nil, // KB shares
		fileService,
		nil, // resource catalog
		nil, // storage resolver
		nil, // models
		nil, // document reader
		nil, // images
		nil, // temporary documents
		nil, // artifact collector
		nil, // workspace checkpoint
		nil, // sandbox lookup
		nil, // memory
		nil, // users
		nil, // members
		nil, // terminal
		nil, // browser skill
		nil, // desktop
		nil, // desktop tickets
		nil, // desktop last
		nil, // redis desktop state
		nil, // fork
		nil, // rewind
		nil, // groups
	)
	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.Use(middleware.ErrorHandler())
	router.Use(func(c *gin.Context) { c.Request = c.Request.WithContext(ctx); c.Next() })
	router.GET("/sessions/continue-stream/:session_id", h.ContinueStream)
	url := "/sessions/continue-stream/" + answer.SessionID + "?message_id=" + answer.ID
	if public {
		url += "&resource_urls=public"
	}
	w := httptest.NewRecorder()
	source := ""
	if answer.SourceLineage != nil && len(answer.SourceLineage.Sources) > 0 {
		source = answer.SourceLineage.Sources[0].KnowledgeID
	}
	router.ServeHTTP(nativeSavedReplayFinalWriter{ResponseRecorder: w, t: t, db: db, source: source}, httptest.NewRequest(http.MethodGet, url, nil))
	return w, streams
}

func nativeSavedReplayPlainFixture(t *testing.T) (*gorm.DB, *repository.KnowledgeInputProvenanceStore, interfaces.MessageService, context.Context) {
	t.Helper()
	parent, db, store, _, _, _ := collectionHTTPFixture(t)
	groups := service.NewGroupAccessService(repository.NewGroupAccessRepository(db))
	guard := access.NewNextcloudSourceLineageGuard(repository.NewKnowledgeRepository(db).(access.NextcloudLineageSnapshotLookup), parent.kbService, nil, nil, groups, nil, store)
	messages := configureNativeSavedReplayMessages(t, db, store, parent.kbService, parent.kgService, guard)
	ctx := context.WithValue(collectionHumanInputContext(7), types.UserIDContextKey, "user")
	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	t.Cleanup(cancel)
	return db, store, messages, ctx
}

func TestNativeSavedReplayActualPlainRedisReconnectAndLegacyReject(t *testing.T) {
	db, _, messages, ctx := nativeSavedReplayPlainFixture(t)
	empty, _ := types.NewCompleteSourceLineage()
	answer := produceNativeSavedReplayMessage(t, db, messages, ctx, "actual native saved answer", empty)
	w, streams := serveNativeSavedReplay(t, db, messages, ctx, answer, nil, false)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	for _, material := range []string{answer.Content, "actual original saved thought", "actual original saved argument", "actual original saved result", "actual original saved reference", `"final_content":"actual native saved answer"`, `"response_type":"complete"`} {
		require.Contains(t, w.Body.String(), material)
	}
	require.NotContains(t, w.Body.String(), "CACHED")
	require.NotContains(t, w.Body.String(), "cached-unsafe.pdf")
	require.Zero(t, streams.reads)
	var live int64
	require.NoError(t, db.Table("nextcloud_content_leases").Where("kind='read' AND released_at_ms IS NULL").Count(&live).Error)
	require.Zero(t, live)
	legacy := &types.Message{SessionID: answer.SessionID, RequestID: "legacy-turn", Role: "assistant", Content: "LEGACY CURRENT ROW SECRET", IsCompleted: true, SourceLineage: empty}
	require.NoError(t, db.Create(legacy).Error)
	w, streams = serveNativeSavedReplay(t, db, messages, ctx, legacy, nil, false)
	require.Equal(t, http.StatusServiceUnavailable, w.Code, w.Body.String())
	require.NotContains(t, w.Body.String(), "SECRET")
	require.Zero(t, streams.reads)
}

func TestNativeSavedReplayActualFinalWriterRejectsBodyABAAndCurrentActor(t *testing.T) {
	for _, change := range []string{"body_aba", "actor_revoke"} {
		t.Run(change, func(t *testing.T) {
			db, _, messages, ctx := nativeSavedReplayPlainFixture(t)
			empty, _ := types.NewCompleteSourceLineage()
			answer := produceNativeSavedReplayMessage(t, db, messages, ctx, "SAVED SECRET ![image](resource://xifDo7NTSL300Lp1goVutw)", empty)
			observed := false
			files := nativeSavedReplayFiles{beforeURL: func() {
				observed = true
				var live int64
				require.NoError(t, db.Table("nextcloud_content_leases").Where("kind='read' AND released_at_ms IS NULL").Count(&live).Error)
				require.Positive(t, live)
				if change == "body_aba" {
					require.NoError(t, db.Model(&types.Message{}).Where("id=?", answer.ID).Update("content", "foreign transient").Error)
					require.NoError(t, db.Model(&types.Message{}).Where("id=?", answer.ID).Update("content", answer.Content).Error)
				} else {
					require.NoError(t, db.Model(&types.User{}).Where("id='user'").Update("is_active", false).Error)
				}
			}}
			w, streams := serveNativeSavedReplay(t, db, messages, ctx, answer, files, true)
			require.True(t, observed)
			require.NotContains(t, w.Body.String(), "SAVED SECRET")
			require.NotContains(t, w.Body.String(), "CACHED")
			require.NotContains(t, w.Body.String(), `"response_type":"complete"`)
			require.Zero(t, streams.reads)
			var live int64
			require.NoError(t, db.Table("nextcloud_content_leases").Where("kind='read' AND released_at_ms IS NULL").Count(&live).Error)
			require.Zero(t, live)
		})
	}
}

func TestNativeSavedReplayActualSignedSourceAndFinalWriterRevoke(t *testing.T) {
	f := faqAllReadLeaseFixture(t, "personal_revoke_before_write")
	f.calls.Store(-10000)
	ctx := context.WithValue(f.ctx, types.UserIDContextKey, "user")
	messages := configureNativeSavedReplayMessages(t, f.db, f.store, f.knowledge.kbService, f.knowledge.kgService, f.guard)
	request := types.FAQInputReadRequest{TenantID: 7, KnowledgeBaseID: "faq-derived", ChunkIDs: []string{f.entry.ChunkID}}
	plan, err := f.store.PeekFAQInputReadPlan(ctx, request)
	require.NoError(t, err)
	inputLease, err := acquireFAQReadPlanLeases(ctx, f.store.ContentLeaseStore(), plan.Scopes, plan.SourceLineage, 0)
	require.NoError(t, err)
	readCtx, err := repository.WithFAQInputReadPlan(inputLease.Context(ctx), plan)
	require.NoError(t, err)
	actual, err := f.store.CaptureFAQInputs(readCtx, request)
	require.NoError(t, err)
	require.Len(t, actual.Chunks, 1)
	answer := produceNativeSavedReplayMessage(t, f.db, messages, readCtx, "CONTROLLED NATIVE SECRET: "+actual.Chunks[0].Content+" ![image](resource://xifDo7NTSL300Lp1goVutw)", actual.Proofs[0].SourceLineage)
	inputLease.Close()
	w, streams := serveNativeSavedReplay(t, f.db, messages, ctx, answer, nil, false)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	quotedContent, err := json.Marshal(answer.Content)
	require.NoError(t, err)
	for _, material := range []string{`"content":` + string(quotedContent), `"final_content":` + string(quotedContent), "actual original saved thought", "actual original saved argument", "actual original saved result", "actual original saved reference", `"response_type":"complete"`} {
		require.Contains(t, w.Body.String(), material)
	}
	require.NotContains(t, w.Body.String(), "CACHED")
	require.Zero(t, streams.reads)
	observed := false
	files := nativeSavedReplayFiles{beforeURL: func() {
		observed = true
		var exact int64
		require.NoError(t, f.db.Table("nextcloud_content_leases").Where("kind='read' AND released_at_ms IS NULL AND knowledge_id='source'").Count(&exact).Error)
		require.Positive(t, exact)
		f.calls.Store(2)
	}}
	w, streams = serveNativeSavedReplay(t, f.db, messages, ctx, answer, files, true)
	require.True(t, observed)
	require.NotContains(t, w.Body.String(), "CONTROLLED NATIVE SECRET")
	require.NotContains(t, w.Body.String(), "CACHED")
	require.NotContains(t, w.Body.String(), `"response_type":"complete"`)
	require.Zero(t, streams.reads)
	var live int64
	require.NoError(t, f.db.Table("nextcloud_content_leases").Where("kind='read' AND released_at_ms IS NULL").Count(&live).Error)
	require.Zero(t, live)
}
