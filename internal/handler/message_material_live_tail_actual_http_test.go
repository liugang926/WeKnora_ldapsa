package handler

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/application/service"
	chatpipeline "github.com/Tencent/WeKnora/internal/application/service/chat_pipeline"
	"github.com/Tencent/WeKnora/internal/config"
	sessionhandler "github.com/Tencent/WeKnora/internal/handler/session"
	"github.com/Tencent/WeKnora/internal/middleware"
	"github.com/Tencent/WeKnora/internal/models/chat"
	"github.com/Tencent/WeKnora/internal/stream"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/alicebob/miniredis/v2"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

const actualHTTPTailAnswer = "LIVE HTTP SECRET ![image](resource://xifDo7NTSL300Lp1goVutw)"

type actualHTTPTailModel struct {
	t      *testing.T
	db     *gorm.DB
	parent *types.Message
	calls  atomic.Int64
}

func (m *actualHTTPTailModel) GetModelName() string { return "actual-local-http-tail" }
func (m *actualHTTPTailModel) GetModelID() string   { return "actual-http-tail-model" }
func (m *actualHTTPTailModel) Chat(context.Context, []chat.Message, *chat.ChatOptions) (*types.ChatResponse, error) {
	return nil, fmt.Errorf("this actual HTTP fixture must only use its local streaming channel")
}

func (m *actualHTTPTailModel) ChatStream(
	ctx context.Context,
	input []chat.Message,
	_ *chat.ChatOptions,
) (<-chan types.StreamResponse, error) {
	m.calls.Add(1)
	if err := types.CheckHistoryMaterialLifetime(ctx); err != nil {
		return nil, err
	}
	assertActualHTTPTailLeases(m.t, m.db, true, "")
	found := false
	for _, item := range input {
		if strings.Contains(item.Content, m.parent.Content) {
			found = true
		}
	}
	require.True(m.t, found, "actual model entry must consume the admitted saved original")
	out := make(chan types.StreamResponse, 1)
	out <- types.StreamResponse{ResponseType: types.ResponseTypeAnswer, Content: actualHTTPTailAnswer, Done: true}
	close(out)
	return out, nil
}

type actualHTTPTailModels struct {
	interfaces.ModelService
	model *actualHTTPTailModel
}

func (m actualHTTPTailModels) GetModelByID(context.Context, string) (*types.Model, error) {
	return &types.Model{
		ID:       "actual-http-tail-model",
		TenantID: 7,
		Type:     types.ModelTypeKnowledgeQA,
		Status:   types.ModelStatusActive,
	}, nil
}

func (m actualHTTPTailModels) GetChatModel(context.Context, string) (chat.Chat, error) {
	return m.model, nil
}

type actualHTTPTailFiles struct {
	interfaces.FileService
	once      sync.Once
	beforeURL func(context.Context)
	observed  atomic.Bool
}

func (f *actualHTTPTailFiles) GetFileURL(ctx context.Context, _ string) (string, error) {
	f.once.Do(func() {
		f.observed.Store(true)
		if f.beforeURL != nil {
			f.beforeURL(ctx)
		}
	})
	return "https://isolated.example.test/http-tail-image.png", nil
}

type actualHTTPTailWriter struct {
	*httptest.ResponseRecorder
	t                *testing.T
	db               *gorm.DB
	session          string
	parentRequest    string
	source           string
	answerWrites     int
	completionWrites int
	guardedFlushes   int
	answerWritten    bool
	lastDerivedWrite bool
	afterAnswerWrite func()
	afterOnce        sync.Once
}

func (w *actualHTTPTailWriter) Write(body []byte) (int, error) {
	answer := bytes.Contains(body, []byte(`"response_type":"answer"`)) &&
		bytes.Contains(body, []byte("LIVE HTTP SECRET"))
	complete := bytes.Contains(body, []byte(`"response_type":"complete"`))
	if bytes.Contains(body, []byte("data:")) {
		w.lastDerivedWrite = false
	}
	if answer || complete {
		w.lastDerivedWrite = true
	}
	if bytes.Contains(body, []byte(`"response_type":"error"`)) {
		w.lastDerivedWrite = false
	}
	if answer || complete {
		assertActualHTTPTailLeases(w.t, w.db, true, w.source)
	}
	if answer {
		w.answerWrites++
		w.answerWritten = true
	}
	if complete {
		w.completionWrites++
		var persisted int64
		require.NoError(
			w.t,
			w.db.Model(&types.Message{}).
				Where(
					"session_id=? AND role='assistant' AND request_id<>? AND is_completed=true",
					w.session,
					w.parentRequest,
				).
				Count(&persisted).
				Error,
		)
		require.EqualValues(
			w.t,
			1,
			persisted,
			"actual completed producer seal and control transfer must precede HTTP complete",
		)
	}
	n, err := w.ResponseRecorder.Write(body)
	if answer && w.afterAnswerWrite != nil {
		w.afterOnce.Do(w.afterAnswerWrite)
	}
	return n, err
}

func (w *actualHTTPTailWriter) Flush() {
	if w.lastDerivedWrite {
		assertActualHTTPTailLeases(w.t, w.db, true, w.source)
		w.guardedFlushes++
	}
	w.ResponseRecorder.Flush()
	w.lastDerivedWrite = false
}

func assertActualHTTPTailLeases(t *testing.T, db *gorm.DB, held bool, source string) {
	t.Helper()
	query := db.Table("nextcloud_content_leases").Where("kind='read' AND released_at_ms IS NULL")
	if source != "" {
		query = query.Where("knowledge_id=?", source)
	}
	var live int64
	require.NoError(t, query.Count(&live).Error)
	if held {
		require.Positive(t, live, "the actual model or final HTTP writer must still hold original source leases")
	} else {
		require.Eventually(
			t,
			func() bool {
				var pending int64
				return db.Table("nextcloud_content_leases").
					Where("kind='read' AND released_at_ms IS NULL").
					Count(&pending).
					Error == nil &&
					pending == 0
			},
			2*time.Second,
			10*time.Millisecond,
			("HTTP release must preserve an active producer until it finishes, then all " +
				"leases close"),
		)
	}
}

type actualHTTPTailFixture struct {
	db           *gorm.DB
	store        *repository.KnowledgeInputProvenanceStore
	messages     interfaces.MessageService
	kbs          interfaces.KnowledgeBaseService
	knowledge    interfaces.KnowledgeService
	guard        *access.NextcloudSourceLineageGuard
	ctx          context.Context
	parent       *types.Message
	revokeSource func()
	source       string
}

func newActualHTTPTailFixture(t *testing.T, controlled bool) *actualHTTPTailFixture {
	t.Helper()
	if !controlled {
		parent, db, store, _, _, _ := collectionHTTPFixture(t)
		guard := access.NewNextcloudSourceLineageGuard(
			repository.NewKnowledgeRepository(db).(access.NextcloudLineageSnapshotLookup),
			parent.kbService,
			nil,
			nil,
			service.NewGroupAccessService(repository.NewGroupAccessRepository(db)),
			nil,
			store,
		)
		messages := configureNativeSavedReplayMessages(t, db, store, parent.kbService, parent.kgService, guard)
		require.NoError(t, repository.UpgradeMessageProducerAdvanceSchema(db))
		ctx := context.WithValue(collectionHumanInputContext(7), types.UserIDContextKey, "user")
		ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
		t.Cleanup(cancel)
		empty, _ := types.NewCompleteSourceLineage()
		original := produceNativeSavedReplayMessage(ctx, t, db, messages, "ACTUAL LIVE PARENT SECRET", empty)
		return &actualHTTPTailFixture{
			db:        db,
			store:     store,
			messages:  messages,
			kbs:       parent.kbService,
			knowledge: parent.kgService,
			guard:     guard,
			ctx:       ctx,
			parent:    original,
		}
	}
	f := faqAllReadLeaseFixture(t, "personal_revoke_before_write")
	f.calls.Store(-10000)
	ctx := context.WithValue(f.ctx, types.UserIDContextKey, "user")
	messages := configureNativeSavedReplayMessages(
		t,
		f.db,
		f.store,
		f.knowledge.kbService,
		f.knowledge.kgService,
		f.guard,
	)
	require.NoError(t, repository.UpgradeMessageProducerAdvanceSchema(f.db))
	request := types.FAQInputReadRequest{
		TenantID:        7,
		KnowledgeBaseID: "faq-derived",
		ChunkIDs:        []string{f.entry.ChunkID},
	}
	plan, err := f.store.PeekFAQInputReadPlan(ctx, request)
	require.NoError(t, err)
	inputLease, err := acquireFAQReadPlanLeases(ctx, f.store.ContentLeaseStore(), plan.Scopes, plan.SourceLineage, 0)
	require.NoError(t, err)
	readCtx, err := repository.WithFAQInputReadPlan(inputLease.Context(ctx), plan)
	require.NoError(t, err)
	original, err := f.store.CaptureFAQInputs(readCtx, request)
	require.NoError(t, err)
	require.Len(t, original.Chunks, 1)
	prior := produceNativeSavedReplayMessage(

		readCtx, t,
		f.db,
		messages,

		"ACTUAL LIVE PARENT SECRET: "+original.Chunks[0].Content,
		original.Proofs[0].SourceLineage,
	)
	inputLease.Close()
	return &actualHTTPTailFixture{
		db:           f.db,
		store:        f.store,
		messages:     messages,
		kbs:          f.knowledge.kbService,
		knowledge:    f.knowledge.kgService,
		guard:        f.guard,
		ctx:          ctx,
		parent:       prior,
		source:       "source",
		revokeSource: func() { f.calls.Store(2) },
	}
}

func serveActualHTTPTail(
	t *testing.T,
	f *actualHTTPTailFixture,
	beforeURL func(context.Context),
	afterWrite func(),
	cancelHTTP bool,
) (*actualHTTPTailWriter, *actualHTTPTailModel, *actualHTTPTailFiles) {
	t.Helper()
	model := &actualHTTPTailModel{t: t, db: f.db, parent: f.parent}
	models := actualHTTPTailModels{model: model}
	cfg := &config.Config{
		Conversation: &config.ConversationConfig{
			MaxRounds: 3,
			Summary: &config.SummaryConfig{
				Prompt:          "Actual operator configured plain chat prompt",
				ContextTemplate: "{{.Query}}",
			},
		},
	}
	manager := chatpipeline.NewEventManager()
	chatpipeline.ConfigureSourceLineage(manager, f.guard)
	chatpipeline.NewPluginLoadHistory(manager, f.messages, cfg)
	chatpipeline.NewPluginMemoryRecall(manager, nil)
	chatpipeline.NewPluginChatCompletionStream(manager, models)
	sessions := service.NewSessionService(
		cfg,
		repository.NewSessionRepository(f.db),
		repository.NewMessageRepository(f.db),
		nil,
		f.knowledge,
		nil,
		models,
		nil,
		manager,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		service.NewSessionBusyGate(),
		nil,
		nil,
	)
	service.ConfigureMessageSourceLineage(f.messages, sessions, &service.SessionForkService{}, manager, f.guard)
	mini := miniredis.RunT(t)
	redis, err := stream.NewRedisStreamManager(mini.Addr(), "", "", 0, "actual-http-tail", time.Minute)
	require.NoError(t, err)
	t.Cleanup(func() { _ = redis.Close() })
	httpCtx, requestCancel := context.WithCancel(f.ctx)
	defer requestCancel()
	files := &actualHTTPTailFiles{beforeURL: func(ctx context.Context) {
		assertActualHTTPTailLeases(t, f.db, true, f.source)
		require.NoError(
			t,
			service.CheckMessageMaterialOutputBoundary(ctx),
			"public URL rewriting must receive the same actual shared original holder",
		)
		if beforeURL != nil {
			beforeURL(ctx)
		}
		if cancelHTTP {
			requestCancel()
		}
	}}
	h := sessionhandler.NewHandler(
		sessions,
		f.messages,
		nil,
		redis,
		cfg,
		nil,
		nil,
		nil,
		nil,
		nil,
		files,
		nil,
		nil,
		models,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
	)
	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.Use(middleware.ErrorHandler())
	router.Use(func(c *gin.Context) {
		c.Request = c.Request.WithContext(httpCtx)
		c.Set(types.TenantIDContextKey.String(), uint64(7))
		c.Set(types.RequestIDContextKey.String(), "actual-live-http-request")
		c.Next()
	})
	router.POST("/knowledge-chat/:session_id", h.KnowledgeQA)
	w := &actualHTTPTailWriter{
		ResponseRecorder: httptest.NewRecorder(),
		t:                t,
		db:               f.db,
		session:          f.parent.SessionID,
		parentRequest:    f.parent.RequestID,
		source:           f.source,
		afterAnswerWrite: afterWrite,
	}
	router.ServeHTTP(
		w,
		httptest.NewRequest(
			http.MethodPost,
			"/knowledge-chat/"+f.parent.SessionID+"?resource_urls=public",
			strings.NewReader(
				("{\"query\":\"actual current human question\",\"summary_model_id\":\"actual"+
					"-http-tail-model\",\"disable_title\":true}"),
			),
		),
	)
	return w, model, files
}

func TestMessageMaterialActualLiveHTTPFinalPersistWriteFlushAndLeaseCleanup(t *testing.T) {
	for _, controlled := range []bool{false, true} {
		t.Run(map[bool]string{false: "plain", true: "signed_source"}[controlled], func(t *testing.T) {
			f := newActualHTTPTailFixture(t, controlled)
			w, model, files := serveActualHTTPTail(t, f, nil, nil, false)
			require.Equal(t, http.StatusOK, w.Code, w.Body.String())
			require.EqualValues(t, 1, model.calls.Load())
			require.True(t, files.observed.Load())
			require.Contains(t, w.Body.String(), "LIVE HTTP SECRET")
			require.Contains(t, w.Body.String(), "https://isolated.example.test/http-tail-image.png")
			require.Contains(t, w.Body.String(), `"response_type":"complete"`)
			require.Positive(t, w.answerWrites)
			require.Positive(t, w.completionWrites)
			require.Positive(t, w.guardedFlushes)
			assertActualHTTPTailLeases(t, f.db, false, "")
		})
	}
}

func TestMessageMaterialActualLiveHTTPRewriteRejectsBodyActorSourceAndCancel(t *testing.T) {
	for _, mode := range []string{"body_aba", "actor_revoke", "source_revoke", "http_cancel"} {
		t.Run(mode, func(t *testing.T) {
			f := newActualHTTPTailFixture(t, mode == "source_revoke")
			observed := false
			mutate := func(context.Context) {
				observed = true
				switch mode {
				case "body_aba":
					require.NoError(
						t,
						f.db.Model(&types.Message{}).
							Where("id=?", f.parent.ID).
							Update("content", "foreign transient parent body").
							Error,
					)
					require.NoError(
						t,
						f.db.Model(&types.Message{}).
							Where("id=?", f.parent.ID).
							Update("content", f.parent.Content).
							Error,
					)
				case "actor_revoke":
					require.NoError(t, f.db.Model(&types.User{}).Where("id='user'").Update("is_active", false).Error)
				case "source_revoke":
					f.revokeSource()
				}
			}
			w, model, files := serveActualHTTPTail(t, f, mutate, nil, mode == "http_cancel")
			require.True(t, observed)
			require.True(t, files.observed.Load())
			require.EqualValues(t, 1, model.calls.Load())
			require.NotContains(t, w.Body.String(), "LIVE HTTP SECRET")
			require.NotContains(t, w.Body.String(), `"response_type":"complete"`)
			require.Zero(t, w.answerWrites)
			require.Zero(t, w.completionWrites)
			assertActualHTTPTailLeases(t, f.db, false, "")
		})
	}
}

func TestMessageMaterialActualLiveHTTPRecheckBetweenWriteAndFlush(t *testing.T) {
	f := newActualHTTPTailFixture(t, true)
	observed := false
	w, model, _ := serveActualHTTPTail(t, f, nil, func() {
		assertActualHTTPTailLeases(t, f.db, true, f.source)
		observed = true
		f.revokeSource()
	}, false)
	require.True(t, observed)
	require.EqualValues(t, 1, model.calls.Load())
	require.Contains(
		t,
		w.Body.String(),
		"LIVE HTTP SECRET",
		"a later revocation cannot retract an already authorized write",
	)
	require.NotContains(t, w.Body.String(), `"response_type":"complete"`)
	require.Positive(t, w.answerWrites)
	require.Zero(t, w.completionWrites)
	require.Zero(t, w.guardedFlushes, "the actual underlying Flush must be blocked after source revocation")
	assertActualHTTPTailLeases(t, f.db, false, "")
}
