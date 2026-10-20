package handler

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
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
	"github.com/Tencent/WeKnora/internal/event"
	sessionhandler "github.com/Tencent/WeKnora/internal/handler/session"
	"github.com/Tencent/WeKnora/internal/middleware"
	"github.com/Tencent/WeKnora/internal/models/chat"
	"github.com/Tencent/WeKnora/internal/stream"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/alicebob/miniredis/v2"
	"github.com/gin-gonic/gin"
	redisclient "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

const (
	actualSteerHTTPPrefix = "actual-steer-original-http"
	actualSteerHTTPQuery  = "original steer guidance\nsecond original line\nthird original line"
	actualSteerHTTPAnswer = "ACTUAL STEER FINAL ANSWER"
)

// Embedding this actual public service port preserves native live publication
// and original-body reads while the observation hook places a revocation at
// the exact consume boundary. No Redis algorithm fixture is involved.
type actualSteerHTTPPort interface {
	interfaces.MessageService
	HasOriginalMessageMaterialStore() bool
	GetMessageForControl(context.Context, string, string) (*types.Message, error)
	GetMessageForStream(context.Context, string, string) (*types.Message, error)
	CheckMessagePublication(context.Context, *types.Message) error
	CheckLiveMessagePublication(context.Context, *types.Message) error
	MessageNativeLiveIdentity(context.Context, string, string) (*types.Message, string, error)
	CaptureNativeLiveMaterialParents(context.Context) ([]*types.MessageMaterialSnapshot, error)
	BeginNativeLiveMaterialReceiver(context.Context, []*types.MessageMaterialSnapshot) (func() error, func(), error)
	CheckNativeLiveIdentity(context.Context, *types.Message, string) error
	CheckNativeLiveFramePublication(context.Context, *types.Message, *types.SourceLineage) error
	BeginNativeLiveFrameSources(context.Context, *types.Message, *types.SourceLineage) (func() error, func(), error)
	AdmitActualSteerInput(context.Context, *types.SteerAuthoredRequest) (*types.SteerMaterialInput, error)
	BeginSteerMaterialRead(
		context.Context,
		*types.SteerMaterialRequest,
	) (*types.SteerMaterialSnapshot, func() error, func(), error)
	ApplyActualSteerControl(context.Context, *types.SteerControlRequest) (*types.SteerCandidateControl, error)
	CheckActualSteerControl(context.Context, *types.SteerCandidateControl) error
	ConsumeActualSteerIntoMessage(
		context.Context,
		*types.SteerMaterialInput,
		string,
		string,
		string,
	) (*types.Message, *types.SteerConsumptionProof, func() error, func(), error)
	ConsumeActualSteerFollowUpIntoMessage(
		context.Context,
		*types.SteerMaterialInput,
		string,
		string,
		string,
	) (*types.Message, *types.SteerConsumptionProof, func() error, func(), error)
	CreateActualSteerFollowUpAssistant(
		context.Context,
		*types.SteerMaterialInput,
		*types.Message,
	) (*types.Message, error)
}

type actualSteerHTTPObservedMessages struct {
	actualSteerHTTPPort
	t               *testing.T
	beforeConsume   func(*types.SteerMaterialInput)
	beforeAssistant func(*types.SteerMaterialInput, *types.Message)
	afterAssistant  func(*types.Message)
	consumeContext  func(context.Context) context.Context
	afterConsume    func(*types.Message, *types.SteerConsumptionProof)
	once            sync.Once
	consumes        atomic.Int64
}

func (m *actualSteerHTTPObservedMessages) ConsumeActualSteerIntoMessage(
	ctx context.Context,
	input *types.SteerMaterialInput,
	run, generation, request string,
) (*types.Message, *types.SteerConsumptionProof, func() error, func(), error) {
	m.consumes.Add(1)
	if m.beforeConsume != nil {
		m.once.Do(func() { m.beforeConsume(input) })
	}
	message, proof, check, closeRead, err := m.actualSteerHTTPPort.ConsumeActualSteerIntoMessage(
		ctx,
		input,
		run,
		generation,
		request,
	)
	if err == nil && m.afterConsume != nil {
		m.once.Do(func() { m.afterConsume(message, proof) })
	}
	return message, proof, check, closeRead, err
}

func (m *actualSteerHTTPObservedMessages) ConsumeActualSteerFollowUpIntoMessage(
	ctx context.Context,
	input *types.SteerMaterialInput,
	run, generation, request string,
) (*types.Message, *types.SteerConsumptionProof, func() error, func(), error) {
	m.consumes.Add(1)
	if m.beforeConsume != nil {
		m.once.Do(func() { m.beforeConsume(input) })
	}
	if m.consumeContext != nil {
		ctx = m.consumeContext(ctx)
	}
	message, proof, check, closeRead2, err := m.actualSteerHTTPPort.ConsumeActualSteerFollowUpIntoMessage(
		ctx,
		input,
		run,
		generation,
		request,
	)
	if err != nil {
		m.t.Logf("actual followup consume refused: %v", err)
	}
	if err == nil && m.afterConsume != nil {
		m.once.Do(func() { m.afterConsume(message, proof) })
	}
	return message, proof, check, closeRead2, err
}

func (m *actualSteerHTTPObservedMessages) CreateActualSteerFollowUpAssistant(
	ctx context.Context,
	input *types.SteerMaterialInput,
	message *types.Message,
) (*types.Message, error) {
	if m.beforeAssistant != nil {
		m.beforeAssistant(input, message)
	}
	created, err := m.actualSteerHTTPPort.CreateActualSteerFollowUpAssistant(ctx, input, message)
	if err != nil {
		m.t.Logf("actual followup assistant transfer refused: %v", err)
	}
	if err == nil && m.afterAssistant != nil {
		m.afterAssistant(created)
	}
	return created, err
}

type actualSteerHTTPModel struct {
	t            *testing.T
	f            *actualHTTPTailFixture
	steerID      string
	ready        chan struct{}
	resume       chan struct{}
	resumeOnce   sync.Once
	calls        atomic.Int64
	inputs       [][]chat.Message
	followUp     bool
	claimFailure bool
	titleCalls   atomic.Int64
}

func (m *actualSteerHTTPModel) GetModelName() string { return "local-original-steer-agent" }
func (m *actualSteerHTTPModel) GetModelID() string   { return "actual-http-tail-model" }
func (m *actualSteerHTTPModel) Chat(context.Context, []chat.Message, *chat.ChatOptions) (*types.ChatResponse, error) {
	return nil, fmt.Errorf("the actual steer HTTP fixture uses only the local Agent streaming model")
}

func (m *actualSteerHTTPModel) ChatStream(
	ctx context.Context,
	input []chat.Message,
	_ *chat.ChatOptions,
) (<-chan types.StreamResponse, error) {
	call := m.calls.Add(1)
	if err := service.CheckMessageMaterialOutputBoundary(ctx); err != nil {
		return nil, err
	}
	assertActualSteerBodyLease(m.t, m.f.db, "", true)
	parentSeen, steerSeen := false, false
	for _, item := range input {
		if item.Content == m.f.parent.Content {
			parentSeen = true
			require.Equal(
				m.t,
				m.f.parent.SourceLineage,
				item.SourceLineage,
				"saved history retains its actual original source envelope",
			)
		}
		if strings.Contains(item.Content, "original steer guidance") {
			steerSeen = true
			if m.followUp {
				envelope, authored, ok := strings.Cut(item.Content, "</runtime_context>\n\n")
				require.True(m.t, ok, "the actual query uses the production runtime scaffold")
				require.Regexp(
					m.t,
					("^<runtime_context scope=\"this_turn\">\\n  <current_time>[0-9]{4}-[0-9]{2}"+
						"-[0-9]{2}</current_time>\\n  <session>")+regexp.QuoteMeta(
						m.f.parent.SessionID,
					)+`</session>\n$`,
					envelope,
				)
				require.Equal(m.t, actualSteerHTTPQuery, authored)
			} else {
				require.Equal(m.t, types.SteerMessageContent(actualSteerHTTPQuery), item.Content)
			}
			require.NoError(m.t, item.SourceLineage.RequireComplete())
		}
		require.NotContains(m.t, item.Content, "FORGED REDIS")
	}
	if m.followUp && !parentSeen {
		for _, item := range input {
			m.t.Logf(
				"actual successor model item role=%s bytes=%d source=%v",
				item.Role,
				len(item.Content),
				item.SourceLineage,
			)
		}
	}
	require.True(m.t, parentSeen, "actual Agent rounds preserve admitted saved history")
	require.Equal(m.t, call > 1, steerSeen, "the next actual model call receives the exact canonical steer input")
	copied := append([]chat.Message(nil), input...)
	m.inputs = append(m.inputs, copied)
	out := make(chan types.StreamResponse, 1)
	if call == 1 {
		close(m.ready)
		go func() {
			defer close(out)
			select {
			case <-ctx.Done():
				return
			case <-m.resume:
			}
			if m.followUp {
				out <- types.StreamResponse{Content: actualSteerHTTPAnswer}
				out <- types.StreamResponse{Done: true, FinishReason: "stop"}
				return
			}
			out <- types.StreamResponse{
				ResponseType: types.ResponseTypeAnswer,
				Content:      "ACTUAL FIRST ROUND",
				Done:         true,
				FinishReason: "stop",
			}
		}()
	} else {
		assertActualSteerBodyLease(m.t, m.f.db, m.steerID, true)
		if m.f.source != "" {
			assertActualHTTPTailLeases(m.t, m.f.db, true, m.f.source)
		}
		out <- types.StreamResponse{
			ResponseType: types.ResponseTypeAnswer,
			Content:      actualSteerHTTPAnswer,
			Done:         true,
			FinishReason: "stop",
		}
		close(out)
	}
	return out, nil
}
func (m *actualSteerHTTPModel) release() { m.resumeOnce.Do(func() { close(m.resume) }) }

type actualSteerHTTPModels struct {
	actualHTTPTailModels
	steered *actualSteerHTTPModel
}

type actualSteerHTTPSessionPort interface {
	interfaces.SessionService
	RejectSendIfRewinding(context.Context, string) error
	HoldSandboxTurn(context.Context, string, string) (func(), error)
}

type actualSteerHTTPObservedSessions struct {
	actualSteerHTTPSessionPort
	titleCalls *atomic.Int64
}

func (s *actualSteerHTTPObservedSessions) GenerateTitleAsync(
	ctx context.Context,
	session *types.Session,
	query, modelID string,
	bus *event.EventBus,
) {
	s.titleCalls.Add(1)
	s.actualSteerHTTPSessionPort.GenerateTitleAsync(ctx, session, query, modelID, bus)
}

func (m actualSteerHTTPModels) GetChatModel(context.Context, string) (chat.Chat, error) {
	return m.steered, nil
}

type actualSteerHTTPWriter struct {
	*httptest.ResponseRecorder
	t              *testing.T
	f              *actualHTTPTailFixture
	steerID        string
	finalWrites    int
	completeWrites int
	lastDerived    bool
}

func (w *actualSteerHTTPWriter) Write(body []byte) (int, error) {
	w.lastDerived = bytes.Contains(body, []byte(actualSteerHTTPAnswer)) ||
		bytes.Contains(body, []byte(`"response_type":"complete"`))
	if w.lastDerived {
		assertActualSteerBodyLease(w.t, w.f.db, w.steerID, true)
		if w.f.source != "" {
			assertActualHTTPTailLeases(w.t, w.f.db, true, w.f.source)
		}
	}
	if bytes.Contains(body, []byte(actualSteerHTTPAnswer)) {
		w.finalWrites++
	}
	if bytes.Contains(body, []byte(`"response_type":"complete"`)) {
		w.completeWrites++
	}
	return w.ResponseRecorder.Write(body)
}

func (w *actualSteerHTTPWriter) Flush() {
	if w.lastDerived {
		assertActualSteerBodyLease(w.t, w.f.db, w.steerID, true)
	}
	w.ResponseRecorder.Flush()
	w.lastDerived = false
}

func assertActualSteerBodyLease(t *testing.T, db *gorm.DB, steerID string, held bool) {
	t.Helper()
	query := db.Table("original_body_leases l").Where("l.kind='read' AND l.released_at_ms IS NULL")
	if steerID != "" {
		query = query.Joins(("JOIN original_body_field_heads h ON h.body_id=l.body_id AND h.source_table" +
			"='steer_authored_origins'")).
			Joins("JOIN steer_authored_origins o ON o.id=h.row_key").
			Where("o.steer_id=?", steerID)
	}
	var count int64
	require.NoError(t, query.Count(&count).Error)
	if held {
		require.Positive(
			t,
			count,
			"actual original body read leases remain live through the next model and final HTTP write",
		)
	} else {
		require.Zero(t, count, "all physical original body read leases close after HTTP and producer completion")
	}
}

func newActualSteerHTTPFixture(t *testing.T, controlled bool) *actualHTTPTailFixture {
	t.Helper()
	var f *actualHTTPTailFixture
	var sourceFAQ *faqAllLeaseFixture
	if controlled {
		sourceFAQ = faqAllReadLeaseFixture(t, "personal_revoke_before_write")
		sourceFAQ.calls.Store(-10000)
		f = &actualHTTPTailFixture{
			db:           sourceFAQ.db,
			store:        sourceFAQ.store,
			kbs:          sourceFAQ.knowledge.kbService,
			knowledge:    sourceFAQ.knowledge.kgService,
			guard:        sourceFAQ.guard,
			ctx:          sourceFAQ.ctx,
			source:       "source",
			revokeSource: func() { sourceFAQ.calls.Store(2) },
		}
	} else {
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
		f = &actualHTTPTailFixture{
			db:        db,
			store:     store,
			kbs:       parent.kbService,
			knowledge: parent.kgService,
			guard:     guard,
			ctx:       collectionHumanInputContext(7),
		}
	}
	f.ctx = context.WithValue(f.ctx, types.UserIDContextKey, "user")
	f.ctx = context.WithValue(f.ctx, types.TenantInfoContextKey, &types.Tenant{ID: 7})
	bounded, cancel := context.WithTimeout(f.ctx, 20*time.Second)
	t.Cleanup(cancel)
	f.ctx = bounded
	f.messages = configureNativeSavedReplayMessages(t, f.db, f.store, f.kbs, f.knowledge, f.guard)
	for _, name := range []string{"000073_steer_original_materials.up.sql", "000071_original_body_retention.up.sql"} {
		ddl, err := os.ReadFile("../../migrations/sqlite/" + name)
		require.NoError(t, err)
		require.NoError(t, f.db.Exec(string(ddl)).Error)
	}
	require.NoError(t, repository.UpgradeMessageProducerAdvanceSchema(f.db))
	require.NoError(t, repository.InstallOriginalBodyRetentionTriggers(f.db))
	require.NoError(t, repository.InstallOriginalBodyRetentionCallbacks(f.db))
	dir := t.TempDir()
	require.NoError(t, os.Chmod(dir, 0o700))
	key, pin := filepath.Join(dir, "key"), filepath.Join(dir, "pin")
	require.NoError(t, os.WriteFile(key, []byte(strings.Repeat("actual-steer-owned-journal-key-", 3)), 0o600))
	t.Setenv("WEKNORA_ORIGINAL_BODY_JOURNAL_DIRECTORY", dir)
	t.Setenv("WEKNORA_ORIGINAL_BODY_JOURNAL_KEY", key)
	t.Setenv("WEKNORA_ORIGINAL_BODY_JOURNAL_PIN", pin)
	require.NoError(t, repository.NewOriginalBodyRetentionStore(f.db).InitializeJournal())
	repository.ConfigureMessageOriginalBodyAvailability(f.store, repository.CheckOriginalBodyAvailableInTx)
	lineage, _ := types.NewCompleteSourceLineage()
	parentCtx := f.ctx
	var closeInput func()
	if controlled {
		request := types.FAQInputReadRequest{
			TenantID:        7,
			KnowledgeBaseID: "faq-derived",
			ChunkIDs:        []string{sourceFAQ.entry.ChunkID},
		}
		plan, err := f.store.PeekFAQInputReadPlan(f.ctx, request)
		require.NoError(t, err)
		inputLease, err := acquireFAQReadPlanLeases(
			f.ctx,
			f.store.ContentLeaseStore(),
			plan.Scopes,
			plan.SourceLineage,
			0,
		)
		require.NoError(t, err)
		parentCtx, err = repository.WithFAQInputReadPlan(inputLease.Context(f.ctx), plan)
		require.NoError(t, err)
		original, err := f.store.CaptureFAQInputs(parentCtx, request)
		require.NoError(t, err)
		require.Len(t, original.Proofs, 1)
		lineage = original.Proofs[0].SourceLineage
		closeInput = inputLease.Close
	}
	f.parent = produceNativeSavedReplayMessage(

		parentCtx, t,
		f.db,
		f.messages,

		"ACTUAL ORIGINAL RETAINED PARENT",
		lineage,
	)
	if closeInput != nil {
		closeInput()
	}
	require.NoError(
		t,
		f.db.Model(&types.Session{}).Where("id=?", f.parent.SessionID).Update("title", "Actual steer fixture").Error,
	)
	require.NoError(t, f.db.AutoMigrate(&types.CustomAgent{}))
	return f
}

func actualSteerHTTPRouter(ctx context.Context, h *sessionhandler.Handler) *gin.Engine {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.Use(middleware.ErrorHandler())
	router.Use(func(c *gin.Context) {
		c.Request = c.Request.WithContext(ctx)
		c.Set(types.TenantIDContextKey.String(), uint64(7))
		c.Set(types.UserIDContextKey.String(), "user")
		c.Set(types.RequestIDContextKey.String(), "actual-steer-current-turn")
		c.Next()
	})
	router.POST("/agent-chat/:session_id", h.AgentQA)
	router.POST("/sessions/:session_id/steer", h.SteerMessage)
	router.POST("/sessions/:session_id/steer/:steer_id/inject", h.PromoteSteerMessage)
	router.GET("/sessions/:id/steer", h.ListSteerMessages)
	router.DELETE("/sessions/:session_id/steer/:steer_id", h.DeleteSteerMessage)
	return router
}

func newActualSteerHTTPHandler(
	t *testing.T,
	f *actualHTTPTailFixture,
	model *actualSteerHTTPModel,
) (*sessionhandler.Handler, *actualSteerHTTPObservedMessages, *stream.RedisStreamManager, *redisclient.Client) {
	t.Helper()
	disabled := false
	custom := &types.CustomAgent{
		ID:        "actual-steer-agent",
		Name:      "Actual local steer agent",
		TenantID:  7,
		CreatedBy: "user",
		Config: types.CustomAgentConfig{
			AgentMode:           types.AgentModeSmartReasoning,
			AgentType:           types.AgentTypeCustom,
			ModelID:             model.GetModelID(),
			SystemPrompt:        "",
			KBSelectionMode:     "none",
			MCPSelectionMode:    "none",
			SkillsSelectionMode: "none",
			WebSearchProviderID: "unused",
			AllowedTools:        []string{"read_document"},
			MultiTurnEnabled:    true,
			MemoryEnabled:       &disabled,
			MaxIterations:       3,
		},
	}
	require.NoError(t, f.db.Create(custom).Error)
	models := actualSteerHTTPModels{steered: model}
	cfg := &config.Config{
		Conversation: &config.ConversationConfig{
			MaxRounds: 3,
			Summary: &config.SummaryConfig{
				Prompt:          "Actual operator configured prompt",
				ContextTemplate: "{{.Query}}",
			},
		},
	}
	agents := service.NewAgentService(
		cfg,
		models,
		f.kbs,
		f.knowledge,
		nil,
		nil,
		nil,
		nil,
		event.NewEventBus(),
		f.db,
		nil,
		nil,
		nil,
		nil,
		f.messages,
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
	service.ConfigureAgentRAGChunkHydration(agents, repository.NewRAGChunkSnapshotReader(f.db, f.store))
	manager := chatpipeline.NewEventManager()
	messageRows := repository.NewMessageRepository(f.db)
	repository.ConfigureMessageMaterialWriters(messageRows, f.store)
	sessions := service.NewSessionService(
		cfg,
		repository.NewSessionRepository(f.db),
		messageRows,
		f.kbs,
		f.knowledge,
		nil,
		models,
		nil,
		manager,
		agents,
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
	groups := service.NewGroupAccessService(repository.NewGroupAccessRepository(f.db))
	service.ConfigureSessionGroupAccess(sessions, groups)
	customs := service.NewCustomAgentService(
		repository.NewCustomAgentRepository(f.db),
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
	)
	mini := miniredis.RunT(t)
	redis, err := stream.NewRedisStreamManager(mini.Addr(), "", "", 0, actualSteerHTTPPrefix, time.Minute)
	require.NoError(t, err)
	raw := redisclient.NewClient(&redisclient.Options{Addr: mini.Addr()})
	t.Cleanup(func() { _ = raw.Close(); _ = redis.Close() })
	actual, ok := f.messages.(actualSteerHTTPPort)
	require.True(t, ok)
	observed := &actualSteerHTTPObservedMessages{actualSteerHTTPPort: actual, t: t}
	var transport interfaces.StreamManager = redis
	if model.claimFailure {
		transport = &actualSteerHTTPClaimFailureStream{StreamManager: redis}
	}
	actualSessions, ok := sessions.(actualSteerHTTPSessionPort)
	require.True(t, ok, "observe the real title producer without dropping the production turn/rewind guards")
	observedSessions := &actualSteerHTTPObservedSessions{
		actualSteerHTTPSessionPort: actualSessions,
		titleCalls:                 &model.titleCalls,
	}
	h := sessionhandler.NewHandler(
		observedSessions,
		observed,
		nil,
		transport,
		cfg,
		f.kbs,
		customs,
		nil,
		nil,
		nil,
		nil,
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
		groups,
	)
	return h, observed, redis, raw
}

type actualSteerHTTPClaimFailureStream struct{ interfaces.StreamManager }

func (*actualSteerHTTPClaimFailureStream) ClaimLiveRun(context.Context, string, string, string) error {
	return fmt.Errorf("controlled actual successor claim failure")
}

func actualSteerHTTPRequest(router http.Handler, method, path string, body interface{}) *httptest.ResponseRecorder {
	var encoded []byte
	if body != nil {
		encoded, _ = json.Marshal(body)
	}
	request := httptest.NewRequest(method, path, bytes.NewReader(encoded))
	request.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, request)
	return w
}

func forgeActualSteerHTTPMirror(
	t *testing.T,
	f *actualHTTPTailFixture,
	redis *stream.RedisStreamManager,
	raw *redisclient.Client,
	run, steerID string,
) {
	t.Helper()
	events, _, err := redis.GetSteerEvents(f.ctx, f.parent.SessionID, run, 0)
	require.NoError(t, err)
	require.Len(t, events, 1)
	require.Equal(t, steerID, events[0].ID)
	events[0].Content = "FORGED REDIS QUERY"
	// Keep the same original source envelope and immutable control labels.
	// None of them authorizes these copied fields or a cached user-row ID.
	events[0].Data["mentioned_items"] = []interface{}{
		map[string]interface{}{"type": "file", "id": "forged-file", "name": "FORGED REDIS MENTION"},
	}
	events[0].Data["channel"] = "FORGED REDIS CHANNEL"
	events[0].Data["user_message_id"] = "FORGED REDIS USER ID"
	encoded, err := json.Marshal(events[0])
	require.NoError(t, err)
	key := fmt.Sprintf("%s:%s:%s:steer", actualSteerHTTPPrefix, f.parent.SessionID, run)
	require.NoError(t, raw.LSet(f.ctx, key, 0, encoded).Err())
}
