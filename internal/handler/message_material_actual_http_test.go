package handler

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/application/service"
	chatpipeline "github.com/Tencent/WeKnora/internal/application/service/chat_pipeline"
	apperrors "github.com/Tencent/WeKnora/internal/errors"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func TestMessageMaterialActualHTTPProducerReadSingleConnection(t *testing.T) {
	db, err := gorm.Open(sqlite.Open(filepath.Join(t.TempDir(), "message.db")), &gorm.Config{DisableForeignKeyConstraintWhenMigrating: true})
	require.NoError(t, err)
	require.NoError(t, db.AutoMigrate(&types.User{}, &types.TenantMember{}, &types.TenantAPIKey{}, &types.KnowledgeBase{}, &types.Knowledge{}, &types.Chunk{}, &types.Session{}, &types.Message{}))
	require.NoError(t, db.Exec("PRAGMA journal_mode=WAL").Error)
	require.NoError(t, db.Exec("ALTER TABLE messages ADD COLUMN artifacts TEXT").Error)
	for _, file := range []string{"000023_message_artifacts_table.up.sql", "000026_message_artifacts_deleted_at.up.sql", "000028_directory_group_access.up.sql", "000046_nextcloud_content_leases.up.sql", "000070_message_original_materials.up.sql"} {
		ddl, err := os.ReadFile("../../migrations/sqlite/" + file)
		require.NoError(t, err)
		require.NoError(t, db.Exec(string(ddl)).Error)
	}
	require.NoError(t, db.Exec("CREATE TABLE im_channel_sessions(id TEXT,session_id TEXT,platform TEXT)").Error)
	sqlDB, err := db.DB()
	require.NoError(t, err)
	defer sqlDB.Close()
	sqlDB.SetMaxOpenConns(1)
	sqlDB.SetMaxIdleConns(1)
	store := repository.NewKnowledgeInputProvenanceStore(db)
	groups := service.NewGroupAccessService(repository.NewGroupAccessRepository(db))
	repository.ConfigureKnowledgeInputActorGroupAccess(store, groups)
	knowledge := repository.NewKnowledgeRepository(db)
	kbrepo := repository.NewKnowledgeBaseRepository(db)
	chunks := repository.NewChunkRepository(db)
	repository.ConfigureKnowledgeDisplayInputWriters(store, nil, knowledge, func(tx *gorm.DB) *repository.KnowledgeInputProvenanceStore {
		actual := repository.NewKnowledgeInputProvenanceStore(tx)
		repository.ConfigureKnowledgeInputActorGroupAccess(actual, service.NewGroupAccessService(repository.NewGroupAccessRepository(tx)))
		return actual
	})
	leases := store.ContentLeaseStore()
	kbs := service.NewKnowledgeBaseService(kbrepo, knowledge, chunks, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, leases)
	sourceGuard := access.NewNextcloudSourceLineageGuard(knowledge.(access.NextcloudLineageSnapshotLookup), kbs, nil, nil, groups, nil, store)
	messages := service.NewMessageService(repository.NewMessageRepository(db), repository.NewSessionRepository(db), nil, kbs, nil, nil, nil, nil, nil)
	service.ConfigureMessageSourceLineage(messages, nil, &service.SessionForkService{}, &chatpipeline.EventManager{}, sourceGuard)
	service.ConfigureMessageMaterialProvenance(messages, store)
	user := &types.User{ID: "actual-http-user", TenantID: 7, Username: "actual", Email: "actual@isolated.test", IsActive: true}
	require.NoError(t, db.Create(user).Error)
	require.NoError(t, db.Create(&types.TenantMember{UserID: user.ID, TenantID: 7, Role: types.TenantRoleContributor, Status: types.TenantMemberStatusActive}).Error)
	kb := &types.KnowledgeBase{ID: "actual-http-kb", TenantID: 7, Name: "actual"}
	require.NoError(t, db.Create(kb).Error)
	ctx := types.WithExecutionTenant(context.Background(), 7)
	ctx = types.WithCaller(ctx, types.Caller{TenantID: 7, UserID: user.ID, Role: types.TenantRoleContributor})
	ctx = types.WithPrincipal(ctx, types.Principal{Type: types.PrincipalWebUser, ID: user.ID})
	ctx = context.WithValue(ctx, types.UserIDContextKey, user.ID)
	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	session := &types.Session{TenantID: 7, UserID: user.ID}
	require.NoError(t, db.Create(session).Error)
	question, err := messages.CreateMessage(ctx, &types.Message{SessionID: session.ID, RequestID: "actual-http-turn", Role: "user", Content: "actual new human question", IsCompleted: true, ExecutionContext: types.MessageExecutionContext{KnowledgeBaseIDs: []string{kb.ID}}})
	require.NoError(t, err)
	answer, err := messages.CreateMessage(ctx, &types.Message{SessionID: session.ID, RequestID: question.RequestID, Role: "assistant", ExecutionContext: question.ExecutionContext})
	require.NoError(t, err)
	answer.Content = "actual new generated saved answer"
	answer.IsCompleted = true
	answer.SourceLineage, _ = types.NewCompleteSourceLineage()
	require.NoError(t, messages.UpdateMessage(ctx, answer))
	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.Use(func(c *gin.Context) {
		c.Request = c.Request.WithContext(ctx)
		c.Next()
		if len(c.Errors) > 0 && !c.Writer.Written() {
			code := http.StatusInternalServerError
			if app, ok := apperrors.IsAppError(c.Errors.Last().Err); ok {
				code = app.HTTPCode
			}
			c.JSON(code, gin.H{"error": c.Errors.Last().Err.Error()})
		}
	})
	router.GET("/messages/:session_id/load", NewMessageHandler(messages, nil, nil).LoadMessages)
	request := func() *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		router.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/messages/"+session.ID+"/load?limit=20", nil))
		return w
	}
	response := request()
	require.Equal(t, http.StatusOK, response.Code, response.Body.String())
	require.Contains(t, response.Body.String(), answer.Content)
	var live int64
	require.NoError(t, db.Table("nextcloud_content_leases").Where("kind='read' AND released_at_ms IS NULL").Count(&live).Error)
	require.Zero(t, live)
	require.NoError(t, db.Model(&types.Message{}).Where("id=?", answer.ID).Update("content", "foreign transient").Error)
	require.NoError(t, db.Model(&types.Message{}).Where("id=?", answer.ID).Update("content", answer.Content).Error)
	response = request()
	require.Equal(t, http.StatusServiceUnavailable, response.Code, response.Body.String())
	require.NotContains(t, response.Body.String(), answer.Content)
}

func TestMessageMaterialActualControlledSavedHTTPAndCurrentSourceRevoke(t *testing.T) {
	f := faqAllReadLeaseFixture(t, "personal_revoke_before_write")
	f.calls.Store(-10000)
	require.NoError(t, f.db.Exec("ALTER TABLE messages ADD COLUMN artifacts TEXT").Error)
	for _, file := range []string{"000023_message_artifacts_table.up.sql", "000026_message_artifacts_deleted_at.up.sql", "000070_message_original_materials.up.sql"} {
		ddl, err := os.ReadFile("../../migrations/sqlite/" + file)
		require.NoError(t, err)
		require.NoError(t, f.db.Exec(string(ddl)).Error)
	}
	require.NoError(t, f.db.Exec("CREATE TABLE im_channel_sessions(id TEXT,session_id TEXT,platform TEXT)").Error)
	ctx := context.WithValue(f.ctx, types.UserIDContextKey, types.CallerFromContext(f.ctx).UserID)
	req := types.FAQInputReadRequest{TenantID: 7, KnowledgeBaseID: "faq-derived", ChunkIDs: []string{f.entry.ChunkID}}
	plan, err := f.store.PeekFAQInputReadPlan(ctx, req)
	require.NoError(t, err)
	held, err := acquireFAQReadPlanLeases(ctx, f.store.ContentLeaseStore(), plan.Scopes, plan.SourceLineage, 0)
	require.NoError(t, err)
	defer held.Close()
	ctx, err = repository.WithFAQInputReadPlan(held.Context(ctx), plan)
	require.NoError(t, err)
	actual, err := f.store.CaptureFAQInputs(ctx, req)
	require.NoError(t, err)
	require.Len(t, actual.Chunks, 1)
	require.Len(t, actual.Proofs, 1)
	require.Len(t, actual.Proofs[0].SourceLineage.Sources, 1)
	messages := service.NewMessageService(repository.NewMessageRepository(f.db), repository.NewSessionRepository(f.db), nil, f.knowledge.kbService, f.knowledge.kgService, nil, nil, nil, nil)
	service.ConfigureMessageSourceLineage(messages, nil, &service.SessionForkService{}, &chatpipeline.EventManager{}, f.guard)
	service.ConfigureMessageMaterialProvenance(messages, f.store)
	session := &types.Session{TenantID: 7, UserID: types.CallerFromContext(ctx).UserID}
	require.NoError(t, f.db.Create(session).Error)
	question, err := messages.CreateMessage(ctx, &types.Message{SessionID: session.ID, RequestID: "actual-controlled-message-turn", Role: "user", Content: "new human question", IsCompleted: true, ExecutionContext: types.MessageExecutionContext{KnowledgeBaseIDs: []string{"kb"}}})
	require.NoError(t, err)
	answer, err := messages.CreateMessage(ctx, &types.Message{SessionID: session.ID, RequestID: question.RequestID, Role: "assistant", ExecutionContext: question.ExecutionContext})
	require.NoError(t, err)
	// The deterministic producer consumes the actual captured original field.
	answer.Content = "new saved derivative: " + actual.Chunks[0].Content
	answer.IsCompleted = true
	answer.SourceLineage = actual.Proofs[0].SourceLineage
	require.NoError(t, messages.UpdateMessage(ctx, answer))
	// A later full update cannot remove the original controlled origins.
	answer.SourceLineage, _ = types.NewCompleteSourceLineage()
	answer.Content += " preserved original fields"
	require.NoError(t, messages.UpdateMessage(ctx, answer))
	require.Len(t, answer.SourceLineage.Sources, 1)
	held.Close()
	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.Use(func(c *gin.Context) {
		c.Request = c.Request.WithContext(f.ctx)
		c.Request = c.Request.WithContext(context.WithValue(c.Request.Context(), types.UserIDContextKey, types.CallerFromContext(f.ctx).UserID))
		c.Next()
		if len(c.Errors) > 0 && !c.Writer.Written() {
			code := http.StatusInternalServerError
			if app, ok := apperrors.IsAppError(c.Errors.Last().Err); ok {
				code = app.HTTPCode
			}
			c.JSON(code, gin.H{"error": c.Errors.Last().Err.Error()})
		}
	})
	router.GET("/messages/:session_id/load", NewMessageHandler(messages, nil, nil).LoadMessages)
	call := func() *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		router.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/messages/"+session.ID+"/load", nil))
		return w
	}
	response := call()
	require.Equal(t, http.StatusOK, response.Code, response.Body.String())
	var decoded struct{ Data []*types.Message }
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &decoded))
	require.Len(t, decoded.Data, 2)
	require.Equal(t, answer.Content, decoded.Data[1].Content)
	observed := false
	f.groups.Hook = func(context.Context) {
		observed = true
		var count int64
		require.NoError(t, f.db.Table("nextcloud_content_leases").Where("kind='read' AND released_at_ms IS NULL AND knowledge_id='source'").Count(&count).Error)
		require.Positive(t, count)
	}
	f.calls.Store(2)
	response = call()
	require.Equal(t, http.StatusForbidden, response.Code, response.Body.String())
	require.NotContains(t, response.Body.String(), "new saved derivative")
	require.True(t, observed)
	var live int64
	require.NoError(t, f.db.Table("nextcloud_content_leases").Where("kind='read' AND released_at_ms IS NULL").Count(&live).Error)
	require.Zero(t, live)
}
