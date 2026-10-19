package handler

import (
	"bytes"
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
	"github.com/Tencent/WeKnora/internal/middleware"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	gormlogger "gorm.io/gorm/logger"
)

// Uses the actual message producer and SQL receipts for a raw authored user
// input. No assistant body/proof, model or remote source is fabricated here.
func TestHistorySearchActualHTTPTransactionAuthorityFactory(t *testing.T) {
	db, err := gorm.Open(sqlite.Open(filepath.Join(t.TempDir(), "history-authority.db")), &gorm.Config{
		DisableForeignKeyConstraintWhenMigrating: true,
		Logger:                                   gormlogger.Default.LogMode(gormlogger.Silent),
	})
	require.NoError(t, err)
	require.NoError(
		t,
		db.AutoMigrate(
			&types.User{},
			&types.TenantMember{},
			&types.TenantAPIKey{},
			&types.KnowledgeBase{},
			&types.Knowledge{},
			&types.Chunk{},
			&types.Session{},
			&types.Message{},
		),
	)
	require.NoError(t, db.Exec("PRAGMA journal_mode=WAL").Error)
	require.NoError(t, db.Exec("ALTER TABLE messages ADD COLUMN artifacts TEXT").Error)
	for _, file := range []string{"000023_message_artifacts_table.up.sql", "000026_message_artifacts_deleted_at.up.sql", "000028_directory_group_access.up.sql", "000046_nextcloud_content_leases.up.sql", "000070_message_original_materials.up.sql"} {
		ddl, err := os.ReadFile("../../migrations/sqlite/" + file)
		require.NoError(t, err)
		require.NoError(t, db.Exec(string(ddl)).Error)
	}
	installHTTPCurrentNextcloudControls(t, db)
	sqlDB, err := db.DB()
	require.NoError(t, err)
	defer func() { require.NoError(t, sqlDB.Close()) }()
	sqlDB.SetMaxOpenConns(1)
	sqlDB.SetMaxIdleConns(1)
	groups := service.NewGroupAccessService(repository.NewGroupAccessRepository(db))
	knowledge := repository.NewKnowledgeRepository(db)
	kbs := collectionHTTPKB{db: db}
	configureFactory := func(store *repository.KnowledgeInputProvenanceStore) {
		repository.ConfigureKnowledgeDisplayInputWriters(
			store,
			nil,
			knowledge,
			func(tx *gorm.DB) *repository.KnowledgeInputProvenanceStore {
				actual := repository.NewKnowledgeInputProvenanceStore(tx)
				repository.ConfigureKnowledgeInputActorGroupAccess(
					actual,
					service.NewGroupAccessService(repository.NewGroupAccessRepository(tx)),
				)
				return actual
			},
		)
	}
	newMessages := func(store *repository.KnowledgeInputProvenanceStore) interfaces.MessageService {
		repository.ConfigureKnowledgeInputActorGroupAccess(store, groups)
		guard := access.NewNextcloudSourceLineageGuard(
			knowledge.(access.NextcloudLineageSnapshotLookup),
			kbs,
			nil,
			nil,
			groups,
			nil,
			store,
		)
		messages := service.NewMessageService(
			repository.NewMessageRepository(db),
			repository.NewSessionRepository(db),
			nil,
			kbs,
			nil,
			nil,
			nil,
			nil,
			nil,
		)
		service.ConfigureMessageMaterialProvenance(messages, store)
		service.ConfigureMessageSourceLineage(
			messages,
			nil,
			&service.SessionForkService{},
			chatpipeline.NewEventManager(),
			guard,
		)
		return messages
	}
	producerStore := repository.NewKnowledgeInputProvenanceStore(db)
	configureFactory(producerStore)
	producer := newMessages(producerStore)
	for _, id := range []string{"history-author", "history-other-reader"} {
		require.NoError(
			t,
			db.Create(
				&types.User{ID: id, TenantID: 7, Username: id, Email: id + "@isolated.test", IsActive: true},
			).Error,
		)
		require.NoError(
			t,
			db.Create(
				&types.TenantMember{
					UserID:   id,
					TenantID: 7,
					Role:     types.TenantRoleContributor,
					Status:   types.TenantMemberStatusActive,
				},
			).Error,
		)
	}
	readerContext := func(id string) context.Context {
		ctx := types.WithExecutionTenant(context.Background(), 7)
		ctx = types.WithCaller(ctx, types.Caller{TenantID: 7, UserID: id, Role: types.TenantRoleContributor})
		ctx = types.WithPrincipal(ctx, types.Principal{Type: types.PrincipalWebUser, ID: id})
		return context.WithValue(ctx, types.UserIDContextKey, id)
	}
	ctx, cancel := context.WithTimeout(readerContext("history-author"), 30*time.Second)
	defer cancel()
	session := &types.Session{TenantID: 7, UserID: "history-author"}
	require.NoError(t, db.Create(session).Error)
	question, err := producer.CreateMessage(ctx, &types.Message{
		SessionID: session.ID, RequestID: "actual-http-history-turn", Role: "user",
		Content: "Actual independently authored transaction factory search target", IsCompleted: true,
	})
	require.NoError(t, err)
	const query = "transaction factory search target"

	// Reopen reader wiring over the same actual database. Omitting the
	// transaction factory must not reinterpret current body bytes as proof.
	readerStore := repository.NewKnowledgeInputProvenanceStore(db)
	reader := newMessages(readerStore)
	currentReader := ctx
	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.Use(middleware.ErrorHandler(), func(c *gin.Context) {
		c.Request = c.Request.WithContext(currentReader)
		c.Next()
	})
	router.POST("/messages/search", NewMessageHandler(reader, nil, nil).SearchMessages)
	search := func() (*httptest.ResponseRecorder, types.MessageSearchResult) {
		encoded, err := json.Marshal(
			map[string]any{"query": query, "mode": "keyword", "limit": 20, "session_ids": []string{session.ID}},
		)
		require.NoError(t, err)
		request := httptest.NewRequest(http.MethodPost, "/messages/search", bytes.NewReader(encoded))
		request.Header.Set("Content-Type", "application/json")
		response := httptest.NewRecorder()
		router.ServeHTTP(response, request)
		require.Equal(t, http.StatusOK, response.Code, response.Body.String())
		var decoded struct {
			Success bool                      `json:"success"`
			Data    types.MessageSearchResult `json:"data"`
		}
		require.NoError(t, json.Unmarshal(response.Body.Bytes(), &decoded))
		require.True(t, decoded.Success)
		return response, decoded.Data
	}
	missingResponse, missing := search()
	require.Zero(t, missing.Total)
	require.Empty(t, missing.Items)
	require.NotContains(t, missingResponse.Body.String(), question.Content)

	// The production container already installs this factory. Only the
	// incomplete HTTP fixture wiring changes; the persisted receipt/body do not.
	configureFactory(readerStore)
	_, allowed := search()
	require.Equal(t, 1, allowed.Total)
	require.Len(t, allowed.Items, 1)
	require.Equal(t, session.ID, allowed.Items[0].SessionID)
	require.Equal(t, question.RequestID, allowed.Items[0].RequestID)
	require.Equal(t, question.Content, allowed.Items[0].QueryContent)
	require.Empty(t, allowed.Items[0].AnswerContent)
	currentReader = readerContext("history-other-reader")
	foreignResponse, foreign := search()
	require.Zero(t, foreign.Total)
	require.Empty(t, foreign.Items)
	require.NotContains(t, foreignResponse.Body.String(), question.Content)

	// The actual copied search projection remains under its original guards;
	// changing output bytes cannot leave original-row receipts sufficient.
	boundary := service.WithMessageMaterialOutputBoundary(ctx)
	defer service.CloseMessageMaterialOutputBoundary(boundary)
	projection, err := reader.SearchMessages(
		boundary,
		&types.MessageSearchParams{
			Query:      query,
			Mode:       types.MessageSearchModeKeyword,
			Limit:      20,
			SessionIDs: []string{session.ID},
		},
	)
	require.NoError(t, err)
	require.Equal(t, 1, projection.Total)
	require.NoError(t, service.CheckMessageMaterialOutputBoundary(boundary))
	projection.Items[0].QueryContent += " [unverified output]"
	require.ErrorIs(t, service.CheckMessageMaterialOutputBoundary(boundary), access.ErrNextcloudPublicationDenied)
	projection.Items[0].QueryContent = question.Content
	require.NoError(t, service.CheckMessageMaterialOutputBoundary(boundary))
	service.CloseMessageMaterialOutputBoundary(boundary)
	var live int64
	require.NoError(
		t,
		db.Table("nextcloud_content_leases").Where("kind='read' AND released_at_ms IS NULL").Count(&live).Error,
	)
	require.Zero(t, live, "HTTP and explicit output guards must release their physical leases")
}
