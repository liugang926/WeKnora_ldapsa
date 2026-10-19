//go:build sqlite_fts5

package handler

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Tencent/WeKnora/internal/application/access"
	sqlindex "github.com/Tencent/WeKnora/internal/application/repository/retriever/sqlite"
	"github.com/Tencent/WeKnora/internal/middleware"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	sqlite_vec "github.com/asg017/sqlite-vec-go-bindings/cgo"
	"github.com/gin-gonic/gin"
	"github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/require"
	gormsqlite "gorm.io/driver/sqlite"
	"gorm.io/gorm"
	gormlogger "gorm.io/gorm/logger"
)

type historyActualStorageFaultService struct {
	interfaces.MessageService
	fault error
}

func (s historyActualStorageFaultService) SearchMessages(
	context.Context,
	*types.MessageSearchParams,
) (*types.MessageSearchResult, error) {
	// A partial cached result must never be serialized alongside an error.
	return &types.MessageSearchResult{
		Total: 1,
		Items: []*types.MessageSearchGroupItem{{AnswerContent: "PRIVATE_PARTIAL_BODY_MUST_NEVER_BE_WRITTEN"}},
	}, s.fault
}

// Generate real SQLite driver faults through independent connections. No error
// value/Code is invented and no original message/assistant proof is fabricated.
func actualHistoryStorageFault(t *testing.T, kind string) (error, sqlite3.ErrNo) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "fault.db")
	shared := ""
	if kind == "locked" {
		shared = "&cache=shared"
	}
	writer, err := sql.Open("sqlite3", "file:"+path+"?_busy_timeout=1"+shared)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, writer.Close()) })
	writer.SetMaxOpenConns(1)
	_, err = writer.Exec("CREATE TABLE fault_probe(id INTEGER PRIMARY KEY)")
	require.NoError(t, err)
	if kind == "readonly" {
		reader, err := sql.Open("sqlite3", "file:"+path+"?mode=ro&_busy_timeout=1")
		require.NoError(t, err)
		defer reader.Close()
		_, err = reader.Exec("INSERT INTO fault_probe(id) VALUES(1)")
		require.Error(t, err)
		return err, sqlite3.ErrReadonly
	}
	conn, err := writer.Conn(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() { _, _ = conn.ExecContext(context.Background(), "ROLLBACK"); require.NoError(t, conn.Close()) })
	_, err = conn.ExecContext(context.Background(), "BEGIN IMMEDIATE")
	require.NoError(t, err)
	_, err = conn.ExecContext(context.Background(), "INSERT INTO fault_probe(id) VALUES(1)")
	require.NoError(t, err)
	contender, err := sql.Open("sqlite3", "file:"+path+"?_busy_timeout=1"+shared)
	require.NoError(t, err)
	defer contender.Close()
	if kind == "locked" {
		var id int
		err = contender.QueryRow("SELECT id FROM fault_probe").Scan(&id)
		require.Error(t, err)
		return err, sqlite3.ErrLocked
	}
	_, err = contender.Exec("INSERT INTO fault_probe(id) VALUES(2)")
	require.Error(t, err)
	return err, sqlite3.ErrBusy
}

func TestMessageHistoryActualSQLiteStorageFaultReturnsUnavailableWithoutBody(t *testing.T) {
	gin.SetMode(gin.TestMode)
	t.Run("native-missing-current-control-tables", func(t *testing.T) {
		sqlite_vec.Auto()
		db, err := gorm.Open(
			gormsqlite.Open(filepath.Join(t.TempDir(), "native-empty.db")),
			&gorm.Config{Logger: gormlogger.Default.LogMode(gormlogger.Silent)},
		)
		require.NoError(t, err)
		sqlDB, err := db.DB()
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, sqlDB.Close()) })
		native, ok := sqlindex.NewSQLiteRetrieveEngineRepository(db).(interfaces.NativeVectorCandidateRetriever)
		require.True(t, ok)
		// The actual index namespace exists and is empty; current authority tables
		// are missing. This state is unavailable, not a complete zero-result page.
		require.NoError(
			t,
			db.Exec(
				"CREATE VIRTUAL TABLE vec_embeddings_2 USING vec0(embedding float[2] distance_metric=cosine)",
			).Error,
		)
		require.True(t, db.Migrator().HasTable("lite_embeddings"))
		require.False(t, db.Migrator().HasTable("knowledges"))
		require.False(t, db.Migrator().HasTable("knowledge_bases"))
		rows, fault := native.RetrieveVectorCandidates(
			types.WithExecutionTenant(context.Background(), 7),
			types.VectorCandidateParams{
				TenantID:         7,
				KnowledgeBaseIDs: []string{"owned-kb-selector"},
				KnowledgeIDs:     []string{"owned-knowledge-selector"},
				Embedding:        []float32{1, 0},
				TopK:             1,
			},
		)
		require.Nil(t, rows)
		require.ErrorIs(t, fault, types.ErrVectorCandidateRetrievalUnavailable)
		for _, knownDenied := range []bool{false, true} {
			returned := fmt.Errorf("history native capability: %w", fault)
			expected := http.StatusServiceUnavailable
			if knownDenied {
				returned = errors.Join(access.ErrNextcloudPublicationDenied, returned)
				expected = http.StatusForbidden
			}
			router := gin.New()
			router.Use(middleware.ErrorHandler())
			router.POST(
				"/api/v1/messages/search",
				NewMessageHandler(historyActualStorageFaultService{fault: returned}, nil, nil).SearchMessages,
			)
			request := httptest.NewRequest(
				http.MethodPost,
				"/api/v1/messages/search",
				strings.NewReader(`{"query":"owned user query","mode":"hybrid"}`),
			)
			request.Header.Set("Content-Type", "application/json")
			response := httptest.NewRecorder()
			router.ServeHTTP(response, request)
			require.Equal(t, expected, response.Code)
			require.NotContains(t, response.Body.String(), "PRIVATE_PARTIAL_BODY_MUST_NEVER_BE_WRITTEN")
			require.NotContains(t, response.Body.String(), fault.Error())
			require.NotContains(t, response.Body.String(), `"success":true`)
		}
	})
	for _, kind := range []string{"busy", "locked", "readonly"} {
		t.Run(kind, func(t *testing.T) {
			fault, code := actualHistoryStorageFault(t, kind)
			var actual sqlite3.Error
			require.True(t, errors.As(fault, &actual))
			require.Equal(t, code, actual.Code, "prove this is the intended actual driver condition")
			for _, knownDenied := range []bool{false, true} {
				returned := fmt.Errorf("history storage checkpoint: %w", fault)
				expected := http.StatusServiceUnavailable
				if knownDenied {
					returned = errors.Join(access.ErrNextcloudPublicationDenied, returned)
					expected = http.StatusForbidden
				}
				router := gin.New()
				router.Use(middleware.ErrorHandler())
				router.POST(
					"/api/v1/messages/search",
					NewMessageHandler(historyActualStorageFaultService{fault: returned}, nil, nil).SearchMessages,
				)
				request := httptest.NewRequest(
					http.MethodPost,
					"/api/v1/messages/search",
					strings.NewReader(`{"query":"owned user query","mode":"hybrid"}`),
				)
				request.Header.Set("Content-Type", "application/json")
				response := httptest.NewRecorder()
				router.ServeHTTP(response, request)
				require.Equal(t, expected, response.Code)
				require.NotContains(t, response.Body.String(), "PRIVATE_PARTIAL_BODY_MUST_NEVER_BE_WRITTEN")
				require.NotContains(
					t,
					response.Body.String(),
					fault.Error(),
					"driver details are not public history output",
				)
			}
		})
	}
}
