package handler

import (
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/google/uuid"
	"github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

func TestSteerOrderWithSQLiteFailure(t *testing.T) {
	for _, failFirst := range []bool{false, true} {
		t.Run(fmt.Sprintf("fail_first_%t", failFirst), func(t *testing.T) {
			runSteerOrderIntegration(t, failFirst)
		})
	}
}

func runSteerOrderIntegration(t *testing.T, failFirst bool) {
	const older = "OLDER: use option A"
	const newer = "NEWER: replace A with option B"
	expected := types.SteerMessageContent(older) + "\n\n" + types.SteerMessageContent(newer)
	f := newActualSteerHTTPFixture(t, false)
	model := &actualSteerHTTPModel{
		t:                    t,
		f:                    f,
		steerID:              uuid.NewString(),
		expectedSteerContent: expected,
		ready:                make(chan struct{}),
		resume:               make(chan struct{}),
	}
	t.Cleanup(model.release)
	h, _, _, _ := newActualSteerHTTPHandler(t, f, model)
	router := actualSteerHTTPRouter(f.ctx, h)
	done := make(chan string, 1)
	go func() {
		res := actualSteerHTTPRequest(router, http.MethodPost, "/agent-chat/"+f.parent.SessionID, map[string]any{
			"query": "Choose an option.", "agent_id": "actual-steer-agent", "agent_enabled": true,
			"summary_model_id": "actual-http-tail-model", "disable_title": true,
		})
		done <- res.Body.String()
	}()
	select {
	case <-model.ready:
	case result := <-done:
		t.Fatalf("first model call missing: %s", result)
	case <-time.After(20 * time.Second):
		t.Fatal("timed out waiting for model")
	}
	var run types.Message
	require.NoError(t, f.db.Where("session_id=? AND role='assistant' AND request_id<>?",
		f.parent.SessionID, f.parent.RequestID).Take(&run).Error)
	for _, entry := range []struct{ id, query string }{{model.steerID, older}, {uuid.NewString(), newer}} {
		res := actualSteerHTTPRequest(router, http.MethodPost, "/sessions/"+f.parent.SessionID+"/steer", map[string]any{
			"steer_id": entry.id, "query": entry.query, "delivery": "inject", "expected_assistant_message_id": run.ID,
		})
		require.Equal(t, http.StatusOK, res.Code, res.Body.String())
		require.Contains(t, res.Body.String(), `"status":"queued"`)
	}
	var conflicted *atomic.Bool
	if failFirst {
		conflicted = conflictFirstSteerWrite(t, f.db)
	}
	model.release()
	select {
	case result := <-done:
		require.Contains(t, result, actualSteerHTTPAnswer)
	case <-time.After(20 * time.Second):
		t.Fatal("timed out waiting for the ordered corrections")
	}
	if failFirst {
		require.True(t, conflicted.Load(), "the first consume hit real SQLite write contention")
	}
	var users []types.Message
	require.NoError(t, f.db.Where("session_id=? AND request_id=? AND role='user'", run.SessionID, run.RequestID).
		Order("created_at").Find(&users).Error)
	require.Len(t, users, 3)
	require.Equal(t, []string{older, newer}, []string{users[1].Content, users[2].Content})
	require.NotEmpty(t, model.inputs)
	var wire []string
	for _, message := range model.inputs[len(model.inputs)-1] {
		if message.Content == expected {
			require.NoError(t, message.SourceLineage.RequireComplete())
			wire = append(wire, message.Content)
		}
	}
	require.Equal(t, []string{expected}, wire)
}

// Commit a separate WAL writer after the consume transaction read its snapshot.
// Its first insert then receives SQLITE_BUSY_SNAPSHOT and must start over.
func conflictFirstSteerWrite(t *testing.T, db *gorm.DB) *atomic.Bool {
	t.Helper()
	var database struct{ File string }
	require.NoError(t, db.Raw("PRAGMA database_list").Scan(&database).Error)
	other, err := sql.Open("sqlite3", database.File)
	require.NoError(t, err)
	t.Cleanup(func() { _ = other.Close() })
	_, err = other.Exec("CREATE TABLE steer_order_lock_fixture (id INTEGER PRIMARY KEY)")
	require.NoError(t, err)
	var armed, confirmed atomic.Bool
	compete := func(tx *gorm.DB) {
		if tx.Statement.Table == "message_generation_origins" && armed.CompareAndSwap(false, true) {
			_, err := other.Exec("INSERT INTO steer_order_lock_fixture VALUES (1)")
			if err != nil {
				_ = tx.AddError(err)
			}
		}
	}
	creates := db.Callback().Create()
	require.NoError(t, creates.Before("gorm:create").Register("test:compete_with_steer", compete))
	observe := func(tx *gorm.DB) {
		var e sqlite3.Error
		if tx.Statement.Table == "message_generation_origins" && errors.As(tx.Error, &e) && e.Code == sqlite3.ErrBusy {
			confirmed.Store(true)
		}
	}
	require.NoError(t, creates.After("gorm:create").Register("test:observe_write_conflict", observe))
	return &confirmed
}
