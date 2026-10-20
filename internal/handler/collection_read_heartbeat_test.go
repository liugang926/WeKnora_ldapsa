package handler

import (
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// Force the real durable heartbeat to overlap the actual capture/authorization
// path; the large fixture can cross its 30-second interval on slower CI hosts.
func TestKnowledgeCollectionHTTPHeartbeatDuringCaptureAndOutput(t *testing.T) {
	h, db, _, _, _, _ := collectionHTTPFixture(t)
	h.leaseHeartbeat = time.Millisecond
	var overlapped atomic.Bool
	require.NoError(
		t,
		db.Callback().Query().After("gorm:query").Register("collection-read-heartbeat-overlap", func(tx *gorm.DB) {
			_, inTransaction := tx.Statement.ConnPool.(gorm.TxCommitter)
			if inTransaction && tx.Statement.Table == "knowledge_bases" && overlapped.CompareAndSwap(false, true) {
				time.Sleep(40 * time.Millisecond)
			}
		}),
	)
	w := serveCollectionHTTP(h, "/knowledge-bases/kb/knowledge?page_size=1", false)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	var updated int64
	require.True(t, overlapped.Load())
	require.NoError(
		t,
		db.Table("nextcloud_content_leases").
			Where(
				"expires_at_ms>created_at_ms+? AND released_at_ms IS NOT NULL",
				nextcloudHTTPReadLeaseTTL.Milliseconds(),
			).
			Count(&updated).
			Error,
	)
	require.Positive(t, updated)
}
