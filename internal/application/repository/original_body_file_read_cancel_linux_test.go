package repository

import (
	"bytes"
	"context"
	"encoding/binary"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
	"gorm.io/gorm"
)

// IN_ACCESS observes a real journal-key ReadFile operation after the payload query. The large
// valid owned bootstrap key gives cancellation a read/verification window;
// no old pin/proof is re-signed and no body/absence receipt is seeded.
func runOriginalBodyFileReadCancelActual(t *testing.T, pg bool) {
	db, store := bodyProtocolFixture(t, pg, bytes.Repeat([]byte("owned-bootstrap-cancel-key-"), 320000))
	bodyID := bodyProtocolCreate(t, db, "actual-read-cancel", "one")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	access := make(chan struct{})
	armed := false
	fd := -1
	defer func() {
		if fd >= 0 {
			_ = unix.Close(fd)
		}
	}()
	require.NoError(t, db.Callback().Query().After("gorm:query").Register("actual-journal-key-read-cancel", func(tx *gorm.DB) {
		if armed || tx.Statement.Table != "original_body_payloads" {
			return
		}
		armed = true
		var err error
		fd, err = unix.InotifyInit1(unix.IN_CLOEXEC)
		require.NoError(t, err)
		_, err = unix.InotifyAddWatch(fd, store.journal.paths.KeyPath, unix.IN_ACCESS)
		require.NoError(t, err)
		ready := make(chan struct{})
		go func() {
			close(ready)
			buf := make([]byte, 4096)
			n, err := unix.Read(fd, buf)
			if err == nil && n >= unix.SizeofInotifyEvent && binary.LittleEndian.Uint32(buf[4:8])&unix.IN_ACCESS != 0 {
				cancel()
				close(access)
			}
		}()
		<-ready
	}))
	defer db.Callback().Query().Remove("actual-journal-key-read-cancel")
	err := CheckOriginalBodyAvailableInTx(ctx, db, "knowledge_auto_tag_input_manifests", "actual-read-cancel")
	require.True(t, armed, "the payload query armed the real journal-key read observer")
	select {
	case <-access:
	case <-time.After(time.Second):
		t.Fatal("real journal-key read access was not observed")
	}
	require.ErrorIs(t, err, context.Canceled, "cancellation during the observed real journal-key file read cannot return body success")
	var state string
	require.NoError(t, bodyInternal(db).Table("original_body_payloads").Select("state").Where("id=?", bodyID).Scan(&state).Error)
	require.Equal(t, "live", state, "cancel rejection never fabricates a purge/absence receipt")
}

func TestOriginalBodyFileReadCancelActualSQLite(t *testing.T) {
	runOriginalBodyFileReadCancelActual(t, false)
}
func TestPostgresOriginalBodyFileReadCancelActual(t *testing.T) {
	runOriginalBodyFileReadCancelActual(t, true)
}
