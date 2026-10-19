package repository

import (
	"context"
	"encoding/binary"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
	"gorm.io/gorm"
)

// IN_ACCESS observes a real journal-key ReadFile operation after the payload query.
// The following actual header query waits for that observation and cancellation,
// establishing cancellation before the operation returns without a scheduling race.
// No old pin/proof is re-signed and no body/absence receipt is seeded.
func runOriginalBodyFileReadCancelActual(t *testing.T, pg bool) {
	db, store := bodyProtocolFixture(t, pg)
	bodyID := bodyProtocolCreate(t, db, "actual-read-cancel", "one")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	access := make(chan struct{})
	armed := false
	observedBeforeReturn := false
	fd := -1
	defer func() {
		if fd >= 0 {
			_ = unix.Close(fd)
		}
	}()
	require.NoError(
		t,
		db.Callback().Query().After("gorm:query").Register("actual-journal-key-read-cancel", func(tx *gorm.DB) {
			if armed {
				if !observedBeforeReturn && tx.Statement.Table == "original_body_field_heads" {
					select {
					case <-access:
						observedBeforeReturn = true
					case <-time.After(time.Second):
						t.Fatal("real journal-key read cancellation was not observed before the final header check")
					}
				}
				return
			}
			if tx.Statement.Table != "original_body_payloads" {
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
				if err == nil && n >= unix.SizeofInotifyEvent &&
					binary.LittleEndian.Uint32(buf[4:8])&unix.IN_ACCESS != 0 {
					cancel()
					close(access)
				}
			}()
			<-ready
		}),
	)
	defer func() { require.NoError(t, db.Callback().Query().Remove("actual-journal-key-read-cancel")) }()
	err := CheckOriginalBodyAvailableInTx(ctx, db, "knowledge_auto_tag_input_manifests", "actual-read-cancel")
	require.True(t, armed, "the payload query armed the real journal-key read observer")
	require.True(t, observedBeforeReturn, "the actual final header query observed cancellation before return")
	select {
	case <-access:
	case <-time.After(time.Second):
		t.Fatal("real journal-key read access was not observed")
	}
	require.ErrorIs(
		t,
		err,
		context.Canceled,
		"cancellation after the observed real journal-key file read cannot return body success",
	)
	var state string
	require.NoError(
		t,
		bodyInternal(db).Table("original_body_payloads").Select("state").Where("id=?", bodyID).Scan(&state).Error,
	)
	require.Equal(t, "live", state, "cancel rejection never fabricates a purge/absence receipt")
}

func TestOriginalBodyFileReadCancelActualSQLite(t *testing.T) {
	runOriginalBodyFileReadCancelActual(t, false)
}

func TestPostgresOriginalBodyFileReadCancelActual(t *testing.T) {
	runOriginalBodyFileReadCancelActual(t, true)
}
