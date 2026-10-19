package session

import (
	"crypto/sha256"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestNativeLiveMetadataPeekExpiryAndBodyIdentityRecheck(t *testing.T) {
	for _, mode := range []string{"expired_after_peek", "body_changed_after_peek", "lineage_changed_after_peek", "closed_after_peek"} {
		t.Run(mode, func(t *testing.T) {
			body := []byte(`{"content":"native actual producer bytes"}`)
			entry := nativeLiveFrame{
				Sequence: 1,
				Body:     body,
				Digest:   sha256.Sum256(body),
				Parents:  []byte(`[{"metadata_digest":"original"}]`),
				Lineage:  []byte(`{"version":1,"state":"complete","sources":[]}`),
				Expires:  time.Now().Add(time.Minute),
			}
			run := &nativeLiveRun{frames: []nativeLiveFrame{entry}, expires: time.Now().Add(time.Minute)}
			metadata, err := run.read(0)
			require.NoError(t, err)
			require.Len(t, metadata, 1)
			require.Nil(t, metadata[0].Body, "metadata planning must not copy raw native frame bytes before leases")
			switch mode {
			case "expired_after_peek":
				run.frames[0].Expires = time.Now().Add(-time.Second)
			case "body_changed_after_peek":
				run.frames[0].Body = []byte(`{"content":"foreign bytes"}`)
			case "lineage_changed_after_peek":
				run.frames[0].Lineage = []byte(`{"version":1,"state":"unknown","sources":[]}`)
			case "closed_after_peek":
				eraseNativeLiveRun(run)
			}
			_, err = run.readBody(metadata[0])
			require.Error(t, err, "an old metadata plan must never certify changed or expired native bytes")
			if mode == "closed_after_peek" {
				require.Empty(t, run.frames)
				require.Zero(t, run.bytes)
			}
		})
	}
}
