package repository

import (
	"errors"
	"fmt"
	"testing"

	apperrors "github.com/Tencent/WeKnora/internal/errors"
)

func TestRepositoryProtocolSentinelsKeepControlDisplayAndIdentity(t *testing.T) {
	// These are the exact public bytes from the immutable 6047 control.
	for _, row := range []struct {
		name       string
		err        error
		public     string
		diagnostic string
	}{
		{
			name:       "ask target",
			err:        ErrNextcloudAskTargetUnavailable,
			public:     "Nextcloud ask target unavailable",
			diagnostic: "nextcloud ask target unavailable",
		},
		{
			name:       "closed content fence",
			err:        ErrNextcloudContentLeaseDenied,
			public:     "Nextcloud content fence is closed",
			diagnostic: "nextcloud content fence is closed",
		},
		{
			name:       "expired content lease",
			err:        ErrNextcloudContentLeaseExpired,
			public:     "Nextcloud content lease expired or released",
			diagnostic: "nextcloud content lease expired or released",
		},
		{
			name:       "busy collection fence",
			err:        ErrNextcloudContentGCBusy,
			public:     "Nextcloud content GC is blocked by an active lease or claim",
			diagnostic: "nextcloud content GC is blocked by an active lease or claim",
		},
		{
			name:       "unproven coverage",
			err:        ErrNextcloudContentGCUncovered,
			public:     "Nextcloud content lease protocol coverage is unproven",
			diagnostic: "nextcloud content lease protocol coverage is unproven",
		},
		{
			name:       "event credential",
			err:        ErrNextcloudEventUnauthorized,
			public:     "Nextcloud event credential is invalid",
			diagnostic: "nextcloud event credential is invalid",
		},
		{
			name:       "event scope",
			err:        ErrNextcloudEventScope,
			public:     "Nextcloud event connection is no longer bound to this data source",
			diagnostic: "nextcloud event connection is no longer bound to this data source",
		},
		{
			name:       "event inbox conflict",
			err:        ErrNextcloudEventConflict,
			public:     "Nextcloud event batch conflicts with the durable inbox",
			diagnostic: "nextcloud event batch conflicts with the durable inbox",
		},
		{
			name:       "lineage snapshot store",
			err:        ErrNextcloudLineageSnapshotStore,
			public:     "Nextcloud lineage snapshot storage unavailable",
			diagnostic: "nextcloud lineage snapshot storage unavailable",
		},
	} {
		t.Run(row.name, func(t *testing.T) {
			if got := apperrors.PublicMessage(row.err); got != row.public {
				t.Fatalf("public message bytes changed: %q", got)
			}
			if row.err.Error() != row.diagnostic {
				t.Fatalf("internal diagnostic changed: %q", row.err.Error())
			}
			if !errors.Is(fmt.Errorf("repository context: %w", row.err), row.err) {
				t.Fatal("repository sentinel identity was lost")
			}
		})
	}
}
