package repository

import (
	"errors"
	"fmt"
	"testing"

	apperrors "github.com/Tencent/WeKnora/internal/errors"
)

func TestAdditionalRepositoryProtocolSentinelContracts(t *testing.T) {
	// Every expected public string is fixed from immutable aa666/ede0.
	for _, row := range []struct {
		name       string
		sentinel   error
		public     string
		diagnostic string
	}{
		{
			name:       "ask_target_not_found",
			sentinel:   ErrNextcloudAskTargetNotFound,
			public:     "Nextcloud ask target not found",
			diagnostic: "nextcloud ask target not found",
		},
		{
			name:       "candidate_retry_not_due",
			sentinel:   ErrNextcloudCandidateRetryNotDue,
			public:     "Nextcloud failed candidate retry is not due",
			diagnostic: "nextcloud failed candidate retry is not due",
		},
		{
			name:       "event_connection_exists",
			sentinel:   ErrNextcloudEventConnectionExists,
			public:     "Nextcloud event connection already exists",
			diagnostic: "nextcloud event connection already exists",
		},
		{
			name:       "event_connection_missing",
			sentinel:   ErrNextcloudEventConnectionMissing,
			public:     "Nextcloud event connection is not active",
			diagnostic: "nextcloud event connection is not active",
		},
		{
			name:       "source_cursor",
			sentinel:   ErrNextcloudSourceCursor,
			public:     "Nextcloud source cursor requires manual review",
			diagnostic: "nextcloud source cursor requires manual review",
		},
		{
			name:     "event_rebind_unsafe",
			sentinel: ErrNextcloudEventRebindUnsafe,
			public: "Nextcloud event dispatch requires manual review before " +
				"source rotation rebind",
			diagnostic: "nextcloud event dispatch requires manual review before " +
				"source rotation rebind",
		},
		{
			name:       "lineage_snapshot_denied",
			sentinel:   ErrNextcloudLineageSnapshotDenied,
			public:     "Nextcloud lineage snapshot is unavailable or changed",
			diagnostic: "nextcloud lineage snapshot is unavailable or changed",
		},
		{
			name:       "source_pairing_conflict",
			sentinel:   ErrNextcloudSourcePairingConflict,
			public:     "Nextcloud source pairing conflicts with existing source",
			diagnostic: "nextcloud source pairing conflicts with existing source",
		},
		{
			name:       "source_pairing_missing",
			sentinel:   ErrNextcloudSourcePairingMissing,
			public:     "Nextcloud source pairing not found",
			diagnostic: "nextcloud source pairing not found",
		},
		{
			name:       "candidate_retry_manual",
			sentinel:   ErrNextcloudCandidateRetryManual,
			public:     "Nextcloud failed candidate requires administrator retry",
			diagnostic: "nextcloud failed candidate requires administrator retry",
		},
	} {
		t.Run(row.name, func(t *testing.T) {
			if got := apperrors.PublicMessage(row.sentinel); got != row.public {
				t.Fatalf("public sentinel bytes changed: %q", got)
			}
			if got := row.sentinel.Error(); got != row.diagnostic {
				t.Fatalf("internal sentinel diagnostic changed: %q", got)
			}
			immediate := fmt.Errorf("repository context: %w", row.sentinel)
			wrapped := apperrors.NewProtocolError(immediate, "repository context: "+row.public)
			if got := wrapped.Error(); got != "repository context: "+row.diagnostic {
				t.Fatalf("wrapped internal diagnostic changed: %q", got)
			}
			if got := apperrors.PublicMessage(wrapped); got != "repository context: "+row.public {
				t.Fatalf("wrapped public bytes changed: %q", got)
			}
			if wrapped.Unwrap() != immediate || !errors.Is(wrapped, immediate) || !errors.Is(wrapped, row.sentinel) {
				t.Fatal("immediate cause or original repository sentinel identity was lost")
			}
			outer := fmt.Errorf("outer context: %w", wrapped)
			if !errors.Is(outer, wrapped) || !errors.Is(outer, immediate) || !errors.Is(outer, row.sentinel) {
				t.Fatal("nested wrapping lost the original repository sentinel identity")
			}
		})
	}
}
