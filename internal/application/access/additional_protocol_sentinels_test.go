package access

import (
	"errors"
	"fmt"
	"testing"

	apperrors "github.com/Tencent/WeKnora/internal/errors"
)

func TestAdditionalPublicationProtocolSentinelContract(t *testing.T) {
	// Public bytes are fixed from immutable aa666/ede0, not the edited source.
	const public = "Nextcloud publication access denied"
	const diagnostic = "nextcloud publication access denied"
	sentinel := ErrNextcloudPublicationDenied
	if got := apperrors.PublicMessage(sentinel); got != public {
		t.Fatalf("public sentinel bytes changed: %q", got)
	}
	if got := sentinel.Error(); got != diagnostic {
		t.Fatalf("internal sentinel diagnostic changed: %q", got)
	}
	immediate := fmt.Errorf("publication context: %w", sentinel)
	wrapped := apperrors.NewProtocolError(immediate, "publication context: "+public)
	if got := wrapped.Error(); got != "publication context: nextcloud publication access denied" {
		t.Fatalf("wrapped internal diagnostic changed: %q", got)
	}
	if got := apperrors.PublicMessage(wrapped); got != "publication context: Nextcloud publication access denied" {
		t.Fatalf("wrapped public bytes changed: %q", got)
	}
	if wrapped.Unwrap() != immediate || !errors.Is(wrapped, immediate) || !errors.Is(wrapped, sentinel) {
		t.Fatal("immediate cause or original publication sentinel identity was lost")
	}
	outer := fmt.Errorf("outer context: %w", wrapped)
	if !errors.Is(outer, wrapped) || !errors.Is(outer, immediate) || !errors.Is(outer, sentinel) {
		t.Fatal("nested wrapping lost the original publication sentinel identity")
	}
}
