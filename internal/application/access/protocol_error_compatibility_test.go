package access

import (
	"errors"
	"fmt"
	"testing"

	apperrors "github.com/Tencent/WeKnora/internal/errors"
)

func TestPublicationProtocolSentinelKeepsControlDisplayAndIdentity(t *testing.T) {
	const public = "Nextcloud publication authorization unavailable"
	const diagnostic = "nextcloud publication authorization unavailable"
	err := ErrNextcloudPublicationUnavailable
	if got := apperrors.PublicMessage(err); got != public {
		t.Fatalf("public message bytes changed: %q", got)
	}
	if err.Error() != diagnostic {
		t.Fatalf("internal diagnostic changed: %q", err.Error())
	}
	if !errors.Is(fmt.Errorf("publication context: %w", err), ErrNextcloudPublicationUnavailable) {
		t.Fatal("publication sentinel identity was lost")
	}
}
