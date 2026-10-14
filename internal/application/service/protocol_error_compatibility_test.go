package service

import (
	"errors"
	"fmt"
	"testing"

	apperrors "github.com/Tencent/WeKnora/internal/errors"
	"github.com/Tencent/WeKnora/internal/types"
)

func TestServiceProtocolSentinelsKeepControlDisplayAndIdentity(t *testing.T) {
	// These are literal expectations from the immutable 6047 control.
	for _, row := range []struct {
		name       string
		err        error
		public     string
		diagnostic string
	}{
		{
			name:       "derived content",
			err:        ErrNextcloudDerivedContent,
			public:     "Nextcloud source content cannot be copied, shared, or synthesized",
			diagnostic: "nextcloud source content cannot be copied, shared, or synthesized",
		},
		{
			name:       "private stream cancellation",
			err:        errNextcloudStreamCanceled,
			public:     "Nextcloud stream canceled",
			diagnostic: "nextcloud stream canceled",
		},
	} {
		t.Run(row.name, func(t *testing.T) {
			if got := apperrors.PublicMessage(row.err); got != row.public {
				t.Fatalf("public message bytes changed: %q", got)
			}
			if row.err.Error() != row.diagnostic {
				t.Fatalf("internal diagnostic changed: %q", row.err.Error())
			}
			if !errors.Is(fmt.Errorf("service context: %w", row.err), row.err) {
				t.Fatal("service sentinel identity was lost")
			}
		})
	}
}

func TestDerivedPolicyNestedContextKeepsExactControlDisplay(t *testing.T) {
	_, child := isNextcloudKnowledge(&types.Knowledge{Metadata: types.JSON("{")})
	if child == nil || !errors.Is(child, ErrNextcloudDerivedContent) {
		t.Fatal("malformed metadata lost the derived-content sentinel")
	}
	const childPublic = ("Nextcloud source content cannot be copied, shared, or s" +
		"ynthesized: malformed knowledge metadata")
	const childDiagnostic = ("nextcloud source content cannot be copied, shared, or s" +
		"ynthesized: malformed knowledge metadata")
	if apperrors.PublicMessage(child) != childPublic || child.Error() != childDiagnostic {
		t.Fatalf("real policy context changed: public=%q diagnostic=%q", apperrors.PublicMessage(child), child.Error())
	}
	outerCause := fmt.Errorf("outer policy lookup: %w", child)
	outer := apperrors.NewProtocolError(outerCause,
		fmt.Sprintf("outer policy lookup: %s", apperrors.PublicMessage(child)))
	const public = ("outer policy lookup: Nextcloud source content cannot be" +
		" copied, shared, or synthesized: malformed knowledge me" +
		"tadata")
	const diagnostic = ("outer policy lookup: nextcloud source content cannot be" +
		" copied, shared, or synthesized: malformed knowledge me" +
		"tadata")
	if apperrors.PublicMessage(outer) != public || outer.Error() != diagnostic {
		t.Fatalf("nested context changed: public=%q diagnostic=%q", apperrors.PublicMessage(outer), outer.Error())
	}
	if !errors.Is(outer, ErrNextcloudDerivedContent) || !errors.Is(outer, outerCause) {
		t.Fatal("nested wrapping lost sentinel or immediate cause identity")
	}
}
