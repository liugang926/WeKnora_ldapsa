package errors

import (
	stderrors "errors"
	"fmt"
	"testing"
)

func TestProtocolErrorPreservesDiagnosticPublicMessageAndCause(t *testing.T) {
	sentinel := stderrors.New("source response rejected")
	cause := fmt.Errorf("nextcloud source request: %w", sentinel)
	const display = "Nextcloud source request: source response rejected"
	err := NewProtocolError(cause, display)
	if err.Error() != "nextcloud source request: source response rejected" {
		t.Fatalf("internal diagnostic changed: %q", err.Error())
	}
	if err.PublicMessage() != display || PublicMessage(err) != display {
		t.Fatalf("public display bytes changed: %q", PublicMessage(err))
	}
	if err.Unwrap() != cause || !stderrors.Is(err, cause) || !stderrors.Is(err, sentinel) {
		t.Fatal("original cause or wrapped sentinel identity was lost")
	}
	var typed *ProtocolError
	if !stderrors.As(err, &typed) || typed != err {
		t.Fatal("protocol error identity was lost")
	}
}

func TestPublicMessageDoesNotStripOuterContext(t *testing.T) {
	cause := stderrors.New("source response rejected")
	inner := NewProtocolError(cause, "Original public source response rejected")
	outer := fmt.Errorf("retry operation failed: %w", inner)
	if PublicMessage(outer) != outer.Error() ||
		PublicMessage(outer) != "retry operation failed: source response rejected" {
		t.Fatalf("outer context was stripped: %q", PublicMessage(outer))
	}
	if !stderrors.Is(outer, cause) {
		t.Fatal("outer wrapping no longer retains the cause")
	}
	ordinary := stderrors.New("ordinary diagnostic")
	if PublicMessage(ordinary) != ordinary.Error() {
		t.Fatal("ordinary error unexpectedly selected a public field")
	}
}

func TestPublicMessageAndProtocolErrorAreNilSafe(t *testing.T) {
	var typed *ProtocolError
	if PublicMessage(nil) != "" || PublicMessage(typed) != "" || typed.Error() != "" || typed.Unwrap() != nil {
		t.Fatal("nil error handling changed")
	}
	err := NewProtocolError(nil, "Existing public message")
	if err.Error() != "protocol error" || err.Unwrap() != nil || PublicMessage(err) != "Existing public message" {
		t.Fatal("nil cause did not retain its explicit display safely")
	}
}
