package errors

// ProtocolError separates an internal Go error from an already-public protocol
// message. Callers must construct the cause using normal Go error conventions
// and supply only the same message bytes that their protocol already exposed.
// It does not add a status, code, details, or any automatic capitalization rule.
type ProtocolError struct {
	cause         error
	publicMessage string
}

// NewProtocolError retains an explicit internal cause and its existing public
// display message. Formatting both values remains the caller's responsibility.
func NewProtocolError(cause error, publicMessage string) *ProtocolError {
	return &ProtocolError{cause: cause, publicMessage: publicMessage}
}

// Error returns the internal diagnostic without rewriting or hiding its cause.
func (e *ProtocolError) Error() string {
	if e == nil {
		return ""
	}
	if e.cause == nil {
		return "protocol error"
	}
	return e.cause.Error()
}

// Unwrap preserves the original cause and its errors.Is/errors.As identity.
func (e *ProtocolError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.cause
}

// PublicMessage returns only the explicitly supplied existing protocol message.
func (e *ProtocolError) PublicMessage() string {
	if e == nil {
		return ""
	}
	return e.publicMessage
}

// PublicMessage selects a display message only on this exact error layer.
// Ordinary or outer contextual errors retain Error(); looking through their
// wrappers would discard context and potentially expose a different message.
func PublicMessage(err error) string {
	if err == nil {
		return ""
	}
	if display, ok := err.(interface{ PublicMessage() string }); ok {
		return display.PublicMessage()
	}
	return err.Error()
}
