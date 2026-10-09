package types

// NextcloudKnowledgeBaseDeletionBlockedError preserves the established public
// service Error() contract for this one domain refusal. Its explicitly supplied
// internal cause follows Go diagnostic conventions and remains available through
// Unwrap. This is not an AppError and adds no HTTP status, code or details.
type NextcloudKnowledgeBaseDeletionBlockedError struct {
	cause error
}

// NewNextcloudKnowledgeBaseDeletionBlockedError retains the original cause
// without changing its identity or formatting its diagnostic.
func NewNextcloudKnowledgeBaseDeletionBlockedError(cause error) *NextcloudKnowledgeBaseDeletionBlockedError {
	return &NextcloudKnowledgeBaseDeletionBlockedError{cause: cause}
}

// Error returns the exact already-public service refusal, not the diagnostic.
func (e *NextcloudKnowledgeBaseDeletionBlockedError) Error() string {
	if e == nil {
		return ""
	}
	return "Nextcloud source must be unpaired before deleting its knowledge base"
}

// Unwrap preserves the explicitly supplied lower diagnostic cause.
func (e *NextcloudKnowledgeBaseDeletionBlockedError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.cause
}

// PublicMessage preserves the same domain contract for HTTP/Agent adapters.
func (e *NextcloudKnowledgeBaseDeletionBlockedError) PublicMessage() string {
	return e.Error()
}
