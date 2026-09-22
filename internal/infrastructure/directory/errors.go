package directory

import (
	"errors"
	"fmt"
	"strings"
)

var (
	ErrEmptyPassword             = errors.New("directory password must not be empty")
	ErrInvalidCredentials        = errors.New("invalid directory credentials")
	ErrInvalidServiceCredentials = errors.New("invalid directory service credentials")
	ErrUserNotFound              = errors.New("directory user not found")
	ErrAmbiguousUser             = errors.New("directory login matched multiple users")
	ErrUserDisabled              = errors.New("directory user is disabled")
	ErrIncompleteResults         = errors.New("directory search result is incomplete")
	ErrInvalidDirectoryObject    = errors.New("invalid directory object")
	ErrDuplicateDirectoryObject  = errors.New("duplicate directory object")
	ErrInvalidMembershipGraph    = errors.New("invalid directory membership graph")
	ErrMembershipCycle           = errors.New("directory group membership cycle")
)

// ControllerAttempt is a redacted failure from one controller. It contains no
// bind password or user password.
type ControllerAttempt struct {
	ControllerURL string
	Err           error
}

// FailoverError reports that every controller failed operationally.
type FailoverError struct {
	Operation string
	Attempts  []ControllerAttempt
}

func (e *FailoverError) Error() string {
	if e == nil {
		return "directory failover failed"
	}
	parts := make([]string, 0, len(e.Attempts))
	for _, attempt := range e.Attempts {
		parts = append(parts, fmt.Sprintf("%s: %v", attempt.ControllerURL, attempt.Err))
	}
	return fmt.Sprintf("directory %s failed on all controllers: %s", e.Operation, strings.Join(parts, "; "))
}

func (e *FailoverError) Unwrap() []error {
	if e == nil {
		return nil
	}
	errs := make([]error, 0, len(e.Attempts))
	for _, attempt := range e.Attempts {
		errs = append(errs, attempt.Err)
	}
	return errs
}
