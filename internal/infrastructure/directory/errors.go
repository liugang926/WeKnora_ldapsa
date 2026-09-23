package directory

import (
	"errors"
	"fmt"
	"strings"
)

var (
	// ErrEmptyPassword rejects an LDAP bind without a password.
	ErrEmptyPassword = errors.New("directory password must not be empty")
	// ErrInvalidCredentials indicates a rejected user bind.
	ErrInvalidCredentials = errors.New("invalid directory credentials")
	// ErrInvalidServiceCredentials indicates a rejected service-account bind.
	ErrInvalidServiceCredentials = errors.New("invalid directory service credentials")
	// ErrUserNotFound indicates that the login was outside the selected scope.
	ErrUserNotFound = errors.New("directory user not found")
	// ErrAmbiguousUser indicates that multiple directory entries matched a login.
	ErrAmbiguousUser = errors.New("directory login matched multiple users")
	// ErrUserDisabled indicates that the directory account cannot authenticate.
	ErrUserDisabled = errors.New("directory user is disabled")
	// ErrIncompleteResults prevents a partial directory result from being applied.
	ErrIncompleteResults = errors.New("directory search result is incomplete")
	// ErrInvalidDirectoryObject identifies malformed directory data.
	ErrInvalidDirectoryObject = errors.New("invalid directory object")
	// ErrDuplicateDirectoryObject identifies conflicting directory identities.
	ErrDuplicateDirectoryObject = errors.New("duplicate directory object")
	// ErrInvalidMembershipGraph identifies inconsistent group relationships.
	ErrInvalidMembershipGraph = errors.New("invalid directory membership graph")
	// ErrMembershipCycle identifies a loop in the group hierarchy.
	ErrMembershipCycle = errors.New("directory group membership cycle")
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
