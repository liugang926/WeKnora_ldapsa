package handler

import (
	"errors"

	"github.com/mattn/go-sqlite3"
)

// A database fault cannot prove either current denial or successful absence.
// Match the driver's typed primary code, including wrapped/extended failures;
// keep malformed-query/constraint/programming failures on the ordinary path.
func messageHistoryStorageUnavailable(err error) bool {
	var fault sqlite3.Error
	if !errors.As(err, &fault) {
		var pointer *sqlite3.Error
		if !errors.As(err, &pointer) || pointer == nil {
			return false
		}
		fault = *pointer
	}
	switch fault.Code {
	case sqlite3.ErrBusy, sqlite3.ErrLocked, sqlite3.ErrReadonly:
		return true
	default:
		return false
	}
}
