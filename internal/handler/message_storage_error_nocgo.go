//go:build !cgo

package handler

// The CGO-disabled sqlite3 driver has no typed runtime errors. Keep non-SQLite
// failures on the ordinary handler path; current permission/material checks
// retain their existing classification before this helper is reached.
func messageHistoryStorageUnavailable(_ error) bool {
	return false
}
