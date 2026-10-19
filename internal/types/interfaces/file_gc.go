package interfaces

import "context"

// Dedicated exact local unlink. Implementations reject linked paths/objects
// and do not mutate catalog rows; the GC transaction owns the receipt.
type NextcloudPhysicalGCDeleter interface {
	DeleteNextcloudObject(context.Context, string) (bool, error)
}
