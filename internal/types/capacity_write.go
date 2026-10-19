package types

import (
	"context"
	"io"
)

// CapacityWriteSession records the exact server-admitted storage write lifecycle.
// Only server-side Nextcloud admission installs this context. It is not a
// reader principal and grants no permission to delete a physical resource.
type CapacityWriteSession interface {
	Token() string
	Backend() string
	RetainWriterLock(io.Closer) error
	PrepareLocal(context.Context, string, string, string, string, int64) error
	LocalCreated(context.Context, string, string) error
	PhysicalWritten(context.Context, string, int64, string) error
	ValidateCatalog(context.Context, string, string) error
	Cataloged(context.Context, string, string, string) error
}
type capacityWriteKey struct{}

// WithCapacityWriteSession attaches the server-admitted storage session.
func WithCapacityWriteSession(ctx context.Context, s CapacityWriteSession) context.Context {
	return context.WithValue(ctx, capacityWriteKey{}, s)
}

// CapacityWriteSessionFromContext returns the admitted write session, if present.
func CapacityWriteSessionFromContext(ctx context.Context) CapacityWriteSession {
	s, _ := ctx.Value(capacityWriteKey{}).(CapacityWriteSession)
	return s
}

// CapacityLocalObject describes one exact physical object during capacity reconciliation.
type CapacityLocalObject struct {
	Token, TenantKnowledgeID, BackendID, PhysicalPath, RootIdentity, ScopeID, ObjectIdentity string
	Size                                                                                     int64
	Exists                                                                                   bool
	SHA256                                                                                   string
}
