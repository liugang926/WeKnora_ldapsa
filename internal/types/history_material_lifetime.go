package types

import (
	"context"
	"sync"
)

type historyMaterialLifetimeKey struct{}

// HistoryMaterialLifetime is a request-local actual material checker. It does not grant source or actor
// access; those checks remain independent in each original producer guard.
type HistoryMaterialLifetime struct {
	Check func() error
	Close func()
	once  sync.Once
}

// WithHistoryMaterialLifetime attaches request-local material checks and cleanup.
func WithHistoryMaterialLifetime(ctx context.Context, check func() error, closeMaterial func()) context.Context {
	return context.WithValue(
		ctx,
		historyMaterialLifetimeKey{},
		&HistoryMaterialLifetime{Check: check, Close: closeMaterial},
	)
}

// CheckHistoryMaterialLifetime checks any attached material lifetime.
func CheckHistoryMaterialLifetime(ctx context.Context) error {
	holder, _ := ctx.Value(historyMaterialLifetimeKey{}).(*HistoryMaterialLifetime)
	if holder == nil {
		return nil
	}
	if holder.Check == nil {
		return ErrSourceLineageUnknown
	}
	return holder.Check()
}

// CloseHistoryMaterialLifetime closes attached material once.
func CloseHistoryMaterialLifetime(ctx context.Context) {
	holder, _ := ctx.Value(historyMaterialLifetimeKey{}).(*HistoryMaterialLifetime)
	if holder != nil {
		holder.once.Do(func() {
			if holder.Close != nil {
				holder.Close()
			}
		})
	}
}
