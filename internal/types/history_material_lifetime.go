package types

import (
	"context"
	"sync"
)

type historyMaterialLifetimeKey struct{}

// A request-local actual material checker. It does not grant source or actor
// access; those checks remain independent in each original producer guard.
type HistoryMaterialLifetime struct {
	Check func() error
	Close func()
	once  sync.Once
}

func WithHistoryMaterialLifetime(ctx context.Context, check func() error, close func()) context.Context {
	return context.WithValue(ctx, historyMaterialLifetimeKey{}, &HistoryMaterialLifetime{Check: check, Close: close})
}
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
