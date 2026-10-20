package interfaces

import "context"

// CurrentReadActorRefresher reconstructs current authority for the original
// authenticated actor selectors. It never substitutes a resource owner.
type CurrentReadActorRefresher interface {
	RefreshCurrentReadActor(context.Context) (context.Context, error)
}
