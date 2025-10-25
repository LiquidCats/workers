package workers

import "context"

type Handler[T any] interface {
	Handle(ctx context.Context, data T)
}
