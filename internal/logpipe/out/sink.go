package out

import "context"

type Sink interface {
	Emit(ctx context.Context, typ string, v any) error
	Close() error
}
