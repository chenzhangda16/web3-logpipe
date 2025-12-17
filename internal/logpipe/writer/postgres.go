package writer

import "internal/logpipe/model"

type Store interface {
	WriteBatch(ctx context.Context, events []model.EnrichedEvent) error
}
