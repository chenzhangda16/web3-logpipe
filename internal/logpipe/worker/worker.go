package worker

import "internal/logpipe/model"

type Worker struct {
	ID    int
	In    <-chan model.Tx
	Out   chan<- model.EnrichedEvent
	State *state.ShardState
}

func (w *Worker) Run(ctx context.Context) {}
