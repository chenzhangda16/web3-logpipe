package scheduler

import "internal/logpipe/model"

type Scheduler struct {
	shardCount int
	outChans   []chan model.Tx
}

func (s *Scheduler) Start(ctx context.Context, in <-chan model.Tx) {}

func (s *Scheduler) shardForTx(tx model.Tx) int {
	// 比如按地址或 Token 做 hash
}
