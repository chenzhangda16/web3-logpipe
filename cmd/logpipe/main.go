package main

import (
	"context"

	"github.com/chenzhangda16/web3-logpipe/internal/logpipe/fetcher"
	"github.com/chenzhangda16/web3-logpipe/internal/logpipe/model"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rawTxCh := make(chan model.Tx, 1024)

	// 启动 fetcher
	fetcher.StartFetchLoop(ctx, rawTxCh)

	// 这里接 rawTxCh，丢给 Scheduler 分 shard，再丢给 worker pool
	// ...
}
