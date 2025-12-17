package main

import "context"

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rawTxCh := make(chan Tx, 1024)

	// 启动 fetcher
	StartFetchLoop(ctx, rawTxCh)

	// 这里接 rawTxCh，丢给 Scheduler 分 shard，再丢给 worker pool
	// ...
}
