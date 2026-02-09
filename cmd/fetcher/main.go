package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/chenzhangda16/web3-logpipe/internal/logpipe/fetcher"
)

func main() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds | log.Lshortfile)
	var (
		// MockChain RPC base, e.g. http://127.0.0.1:18080
		rpcBase = flag.String("rpc", "http://127.0.0.1:18080", "mockchain rpc base url")

		// Kafka
		brokers = flag.String("brokers", "127.0.0.1:9092", "kafka brokers, comma-separated")
		topic   = flag.String("topic", "mockchain.blocks", "kafka topic to produce blocks")

		// Backfill window (seconds). -1 disables cold start backfill; will start from checkpoint or head.
		backfillSec = flag.Int64("backfill-sec", 86400, "cold start backfill window in seconds; -1 disables")

		// Pagination and pacing
		pageSize      = flag.Int("page", 200, "blocks per range request (keep small if block JSON is big)")
		pollHeadEvery = flag.Duration("poll-head", 2*time.Second, "how often to refresh head")
		idleSleep     = flag.Duration("idle-sleep", 300*time.Millisecond, "sleep when caught up")

		// Checkpoint
		ckptPath = flag.String("ckpt", "./data/fetcher.ckpt", "checkpoint file path")
	)
	flag.Parse()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	cfg := fetcher.Config{
		RPCBaseURL: *rpcBase,
		Brokers:    *brokers,
		Topic:      *topic,

		BackfillSec: *backfillSec,
		PageSize:    *pageSize,

		PollHeadEvery: *pollHeadEvery,
		IdleSleep:     *idleSleep,

		CheckpointPath: *ckptPath,
	}

	f, err := fetcher.New(cfg)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	if err := f.Run(ctx); err != nil && err != context.Canceled {
		log.Fatal(err)
	}
}
