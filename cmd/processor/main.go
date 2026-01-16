package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/chenzhangda16/web3-logpipe/internal/logpipe/processor"
)

func main() {
	var (
		brokers = flag.String("brokers", "127.0.0.1:9092", "kafka brokers csv")
		group   = flag.String("group", "logpipe-processor", "kafka consumer group")
		topic   = flag.String("topic", "mockchain.blocks", "topic to consume blocks")

		windowSec = flag.Int64("window-sec", 86400, "sliding window length in seconds")
		gapSec    = flag.Int64("gap-sec", 3, "allowed timestamp gap in seconds (for monotonic guard)")

		ckptPath = flag.String("ckpt", "./data/processor.ckpt", "processor checkpoint path")
	)
	flag.Parse()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	cfg := processor.Config{
		Brokers: *brokers,
		Group:   *group,
		Topic:   *topic,

		WindowSec: *windowSec,
		GapSec:    *gapSec,

		CheckpointPath: *ckptPath,
	}

	p, err := processor.New(cfg)
	if err != nil {
		log.Fatal(err)
	}
	defer p.Close()

	if err := p.Run(ctx); err != nil && err != context.Canceled {
		log.Fatal(err)
	}
	_ = time.Second
}
