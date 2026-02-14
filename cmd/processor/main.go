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
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds | log.Lshortfile)
	var (
		brokers = flag.String("brokers", "127.0.0.1:9092", "kafka brokers csv")
		group   = flag.String("group", "logpipe-processor", "kafka consumer group")
		topic   = flag.String("topic", "mockchain.blocks", "topic to consume blocks")

		spoolPath    = flag.String("spool", "./data/spool.wal", "spool WAL path (ingest barrier)")
		decodeWorker = flag.Int("decode-worker", 4, "number of decode workers")
		decodeQueue  = flag.Int("decode-queue", 8192, "decode queue size")

		ckptPath  = flag.String("ckpt", "./data/processor.ckpt", "processor checkpoint path (reserved)")
		readyFifo = flag.String("ready-fifo", "./data/ready/processor.ready.fifo", "write one line to FIFO when ready")
	)
	flag.Parse()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	cfg := processor.Config{
		ReadyFifo: *readyFifo,
		Brokers:   *brokers,
		Group:     *group,
		Topic:     *topic,

		SpoolPath:    *spoolPath,
		DecodeWorker: *decodeWorker,
		DecodeQueue:  *decodeQueue,

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
