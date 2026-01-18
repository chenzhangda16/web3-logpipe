package processor

import (
	"context"
	"log"
	"time"

	"github.com/chenzhangda16/web3-logpipe/internal/logpipe/dispatcher"
	"github.com/chenzhangda16/web3-logpipe/internal/logpipe/ids"
	"github.com/chenzhangda16/web3-logpipe/internal/logpipe/ingest"
)

type Config struct {
	Brokers string
	Group   string
	Topic   string

	SpoolPath    string
	DecodeWorker int
	DecodeQueue  int

	CheckpointPath string // 暂时还能留着（以后迁移）
	// WindowSec/GapSec 先留着，后面下游再用
}

type Processor struct {
	cfg Config

	cons *Consumer

	// 上游新组件
	spool    ingest.Spool
	disp     *dispatcher.Dispatcher
	ingestor *ingest.Ingestor
}

func New(cfg Config) (*Processor, error) {
	cons, err := NewConsumer(cfg.Brokers, cfg.Group, cfg.Topic)
	if err != nil {
		return nil, err
	}

	sp, err := ingest.NewFileSpool(cfg.SpoolPath)
	if err != nil {
		_ = cons.Close()
		return nil, err
	}

	disp := dispatcher.NewDispatcher(1024) // 先给个 cap
	addrs := ids.NewAddressID(64, 1<<12)
	tokens := ids.NewTokenID(32, 1<<10)
	adapter := ingest.NewMockChainAdapter(addrs, tokens)

	ig := ingest.NewIngestor(disp, sp, cfg.DecodeWorker, cfg.DecodeQueue, adapter)

	return &Processor{
		cfg:      cfg,
		cons:     cons,
		spool:    sp,
		disp:     disp,
		ingestor: ig,
	}, nil
}

func (p *Processor) Close() error {
	_ = p.ingestor.Close()
	return p.cons.Close()
}

func (p *Processor) Run(ctx context.Context) error {
	// ingestor 本身实现 sarama.ConsumerGroupHandler
	for {
		if err := p.cons.group.Consume(ctx, []string{p.cfg.Topic}, p.ingestor); err != nil {
			log.Printf("[processor] consume err: %v", err)
			time.Sleep(300 * time.Millisecond)
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
	}
}
