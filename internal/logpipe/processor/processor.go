package processor

import (
	"context"
	"log"
	"strings"
	"time"

	"github.com/IBM/sarama"
	"github.com/chenzhangda16/web3-logpipe/internal/logpipe/out"
	"github.com/chenzhangda16/web3-logpipe/internal/logpipe/window"

	"github.com/chenzhangda16/web3-logpipe/internal/logpipe/dispatcher"
	"github.com/chenzhangda16/web3-logpipe/internal/logpipe/ids"
	"github.com/chenzhangda16/web3-logpipe/internal/logpipe/ingest"
)

type Config struct {
	ReadyFifo string
	Brokers   string
	Group     string
	Topic     string

	SpoolPath    string
	DecodeWorker int
	DecodeQueue  int

	CheckpointPath string // 暂时还能留着（以后迁移）
	// WindowSec/GapSec 先留着，后面下游再用
}

type Processor struct {
	cfg Config

	cons     *Consumer
	client   sarama.Client
	spool    ingest.Spool
	disp     *dispatcher.Dispatcher
	ingestor *ingest.Ingestor
	wins     []*window.Runner
}

func New(cfg Config) (*Processor, error) {
	sp, err := ingest.NewFileSpool(cfg.SpoolPath)

	disp := dispatcher.NewDispatcher(16)
	addrs := ids.NewAddressID(64, 1<<12)
	tokens := ids.NewTokenID(32, 1<<10)
	adapter := ingest.NewMockChainAdapter(addrs, tokens)

	client, err := sarama.NewClient(strings.Split(cfg.Brokers, ","), sarama.NewConfig())
	if err != nil {
		return nil, err
	}

	cons, err := NewConsumerWithClient(client, cfg.Group)
	if err != nil {
		_ = client.Close()
		return nil, err
	}

	ig := ingest.NewIngestor(cfg.ReadyFifo, disp, sp, cfg.DecodeWorker, cfg.DecodeQueue, adapter, client, cfg.Topic)

	sink, _ := out.NewKafkaSink([]string{"127.0.0.1:9092"}, "logpipe.out", sarama.NewConfig())

	allOpen := false

	wins := []*window.Runner{
		window.NewRunner(0, disp, sink, &allOpen, false, &window.EmitTick{Every: 50}),
		window.NewRunner(1, disp, sink, &allOpen, false, &window.EmitTick{Every: 200}),
		window.NewRunner(2, disp, sink, &allOpen, false, &window.EmitTick{Every: 1000}),
		window.NewRunner(3, disp, sink, &allOpen, true, &window.EmitTick{Every: 5000}),
	}

	return &Processor{
		cfg:      cfg,
		cons:     cons,
		spool:    sp,
		disp:     disp,
		ingestor: ig,
		wins:     wins,
	}, nil
}

func (p *Processor) Close() error {
	if p.ingestor != nil {
		_ = p.ingestor.Close()
	}
	if p.cons != nil {
		_ = p.cons.Close()
	}
	if p.client != nil {
		_ = p.client.Close()
	}
	if p.spool != nil {
		_ = p.spool.Close()
	}
	return nil
}

func (p *Processor) Run(ctx context.Context) error {
	// 1) 启动窗口实例（跟随 ctx 生命周期）
	for _, w := range p.wins {
		w := w
		go func() {
			if err := w.Run(ctx); err != nil {
				log.Printf("[processor] window runner exit: %v", err)
			}
		}()
	}

	// 2) 启动 Kafka consume 主循环
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
