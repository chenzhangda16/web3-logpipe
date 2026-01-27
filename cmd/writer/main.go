package main

import (
	"context"
	"encoding/json"
	"flag"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/IBM/sarama"

	"github.com/chenzhangda16/web3-logpipe/internal/logpipe/out"
	"github.com/chenzhangda16/web3-logpipe/internal/logpipe/writer"
)

type Handler struct {
	pg *writer.PGWriter
}

func (h *Handler) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (h *Handler) Cleanup(sarama.ConsumerGroupSession) error { return nil }

func (h *Handler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	ctx := sess.Context()
	for msg := range claim.Messages() {
		var env out.Envelope
		if err := json.Unmarshal(msg.Value, &env); err != nil {
			log.Printf("[writer] bad envelope: err=%v", err)
			sess.MarkMessage(msg, "")
			continue
		}

		switch env.Type {
		case "win_tick":
			var t out.WinTick
			if err := json.Unmarshal(env.Data, &t); err != nil {
				log.Printf("[writer] bad win_tick: err=%v", err)
				sess.MarkMessage(msg, "")
				continue
			}
			if err := h.pg.InsertWinTick(ctx, t); err != nil {
				// 这里返回 error 会让 claim 退出并触发重平衡；调试期你可能更想“打印并继续”
				log.Printf("[writer] insert failed: err=%v", err)
				// 不 mark，让它重试（至少一次）
				continue
			}
			sess.MarkMessage(msg, "")
		default:
			// 未知类型直接 mark 掉（或你想保留重试也行）
			sess.MarkMessage(msg, "")
		}
	}
	return nil
}

func main() {
	var (
		brokers = flag.String("brokers", "127.0.0.1:9092", "kafka brokers, comma separated")
		topic   = flag.String("topic", "logpipe.out", "out topic")
		group   = flag.String("group", "logpipe.writer", "consumer group")
	)
	flag.Parse()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		ch := make(chan os.Signal, 2)
		signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
		<-ch
		cancel()
	}()

	pg, err := writer.NewPGWriterFromEnv()
	if err != nil {
		log.Fatalf("pg init failed: %v", err)
	}
	defer func() { _ = pg.Close() }()

	if err := pg.EnsureSchema(ctx); err != nil {
		log.Fatalf("ensure schema failed: %v", err)
	}

	cfg := sarama.NewConfig()
	cfg.Version = sarama.V2_1_0_0
	cfg.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRange
	cfg.Consumer.Offsets.Initial = sarama.OffsetOldest

	cg, err := sarama.NewConsumerGroup(strings.Split(*brokers, ","), *group, cfg)
	if err != nil {
		log.Fatalf("consumer group init failed: %v", err)
	}
	defer func() { _ = cg.Close() }()

	h := &Handler{pg: pg}

	log.Printf("[writer] start: topic=%s group=%s brokers=%s", *topic, *group, *brokers)

	for ctx.Err() == nil {
		if err := cg.Consume(ctx, []string{*topic}, h); err != nil {
			log.Printf("[writer] consume err: %v", err)
			time.Sleep(300 * time.Millisecond)
		}
	}
	log.Printf("[writer] exit: %v", ctx.Err())
}
