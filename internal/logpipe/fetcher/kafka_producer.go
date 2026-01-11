package fetcher

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/IBM/sarama"
	"github.com/chenzhangda16/web3-logpipe/internal/mockchain/model"
)

type Producer struct {
	topic string
	sp    sarama.SyncProducer
}

func NewProducer(brokersCSV string, topic string) (*Producer, error) {
	if topic == "" {
		return nil, errors.New("topic empty")
	}
	brokers := splitCSV(brokersCSV)
	if len(brokers) == 0 {
		return nil, errors.New("no brokers")
	}

	cfg := sarama.NewConfig()

	// Reliability-oriented defaults
	cfg.Producer.RequiredAcks = sarama.WaitForAll
	cfg.Producer.Retry.Max = 10
	cfg.Producer.Retry.Backoff = 200 * time.Millisecond

	// SyncProducer must have Return.Successes=true
	cfg.Producer.Return.Successes = true
	cfg.Producer.Return.Errors = true

	// If your Kafka cluster supports idempotent producer, keep it on.
	// NOTE: idempotency may require additional constraints (max.in.flight, retries, acks).
	// Sarama will validate some of these.
	cfg.Producer.Idempotent = true

	// If you know your Kafka version, set it explicitly to avoid negotiation issues.
	cfg.Version = sarama.V2_1_0_0

	sp, err := sarama.NewSyncProducer(brokers, cfg)
	if err != nil {
		return nil, err
	}

	return &Producer{
		topic: topic,
		sp:    sp,
	}, nil
}

func (p *Producer) Close() error {
	if p.sp != nil {
		return p.sp.Close()
	}
	return nil
}

// ProduceBlock sends the block to Kafka and waits for broker ACK (sync).
// It is safe to checkpoint after this returns nil.
func (p *Producer) ProduceBlock(ctx context.Context, b model.Block) error {
	payload, err := json.Marshal(b)
	if err != nil {
		return err
	}

	msg := &sarama.ProducerMessage{
		Topic: p.topic,
		Key:   sarama.StringEncoder(strconv.FormatInt(b.Header.Number, 10)),
		Value: sarama.ByteEncoder(payload),
	}

	// sarama SyncProducer doesn't accept context directly; we can only check ctx before/after.
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	_, _, err = p.sp.SendMessage(msg)
	if err != nil {
		return err
	}

	select {
	case <-ctx.Done():
		// message is already acked, but caller wants to stop; return ctx error to exit quickly
		return ctx.Err()
	default:
	}
	return nil
}

func splitCSV(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, x := range parts {
		x = strings.TrimSpace(x)
		if x != "" {
			out = append(out, x)
		}
	}
	return out
}
