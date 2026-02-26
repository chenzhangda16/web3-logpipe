package fetcher

import (
	"context"
	"encoding/json"
	"strconv"
	"strings"
	"time"

	"github.com/IBM/sarama"
	"github.com/chenzhangda16/web3-logpipe/internal/mockchain/model"
	"github.com/chenzhangda16/web3-logpipe/pkg/hash"
)

// internal/logpipe/fetcher/kafka_producer.go  (single partition async)

type Producer struct {
	topic string
	ap    sarama.AsyncProducer
}

type ProduceMeta struct {
	PageSeq int64
	Height  int64
	Hash    hash.Hash32
}

func NewProducer(brokersCSV, topic string) (*Producer, error) {
	// ... splitCSV / validate 省略

	cfg := sarama.NewConfig()

	cfg.Producer.RequiredAcks = sarama.WaitForAll
	cfg.Producer.Retry.Max = 10
	cfg.Producer.Retry.Backoff = 200 * time.Millisecond

	cfg.Producer.Return.Successes = true
	cfg.Producer.Return.Errors = true

	cfg.Producer.Idempotent = true
	cfg.Net.MaxOpenRequests = 1

	// batching
	cfg.Producer.Flush.Messages = 500
	cfg.Producer.Flush.Frequency = 10 * time.Millisecond

	cfg.Producer.Compression = sarama.CompressionLZ4
	cfg.Version = sarama.V2_1_0_0

	ap, err := sarama.NewAsyncProducer(splitCSV(brokersCSV), cfg)
	if err != nil {
		return nil, err
	}

	return &Producer{topic: topic, ap: ap}, nil
}

func (p *Producer) Close() error {
	if p == nil || p.ap == nil {
		return nil
	}
	return p.ap.Close()
}

func (p *Producer) Successes() <-chan *sarama.ProducerMessage { return p.ap.Successes() }
func (p *Producer) Errors() <-chan *sarama.ProducerError      { return p.ap.Errors() }

// ProduceBlockSinglePartition: 固定打到 partition 0（全局顺序最简单）
func (p *Producer) ProduceBlockSinglePartition(
	ctx context.Context,
	b model.Block,
	pageSeq int64,
) error {
	payload, err := json.Marshal(b)
	if err != nil {
		return err
	}
	h := b.Header.Number
	ts := time.Unix(b.Header.Timestamp, 0)

	msg := &sarama.ProducerMessage{
		Topic:     p.topic,
		Partition: 0, // <- 单 partition
		Key:       sarama.StringEncoder(strconv.FormatInt(h, 10)),
		Value:     sarama.ByteEncoder(payload),
		Timestamp: ts,
		Metadata:  ProduceMeta{PageSeq: pageSeq, Height: h, Hash: b.Hash},
	}

	// ctx-aware enqueue（关键：send 放进 select）
	select {
	case <-ctx.Done():
		return ctx.Err()
	case p.ap.Input() <- msg:
		return nil
	}
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
