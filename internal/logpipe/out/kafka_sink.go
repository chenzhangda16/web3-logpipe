package out

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/IBM/sarama"
)

type KafkaSink struct {
	topic string
	p     sarama.SyncProducer
}

func NewKafkaSink(brokers []string, topic string, cfg *sarama.Config) (*KafkaSink, error) {
	if cfg == nil {
		cfg = sarama.NewConfig()
	}
	// SyncProducer 需要 Return.Successes=true
	cfg.Producer.Return.Successes = true
	cfg.Producer.Return.Errors = true

	p, err := sarama.NewSyncProducer(brokers, cfg)
	if err != nil {
		return nil, err
	}
	return &KafkaSink{topic: topic, p: p}, nil
}

func (s *KafkaSink) Close() error {
	if s.p != nil {
		return s.p.Close()
	}
	return nil
}

func (s *KafkaSink) Emit(ctx context.Context, typ string, v any) error {
	// ctx 先不用强绑定（sarama 的 SyncProducer 不吃 ctx），但保留签名方便你未来统一 cancel
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	env := Envelope{
		Type: typ,
		TS:   time.Now().UnixMilli(),
		Data: data,
	}
	b, err := json.Marshal(env)
	if err != nil {
		return err
	}

	msg := &sarama.ProducerMessage{
		Topic: s.topic,
		Value: sarama.ByteEncoder(b),
	}
	_, _, err = s.p.SendMessage(msg)
	if err != nil {
		return fmt.Errorf("kafka emit failed: %w", err)
	}
	return nil
}
