package main

import (
	"context"
	"log"

	"github.com/IBM/sarama"
)

type Handler struct{}

func (Handler) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (Handler) Cleanup(sarama.ConsumerGroupSession) error { return nil }
func (Handler) ConsumeClaim(
	s sarama.ConsumerGroupSession,
	c sarama.ConsumerGroupClaim,
) error {
	for msg := range c.Messages() {
		log.Printf(
			"key=%s value=%s partition=%d offset=%d",
			string(msg.Key),
			string(msg.Value),
			msg.Partition,
			msg.Offset,
		)
		s.MarkMessage(msg, "")
	}
	return nil
}

func main() {
	cfg := sarama.NewConfig()
	cfg.Version = sarama.V2_8_0_0
	cfg.Consumer.Offsets.Initial = sarama.OffsetOldest

	group, err := sarama.NewConsumerGroup(
		[]string{"localhost:9092"},
		"logpipe-test_tools",
		cfg,
	)
	if err != nil {
		log.Fatal(err)
	}
	defer group.Close()

	for {
		err = group.Consume(
			context.Background(),
			[]string{"raw.events"},
			Handler{},
		)
		if err != nil {
			log.Fatal(err)
		}
	}
}
