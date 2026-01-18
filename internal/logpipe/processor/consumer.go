package processor

import (
	"strings"

	"github.com/IBM/sarama"
)

type Consumer struct {
	group sarama.ConsumerGroup
	topic string
}

func NewConsumer(brokersCSV, groupID, topic string) (*Consumer, error) {
	brokers := splitCSV(brokersCSV)

	cfg := sarama.NewConfig()
	cfg.Version = sarama.V2_1_0_0 // adjust if needed
	cfg.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRange
	cfg.Consumer.Offsets.Initial = sarama.OffsetOldest
	cfg.Consumer.Return.Errors = true

	cg, err := sarama.NewConsumerGroup(brokers, groupID, cfg)
	if err != nil {
		return nil, err
	}
	return &Consumer{group: cg, topic: topic}, nil
}

func (c *Consumer) Close() error { return c.group.Close() }

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
