package processor

import (
	"github.com/IBM/sarama"
)

type Consumer struct {
	group  sarama.ConsumerGroup
	client sarama.Client
}

func NewConsumerWithClient(
	client sarama.Client,
	groupID string,
) (*Consumer, error) {
	group, err := sarama.NewConsumerGroupFromClient(groupID, client)
	if err != nil {
		return nil, err
	}
	return &Consumer{
		group:  group,
		client: client,
	}, nil
}

func (c *Consumer) Close() error { return c.group.Close() }
