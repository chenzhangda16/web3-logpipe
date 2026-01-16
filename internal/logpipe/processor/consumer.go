package processor

import (
	"context"
	"encoding/json"
	"log"
	"strings"
	"time"

	"github.com/IBM/sarama"
	"github.com/chenzhangda16/web3-logpipe/internal/mockchain/model"
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

type Handler struct {
	onBlock func(ctx context.Context, part int32, offset int64, b model.Block) error
}

func (h *Handler) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (h *Handler) Cleanup(sarama.ConsumerGroupSession) error { return nil }

func (h *Handler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		var blk model.Block
		if err := json.Unmarshal(msg.Value, &blk); err != nil {
			log.Printf("[processor] decode block failed: partition=%d offset=%d err=%v", msg.Partition, msg.Offset, err)
			// mark to skip bad message (for mock)
			sess.MarkMessage(msg, "bad-json")
			continue
		}

		if err := h.onBlock(sess.Context(), msg.Partition, msg.Offset, blk); err != nil {
			// do not mark, let it retry
			time.Sleep(200 * time.Millisecond)
			continue
		}

		sess.MarkMessage(msg, "")
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
