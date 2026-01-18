package ingest

import (
	"encoding/json"
	"log"
	"sync"

	"github.com/IBM/sarama"

	"github.com/chenzhangda16/web3-logpipe/internal/logpipe/dispatcher"
	"github.com/chenzhangda16/web3-logpipe/internal/logpipe/event"
	mc "github.com/chenzhangda16/web3-logpipe/internal/mockchain/model"
)

type Ingestor struct {
	disp  *dispatcher.Dispatcher
	spool Spool

	rawCh chan []byte
	wg    sync.WaitGroup

	adapter *MockChainAdapter
}

func NewIngestor(disp *dispatcher.Dispatcher, spool Spool, workerN int, chSize int, adapter *MockChainAdapter) *Ingestor {
	if workerN <= 0 {
		workerN = 1
	}
	if chSize <= 0 {
		chSize = 1024
	}
	ig := &Ingestor{
		disp:    disp,
		spool:   spool,
		rawCh:   make(chan []byte, chSize),
		adapter: adapter,
	}
	ig.wg.Add(workerN)
	for i := 0; i < workerN; i++ {
		go func() {
			defer ig.wg.Done()
			ig.decodeLoop()
		}()
	}
	return ig
}

func (ig *Ingestor) Close() error {
	close(ig.rawCh)
	ig.wg.Wait()
	return ig.spool.Close()
}

// ConsumeClaim：只做 barrier + commit + 投递，不做任何窗口/图计算
func (ig *Ingestor) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		// 1) enqueue barrier（可重放落地）
		if err := ig.spool.Append(msg.Partition, msg.Offset, msg.Value); err != nil {
			log.Printf("[ingest] spool append failed: p=%d off=%d err=%v", msg.Partition, msg.Offset, err)
			// 不 mark：让它重试（保证不丢）
			continue
		}

		// 2) commit offset：不等任何下游
		sess.MarkMessage(msg, "")

		// 3) 异步解码投递（这里允许短暂阻塞，但不会等窗口）
		select {
		case ig.rawCh <- msg.Value:
		default:
			// 背压策略：这里你有三种选项
			// A) 阻塞等待（最安全，不丢）
			// B) 丢弃（不推荐，会造成窗口空洞）
			// C) 记录 metrics + 阻塞一小会
			ig.rawCh <- msg.Value
		}
	}
	return nil
}

func (ig *Ingestor) decodeLoop() {
	for raw := range ig.rawCh {
		var blk mc.Block
		if err := json.Unmarshal(raw, &blk); err != nil {
			log.Printf("[ingest] decode block failed: err=%v", err)
			continue
		}

		ig.adapter.EmitTxEventsFromBlock(blk, func(ev event.TxEvent) uint64 {
			seq, _ := ig.disp.Append(ev) // Append 内部会写 ev.Seq
			return seq
		})
	}
}

// 你如果需要 ctx cancel，可把 rawCh 改成带 ctx 的 select（略）
var _ sarama.ConsumerGroupHandler = (*Ingestor)(nil)

func (ig *Ingestor) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (ig *Ingestor) Cleanup(sarama.ConsumerGroupSession) error { return nil }
