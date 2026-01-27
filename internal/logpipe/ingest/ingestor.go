package ingest

import (
	"encoding/json"
	"log"
	"sync"
	"time"

	"github.com/IBM/sarama"

	"github.com/chenzhangda16/web3-logpipe/internal/logpipe/dispatcher"
	"github.com/chenzhangda16/web3-logpipe/internal/logpipe/event"
	mc "github.com/chenzhangda16/web3-logpipe/internal/mockchain/model"
)

const MaxGroutines = 20

type RawMsg struct {
	Partition int32
	Offset    int64
	Value     []byte
}

type BlockWinMarginInfo struct {
	blockTs     int64
	relativeIdx int64
}

type Ingestor struct {
	disp  *dispatcher.Dispatcher
	spool Spool

	rawCh chan RawMsg
	wg    sync.WaitGroup

	maxWindowBlocks uint32
	blockTail       []uint32
	winTs           []int64

	validCount  uint32
	rbInCh      [MaxGroutines]chan struct{}
	rbOutCh     [MaxGroutines]chan struct{}
	rbBlockInfo *[dispatcher.MaxBlocksPerWindow]BlockWinMarginInfo
	rbTxSum     int64
	firstOffset int64
	client      sarama.Client
	topic       string

	adapter *MockChainAdapter
}

func NewIngestor(
	disp *dispatcher.Dispatcher,
	spool Spool,
	workerN int,
	chSize int,
	adapter *MockChainAdapter,
	client sarama.Client,
	topic string,
) *Ingestor {
	if workerN <= 0 {
		workerN = MaxGroutines
	}
	if chSize <= 0 {
		chSize = 1024
	}

	ig := &Ingestor{
		disp:    disp,
		spool:   spool,
		rawCh:   make(chan RawMsg, chSize),
		adapter: adapter,
		client:  client,
		topic:   topic,

		// 需求 1：blockTail 赋值为 4 个 0
		// 注意：make([]uint32, 4) 默认就是 [0,0,0,0]
		blockTail: make([]uint32, 4),

		// 需求 2：winTs 赋值为 60、300、3600、86400
		winTs: []int64{60, 300, 3600, 86400},
	}

	// 需求 3：rbInCh 赋值为容量为 1 的 chan bool（MaxGroutines 个）
	for i := 0; i < MaxGroutines; i++ {
		ig.rbInCh[i] = make(chan struct{}, 1)
		ig.rbOutCh[i] = make(chan struct{}, 1)
	}
	ig.rbInCh[0] <- struct{}{}
	ig.rbOutCh[0] <- struct{}{}
	ig.rbBlockInfo = new([dispatcher.MaxBlocksPerWindow]BlockWinMarginInfo)

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
		if err := ig.spool.Append(msg.Partition, msg.Offset, msg.Value); err != nil {
			log.Printf("[ingest] spool append failed: p=%d off=%d err=%v", msg.Partition, msg.Offset, err)
			continue
		}

		sess.MarkMessage(msg, "")

		// 推荐：拷贝 Value，避免生命周期/复用风险
		val := make([]byte, len(msg.Value))
		copy(val, msg.Value)

		rm := RawMsg{
			Partition: msg.Partition,
			Offset:    msg.Offset,
			Value:     val,
		}

		// 背压：直接阻塞写最清晰
		ig.rawCh <- rm
	}
	return nil
}

func (ig *Ingestor) decodeLoop() {
	for rawMsg := range ig.rawCh {
		var blk mc.Block
		if err := json.Unmarshal(rawMsg.Value, &blk); err != nil {
			log.Printf("[ingest] decode block failed: err=%v", err)
			continue
		}
		reOffset := rawMsg.Offset - ig.firstOffset
		reIdx := reOffset % dispatcher.MaxBlocksPerWindow

		<-ig.rbInCh[reOffset%MaxGroutines]

		curRbTxSum := ig.rbTxSum
		ig.rbTxSum += int64(len(blk.Txs))
		openWin := false
		for idx, tail := range ig.blockTail {
			for blk.Header.Timestamp-ig.rbBlockInfo[tail%dispatcher.MaxBlocksPerWindow].blockTs > ig.winTs[idx] {
				tail++
			}
			ig.blockTail[idx] = tail
			if idx == len(ig.blockTail)-1 {
				openWin = tail != 0
				//	条件是可以修改的，比如ig.rawCh内空了可能更能证明窗口填好了
			}
		}
		curTxTail := make([]int64, len(ig.blockTail))
		curTxHead := ig.rbTxSum
		for idx, tail := range ig.blockTail {
			curTxTail[idx] = ig.rbBlockInfo[tail%dispatcher.MaxBlocksPerWindow].relativeIdx
		}
		ig.rbBlockInfo[reIdx] = BlockWinMarginInfo{
			blockTs:     blk.Header.Timestamp,
			relativeIdx: curRbTxSum,
		}

		ig.rbInCh[(reOffset+1)%MaxGroutines] <- struct{}{}

		parts := 1
		if len(ig.rawCh) == 0 {
			parts = 16 // 写死最大并发
		}

		ig.adapter.EmitTxEventsFromBlock(blk, curRbTxSum, func(ev event.TxEvent, idx int64) {
			ig.disp.Append(ev, idx)
		}, parts)

		<-ig.rbOutCh[reOffset%MaxGroutines]

		ig.disp.WinMove(curTxTail, curTxHead, openWin)

		ig.rbOutCh[(reOffset+1)%MaxGroutines] <- struct{}{}
	}
}

// 你如果需要 ctx cancel，可把 rawCh 改成带 ctx 的 select（略）
var _ sarama.ConsumerGroupHandler = (*Ingestor)(nil)

func (ig *Ingestor) Setup(sess sarama.ConsumerGroupSession) error {
	// 现在往回 24 小时
	targetMs := time.Now().Add(-24 * time.Hour).UnixMilli()

	claims := sess.Claims() // map[topic][]partition
	parts := claims[ig.topic]

	for _, p := range parts {
		off, err := ig.client.GetOffset(ig.topic, p, targetMs)
		if err != nil {
			log.Printf("[ingest] GetOffset failed: topic=%s p=%d err=%v", ig.topic, p, err)
			continue
		}
		ig.firstOffset = off
		// 注意：如果 retention 不够，off 会退化成当前最早可用的 offset
		sess.ResetOffset(ig.topic, p, off, "")
		log.Printf("[ingest] reset offset: topic=%s p=%d off=%d (t=%dms)", ig.topic, p, off, targetMs)
	}
	return nil
}
func (ig *Ingestor) Cleanup(sarama.ConsumerGroupSession) error { return nil }
