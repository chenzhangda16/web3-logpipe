package ingest

import (
	"encoding/json"
	"log"
	"sync"
	"time"

	"github.com/IBM/sarama"

	"github.com/chenzhangda16/web3-logpipe/internal/logpipe/dispatcher"
	"github.com/chenzhangda16/web3-logpipe/internal/logpipe/event"
	"github.com/chenzhangda16/web3-logpipe/internal/logpipe/ready"
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
	readyFifo          string
	readyOnce, testLog sync.Once

	disp  *dispatcher.Dispatcher
	spool Spool

	rawCh chan RawMsg
	wg    sync.WaitGroup

	maxWindowBlocks uint32
	blockTail       []uint32
	winTs           []int64

	validCount   uint32
	rbInCh       [MaxGroutines]chan struct{}
	rbOutCh      [MaxGroutines]chan struct{}
	curTxTailBuf [MaxGroutines][]int64
	rbBlockInfo  *[dispatcher.MaxBlocksPerWindow]BlockWinMarginInfo
	rbTxSum      int64

	// --- offsets / cold-start observability ---
	offMu             sync.RWMutex
	firstOffsetByPart map[int32]int64
	firstSeenByPart   map[int32]bool

	setupAt time.Time // set in Setup()

	client sarama.Client
	topic  string

	adapter *MockChainAdapter
}

func NewIngestor(
	fifoPath string,
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
		readyFifo: fifoPath,
		disp:      disp,
		spool:     spool,
		rawCh:     make(chan RawMsg, chSize),
		adapter:   adapter,
		client:    client,
		topic:     topic,

		firstOffsetByPart: make(map[int32]int64),
		firstSeenByPart:   make(map[int32]bool),

		// 需求 1：blockTail 赋值为 4 个 0
		blockTail: make([]uint32, 4),

		// 需求 2：winTs 赋值为 60、300、3600、86400
		winTs: []int64{60, 300, 3600, 86400},
	}

	// 需求 3：rbInCh / rbOutCh
	for i := 0; i < MaxGroutines; i++ {
		ig.rbInCh[i] = make(chan struct{}, 1)
		ig.rbOutCh[i] = make(chan struct{}, 1)
		ig.curTxTailBuf[i] = make([]int64, len(ig.blockTail))
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

func (ig *Ingestor) getFirstOffset(part int32) (int64, bool) {
	ig.offMu.RLock()
	defer ig.offMu.RUnlock()
	off, ok := ig.firstOffsetByPart[part]
	return off, ok
}

func (ig *Ingestor) markFirstSeen(part int32, off int64, base int64) {
	ig.offMu.Lock()
	defer ig.offMu.Unlock()
	if ig.firstSeenByPart[part] {
		return
	}
	ig.firstSeenByPart[part] = true

	cost := time.Since(ig.setupAt)
	log.Printf("[ingest] first_msg: topic=%s p=%d off=%d base=%d re=%d rawCh=%d since_setup=%s",
		ig.topic, part, off, base, off-base, len(ig.rawCh), cost)
}

func (ig *Ingestor) decodeLoop() {
	for rawMsg := range ig.rawCh {
		// 0) base offset (per partition)
		base, ok := ig.getFirstOffset(rawMsg.Partition)
		if !ok {
			// fallback: should not happen if Setup ran and ResetOffset succeeded.
			// keep non-fatal: use current msg offset as base to avoid negative reOffset.
			base = rawMsg.Offset
			ig.offMu.Lock()
			ig.firstOffsetByPart[rawMsg.Partition] = base
			ig.offMu.Unlock()
			log.Printf("[ingest][warn] missing base offset => init from first seen: topic=%s p=%d base=%d",
				ig.topic, rawMsg.Partition, base)
		}

		ig.markFirstSeen(rawMsg.Partition, rawMsg.Offset, base)

		var blk mc.Block
		if err := json.Unmarshal(rawMsg.Value, &blk); err != nil {
			log.Printf("[ingest] decode block failed: p=%d off=%d err=%v", rawMsg.Partition, rawMsg.Offset, err)
			continue
		}
		ig.testLog.Do(func() {
			log.Printf("[ingest] blk head first %d.", blk.Header.Number)
		})
		reOffset := rawMsg.Offset - base
		if reOffset < 0 {
			// should not happen; indicates base is wrong or offset rewind
			log.Printf("[ingest][warn] reOffset<0: topic=%s p=%d off=%d base=%d re=%d",
				ig.topic, rawMsg.Partition, rawMsg.Offset, base, reOffset)
			continue
		}

		reIdx := reOffset % dispatcher.MaxBlocksPerWindow

		<-ig.rbInCh[reOffset%MaxGroutines]

		curRbTxSum := ig.rbTxSum
		ig.rbTxSum += int64(len(blk.Txs))

		ig.rbBlockInfo[reIdx] = BlockWinMarginInfo{
			blockTs:     blk.Header.Timestamp,
			relativeIdx: curRbTxSum,
		}

		openWin := false
		for idx, tail := range ig.blockTail {
			for blk.Header.Timestamp-ig.rbBlockInfo[tail%dispatcher.MaxBlocksPerWindow].blockTs > ig.winTs[idx] {
				tail++
			}
			ig.blockTail[idx] = tail
			if idx == len(ig.blockTail)-1 {
				openWin = tail != 0
			}
		}

		lane := reOffset % MaxGroutines

		curTxTail := ig.curTxTailBuf[lane]
		curTxHead := ig.rbTxSum
		for idx, tail := range ig.blockTail {
			curTxTail[idx] = ig.rbBlockInfo[tail%dispatcher.MaxBlocksPerWindow].relativeIdx
		}

		ig.rbInCh[(reOffset+1)%MaxGroutines] <- struct{}{}

		parts := 1
		//if len(ig.rawCh) == 0 {
		//	parts = 16 // 写死最大并发
		//}

		ig.adapter.EmitTxEventsFromBlock(blk, curRbTxSum, func(ev event.TxEvent, idx int64) {
			ig.disp.Append(ev, idx)
		}, parts)

		<-ig.rbOutCh[reOffset%MaxGroutines]

		ig.disp.WinMove(curTxTail, curTxHead, openWin)

		if reOffset%100 == 0 {
			log.Printf("[ingest] p=%d off=%d base=%d re=%d blk=%d tx=%d rawCh=%d",
				rawMsg.Partition, rawMsg.Offset, base, reOffset, blk.Header.Number, len(blk.Txs), len(ig.rawCh))
		}

		ig.rbOutCh[(reOffset+1)%MaxGroutines] <- struct{}{}
	}
}

// 你如果需要 ctx cancel，可把 rawCh 改成带 ctx 的 select（略）
var _ sarama.ConsumerGroupHandler = (*Ingestor)(nil)

func (ig *Ingestor) Setup(sess sarama.ConsumerGroupSession) error {
	ig.setupAt = time.Now()

	// 现在往回 24 小时
	targetMs := time.Now().Add(-24 * time.Hour).UnixMilli()

	claims := sess.Claims() // map[topic][]partition
	parts := claims[ig.topic]

	log.Printf("[ingest][setup] topic=%s parts=%v targetMs=%d", ig.topic, parts, targetMs)
	if len(parts) > 1 {
		// 重要：你当前 rbTxSum/rbBlockInfo/reOffset 的拓扑，隐含“单顺序流”假设。
		log.Printf("[ingest][warn] topic has multiple partitions=%d; current window/ring logic assumes a single ordered stream",
			len(parts))
	}

	for _, p := range parts {
		t0 := time.Now()
		off, err := ig.client.GetOffset(ig.topic, p, targetMs)
		cost := time.Since(t0)
		if err != nil {
			log.Printf("[ingest][setup] GetOffset failed: topic=%s p=%d targetMs=%d err=%v cost=%s",
				ig.topic, p, targetMs, err, cost)
			continue
		}

		log.Printf("[ingest][setup] reset offset: topic=%s p=%d off=%d (t=%dms) cost=%s",
			ig.topic, p, off, targetMs, cost)
		ig.offMu.Lock()
		ig.firstOffsetByPart[p] = max(off, 0)
		ig.firstSeenByPart[p] = false
		ig.offMu.Unlock()

		// 注意：如果 retention 不够，off 会退化成当前最早可用的 offset
		sess.ResetOffset(ig.topic, p, off, "")

	}
	ig.readyOnce.Do(func() {
		log.Printf("[ready] processor session established, signaling fifo=%s", ig.readyFifo)
		go ready.SignalFifoCtx(sess.Context(), ig.readyFifo, "READY\n", 8*time.Second)
	})

	return nil
}

func (ig *Ingestor) Cleanup(sarama.ConsumerGroupSession) error { return nil }
