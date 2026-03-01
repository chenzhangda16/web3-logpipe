package ingest

import (
	"context"
	"encoding/json"
	"log"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"github.com/chenzhangda16/web3-logpipe/internal/logpipe/bench"

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

	maxWindowBlocks      int
	blockTail            []int
	winTs                []int64
	delayWinMoveBlockTs  []int64
	delayWinMoveBlockIdx []int64
	openWin              bool

	validCount   int
	rbInCh       [MaxGroutines]chan struct{}
	rbOutCh      [MaxGroutines]chan struct{}
	curTxTailBuf [MaxGroutines][]int64
	rbBlockInfo  *[dispatcher.MaxBlocksPerWindow]BlockWinMarginInfo
	rbTxSum      int64

	// --- offsets / cold-start observability ---
	setupAt int64 // set in Setup()

	client  sarama.Client
	topic   string
	sessCtx context.Context

	adapter      *MockChainAdapter
	procBench    *bench.ProcBench
	readyBenchCh chan struct{}
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
	procBench *bench.ProcBench,
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

		blockTail: make([]int, 4),

		winTs:                []int64{60, 300, 3600, 86400},
		delayWinMoveBlockTs:  make([]int64, 4),
		delayWinMoveBlockIdx: make([]int64, 4),
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
	ig.procBench = procBench
	ig.readyBenchCh = make(chan struct{})

	ig.wg.Add(workerN)
	for i := 0; i < workerN; i++ {
		go func() {
			defer ig.wg.Done()
			ig.decodeLoop()
		}()
	}
	return ig
}

func (ig *Ingestor) RawChSnapshot() (ln, cp int) { return len(ig.rawCh), cap(ig.rawCh) }

func (ig *Ingestor) ReadyBenchCh() <-chan struct{} {
	return ig.readyBenchCh
}

func (ig *Ingestor) Close() error {
	close(ig.rawCh)
	ig.wg.Wait()
	return ig.spool.Close()
}

// ConsumeClaim：只做 barrier + commit + 投递，不做任何窗口/图计算
func (ig *Ingestor) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		if ig.procBench != nil {
			ig.procBench.AddConsumeMsg(1)
		}
		t0 := time.Now()
		err := ig.spool.Append(msg.Partition, msg.Offset, msg.Value)
		if ig.procBench != nil {
			ig.procBench.ObserveSpool(time.Since(t0), err)
		}
		if err != nil {
			log.Printf("[ingest] spool append failed: p=%d off=%d err=%v", msg.Partition, msg.Offset, err)
			continue
		}

		sess.MarkMessage(msg, "")

		val := getMsgBuf(len(msg.Value))
		copy(val, msg.Value)
		tSend := time.Now()
		ig.rawCh <- RawMsg{
			Partition: msg.Partition,
			Offset:    msg.Offset,
			Value:     val,
		}
		if ig.procBench != nil {
			d := time.Since(tSend)
			if d > 0 { // 只要你想都记，就直接 Add；如果你只想记录“明显阻塞”，可以加阈值比如 >200µs
				ig.procBench.AddRawSendBlocked(d)
			}
		}
	}
	return nil
}

func recvPermit(ctx context.Context, ch <-chan struct{}) bool {
	select {
	case <-ctx.Done():
		return false
	case <-ch:
		return true
	}
}

func sendPermit(ctx context.Context, ch chan<- struct{}) bool {
	select {
	case <-ctx.Done():
		return false
	case ch <- struct{}{}:
		return true
	}
}

func (ig *Ingestor) decodeLoop() {
	for rawMsg := range ig.rawCh {
		var blk mc.Block
		tDec := time.Now()
		err := json.Unmarshal(rawMsg.Value, &blk)
		if ig.procBench != nil {
			ig.procBench.ObserveDecode(time.Since(tDec), err)
		}
		if err != nil {
			log.Printf("[ingest] decode block failed: p=%d off=%d err=%v", rawMsg.Partition, rawMsg.Offset, err)
			putMsgBuf(rawMsg.Value)
			continue
		}
		putMsgBuf(rawMsg.Value)
		ig.testLog.Do(func() {
			log.Printf("[ingest] blk head first %d.", blk.Header.Number)
		})
		reOffset := rawMsg.Offset

		reIdx := reOffset % dispatcher.MaxBlocksPerWindow

		if !recvPermit(ig.sessCtx, ig.rbInCh[reOffset%MaxGroutines]) {
			return
		}

		curRbTxSum := ig.rbTxSum
		ig.rbTxSum += int64(len(blk.Txs))

		ig.rbBlockInfo[reIdx] = BlockWinMarginInfo{
			blockTs:     blk.Header.Timestamp,
			relativeIdx: curRbTxSum,
		}

		for idx, tail := range ig.blockTail {
			for blk.Header.Timestamp-ig.rbBlockInfo[tail%dispatcher.MaxBlocksPerWindow].blockTs > ig.winTs[idx] {
				tail++
			}
			ig.blockTail[idx] = tail
		}
		maxIdx := len(ig.blockTail) - 1
		if !ig.openWin && ig.blockTail[maxIdx] != 0 {
			ig.openWin = true
			if ig.procBench != nil {
				ig.procBench.MarkSteady()
			}
			log.Printf("[procbench] switch steady: re_off=%d blk=%d ts=%d", reOffset, blk.Header.Number, blk.Header.Timestamp)
		}

		if ig.procBench != nil {
			ig.procBench.SetLastProgress(reOffset, blk.Header.Number)
		}

		lane := reOffset % MaxGroutines

		curTxTail := ig.curTxTailBuf[lane]
		curTxHead := ig.rbTxSum
		for idx, tail := range ig.blockTail {
			startIdx := &ig.delayWinMoveBlockIdx[idx]
			if *startIdx == -1 {
				if blk.Header.Timestamp >= ig.delayWinMoveBlockTs[idx] {
					*startIdx = reOffset
				} else {
					curTxTail[idx] = -1
					continue
				}
			}

			curTxTail[idx] = max(
				ig.rbBlockInfo[tail%dispatcher.MaxBlocksPerWindow].relativeIdx,
				ig.rbBlockInfo[*startIdx%dispatcher.MaxBlocksPerWindow].relativeIdx,
			)
		}

		if !sendPermit(ig.sessCtx, ig.rbInCh[(reOffset+1)%MaxGroutines]) {
			return
		}

		parts := 1
		//if len(ig.rawCh) == 0 {
		//	parts = 16 // 写死最大并发
		//}

		ig.adapter.EmitTxEventsFromBlock(blk, curRbTxSum, func(ev event.TxEvent, idx int64) {
			ig.disp.Append(ev, idx)
		}, parts)

		if !recvPermit(ig.sessCtx, ig.rbOutCh[reOffset%MaxGroutines]) {
			return
		}

		ig.disp.WinMove(curTxTail, curTxHead, ig.openWin)

		if !sendPermit(ig.sessCtx, ig.rbOutCh[(reOffset+1)%MaxGroutines]) {
			return
		}
	}
}

// 你如果需要 ctx cancel，可把 rawCh 改成带 ctx 的 select（略）
var _ sarama.ConsumerGroupHandler = (*Ingestor)(nil)

func (ig *Ingestor) Setup(sess sarama.ConsumerGroupSession) error {
	ig.setupAt = time.Now().Unix()
	ig.sessCtx = sess.Context()
	for i := range ig.winTs {
		ig.delayWinMoveBlockTs[i] = ig.setupAt - ig.winTs[i]
		ig.delayWinMoveBlockIdx[i] = -1
	}

	// 现在往回 24 小时
	targetMs := time.Now().Add(-24 * time.Hour).UnixMilli()

	claims := sess.Claims() // map[topic][]partition
	parts := claims[ig.topic]

	log.Printf("[ingest][setup] topic=%s parts=%v targetMs=%d", ig.topic, parts, targetMs)
	if len(parts) > 1 {
		// 重要：当前 rbTxSum/rbBlockInfo/reOffset 的拓扑，隐含“单顺序流”假设。
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

		// 注意：如果 retention 不够，off 会退化成当前最早可用的 offset
		sess.ResetOffset(ig.topic, p, off, "")

	}
	ig.readyOnce.Do(func() {
		log.Printf("[ready] processor session established, signaling fifo=%s", ig.readyFifo)
		go ready.SignalFifoCtx(sess.Context(), ig.readyFifo, "READY\n", 8*time.Second)
		if ig.procBench != nil {
			ig.procBench.Start()
		}
		close(ig.readyBenchCh)
	})
	return nil
}

func (ig *Ingestor) Cleanup(sarama.ConsumerGroupSession) error { return nil }
