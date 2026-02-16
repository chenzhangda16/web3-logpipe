package ingest

import (
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/chenzhangda16/web3-logpipe/internal/logpipe/event"
	"github.com/chenzhangda16/web3-logpipe/internal/logpipe/ids"
	mc "github.com/chenzhangda16/web3-logpipe/internal/mockchain/model"
)

type MockChainAdapter struct {
	Addrs  *ids.AddressID
	Tokens *ids.TokenID

	// 可选：统计丢弃量
	DropBadAddr bool
	DropNoToken bool
}

var emitInFlight int64
var emitMaxInFlight int64
var emitLastLog int64 // unix nano

func emitEnter() {
	cur := atomic.AddInt64(&emitInFlight, 1)

	// 更新 max
	for {
		maxn := atomic.LoadInt64(&emitMaxInFlight)
		if cur <= maxn {
			break
		}
		if atomic.CompareAndSwapInt64(&emitMaxInFlight, maxn, cur) {
			break
		}
	}
}

func emitExit() {
	atomic.AddInt64(&emitInFlight, -1)

	// 每 1s 打一次（避免刷屏）
	now := time.Now().UnixNano()
	last := atomic.LoadInt64(&emitLastLog)
	if now-last > int64(time.Second) && atomic.CompareAndSwapInt64(&emitLastLog, last, now) {
		cur := atomic.LoadInt64(&emitInFlight)
		maxn := atomic.LoadInt64(&emitMaxInFlight)
		log.Printf("[emit] inflight=%d max=%d", cur, maxn)
	}
}

func NewMockChainAdapter(addrs *ids.AddressID, tokens *ids.TokenID) *MockChainAdapter {
	return &MockChainAdapter{
		Addrs:       addrs,
		Tokens:      tokens,
		DropBadAddr: true,
		DropNoToken: false, // token 为空可以映射成 0（看你后续策略）
	}
}

func (a *MockChainAdapter) EmitTxEventsFromBlock(
	blk mc.Block,
	txRelativeIdx int64,
	emit func(ev event.TxEvent, idx int64),
	parts int, // <=1: linear; >1: chunk-parallel
) {
	//emitEnter()
	//defer emitExit()
	blockTs := blk.Header.Timestamp
	txs := blk.Txs
	n := len(txs)

	// small blocks: no point parallelizing
	if parts <= 1 || n < parts*2 {
		a.emitRange(blockTs, txs, txRelativeIdx, 0, n, emit)
		return
	}

	// chunk-parallel
	chunk := (n + parts - 1) / parts

	var wg sync.WaitGroup
	wg.Add(parts)

	for p := 0; p < parts; p++ {
		start := p * chunk
		end := start + chunk
		if start >= n {
			wg.Done()
			continue
		}
		if end > n {
			end = n
		}

		go func(start, end int) {
			defer wg.Done()
			a.emitRange(blockTs, txs, txRelativeIdx, start, end, emit)
		}(start, end)
	}

	wg.Wait()
}

func (a *MockChainAdapter) emitRange(
	blockTs int64,
	txs []mc.Tx,
	base int64,
	start, end int,
	emit func(ev event.TxEvent, idx int64),
) {
	for i := start; i < end; i++ {
		ev, ok := a.txToEvent(blockTs, txs[i])
		if !ok {
			continue
		}
		emit(ev, base+int64(i))
	}
}

func (a *MockChainAdapter) txToEvent(blockTs int64, tx mc.Tx) (event.TxEvent, bool) {
	body := tx.TxBody

	fromID, ok := a.Addrs.ID(body.From)
	if !ok {
		if a.DropBadAddr {
			return event.TxEvent{}, false
		}
		fromID = 0
	}

	toID, ok := a.Addrs.ID(body.To)
	if !ok {
		if a.DropBadAddr {
			return event.TxEvent{}, false
		}
		toID = 0
	}

	tokenID := a.Tokens.ID(body.Token)
	if tokenID == 0 && a.DropNoToken {
		return event.TxEvent{}, false
	}

	return event.TxEvent{
		Ts:     blockTs,
		From:   fromID,
		To:     toID,
		Token:  tokenID,
		Amount: body.Amount,
	}, true
}
