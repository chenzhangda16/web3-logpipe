package dispatcher

import (
	"time"

	"github.com/chenzhangda16/web3-logpipe/internal/logpipe/event"
)

const MaxBlocksPerWindow = 172800
const MaxTxPerBlock = 100
const MaxTxPerWindow = MaxTxPerBlock * MaxBlocksPerWindow

type TxWinMarginInfo struct {
	TxHead  int64
	TxTail  int64
	OpenWin bool
}

// winMoveBench：dispatcher 对 bench 的最小依赖（接口化，避免引入循环依赖/污染基础设施）
type winMoveBench interface {
	AddWinMove(n int)
	AddWinMoveBlockWin(winIdx int, d time.Duration)
}

type Dispatcher struct {
	log           *[MaxTxPerWindow]event.TxEvent
	winMoveRecord []chan TxWinMarginInfo

	bench winMoveBench
}

type WinChSnapshot struct {
	Len [4]int
	Cap [4]int
}

func (d *Dispatcher) WinChSnapshot() WinChSnapshot {
	var s WinChSnapshot
	for i := range d.winMoveRecord {
		s.Len[i] = len(d.winMoveRecord[i])
		s.Cap[i] = cap(d.winMoveRecord[i])
	}
	return s
}

func NewDispatcher(initialCap int, b winMoveBench) *Dispatcher {
	if initialCap <= 0 {
		initialCap = 8192
	}
	disp := &Dispatcher{
		log:           &[MaxTxPerWindow]event.TxEvent{},
		winMoveRecord: make([]chan TxWinMarginInfo, 4),
		bench:         b,
	}
	for i := 0; i < 4; i++ {
		disp.winMoveRecord[i] = make(chan TxWinMarginInfo, initialCap)
	}
	return disp
}

// 可选：如果你想 NewDispatcher 不传 bench，也可以后续注入
func (d *Dispatcher) SetBench(b winMoveBench) {
	d.bench = b
}

func (d *Dispatcher) Append(ev event.TxEvent, idx int64) {
	d.log[idx%MaxTxPerWindow] = ev
}

// WinMove：广播窗口边界变化；
// - sent 语义=“实际投递条数”（跳过 tail=-1 的窗口）
// - block 语义=“本次 WinMove 在发送阶段花费的总 wall time”（近似背压阻塞）
func (d *Dispatcher) WinMove(txTail []int64, txHead int64, openWin bool) {
	sent := 0
	hasBench := d.bench != nil

	for i := range d.winMoveRecord {
		if txTail[i] == -1 {
			continue
		}

		var t0 time.Time
		if hasBench {
			t0 = time.Now()
		}

		d.winMoveRecord[i] <- TxWinMarginInfo{
			TxHead:  txHead,
			TxTail:  txTail[i],
			OpenWin: openWin,
		}

		if hasBench {
			d.bench.AddWinMoveBlockWin(i, time.Since(t0))
		}
		sent++
	}

	if hasBench && sent > 0 {
		d.bench.AddWinMove(sent)
	}
}

// 订阅窗口 move（只读 chan，避免外部 close/写入）
func (d *Dispatcher) WinMoveCh(winIdx int) <-chan TxWinMarginInfo {
	return d.winMoveRecord[winIdx]
}

// 按 idx 读 event（环形）
func (d *Dispatcher) Get(idx int64) event.TxEvent {
	return d.log[idx%MaxTxPerWindow]
}
