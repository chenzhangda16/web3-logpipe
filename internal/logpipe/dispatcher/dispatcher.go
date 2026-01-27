package dispatcher

import "github.com/chenzhangda16/web3-logpipe/internal/logpipe/event"

const MaxBlocksPerWindow = 172800
const MaxTxPerBlock = 100
const MaxTxPerWindow = MaxTxPerBlock * MaxBlocksPerWindow

type TxWinMarginInfo struct {
	TxHead  int64
	TxTail  int64
	OpenWin bool
}

type Dispatcher struct {
	log           *[MaxTxPerWindow]event.TxEvent
	winMoveRecord []chan TxWinMarginInfo
}

func NewDispatcher(initialCap int) *Dispatcher {
	if initialCap <= 0 {
		initialCap = 16
	}
	disp := &Dispatcher{
		log:           &[MaxTxPerWindow]event.TxEvent{},
		winMoveRecord: make([]chan TxWinMarginInfo, 4),
	}
	for i := 0; i < 4; i++ {
		disp.winMoveRecord[i] = make(chan TxWinMarginInfo, initialCap)
	}
	return disp
}

func (d *Dispatcher) Append(ev event.TxEvent, idx int64) {
	d.log[idx%MaxTxPerWindow] = ev
}

func (d *Dispatcher) WinMove(txTail []int64, txHead int64, openWin bool) {
	for i := range d.winMoveRecord {
		d.winMoveRecord[i] <- TxWinMarginInfo{
			TxHead:  txHead,
			TxTail:  txTail[i],
			OpenWin: openWin,
		}
	}
}

// 新增：订阅窗口 move
func (d *Dispatcher) WinMoveCh(winIdx int) <-chan TxWinMarginInfo {
	return d.winMoveRecord[winIdx]
}

// 新增：按 idx 读 event
func (d *Dispatcher) Get(idx int64) event.TxEvent {
	return d.log[idx%MaxTxPerWindow]
}
