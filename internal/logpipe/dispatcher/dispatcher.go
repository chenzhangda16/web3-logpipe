package dispatcher

import (
	"context"
	"sync"

	"github.com/chenzhangda16/web3-logpipe/internal/logpipe/event"
)

// processor.TxEvent 是 Dispatcher 的最小单元：按时间/高度单调追加。
// 你可以把 Block decode 成 processor.TxEvent（携带 blockNum、ts、payload等）再丢进来。

// Watermark 表示 Dispatcher 已经“看见并追加”的最新时间与序号。
// Window runner 推进 r 时通常以 wm 为上界。
type Watermark struct {
	Seq uint64
	Ts  int64
}

// Dispatcher：单入口 Append，多读者 ReadBySeq，watermark 广播。
type Dispatcher struct {
	mu sync.RWMutex

	// append-only event log（先内存，后续可替换成 spool+冷热分层）
	log []event.TxEvent

	// 下一条事件分配的 Seq
	nextSeq uint64

	// 当前 watermark（最后一条事件的时间/seq）
	wm Watermark

	// watermark subscribers
	subMu sync.Mutex
	subs  map[uint64]chan Watermark
	subID uint64
}

func NewDispatcher(initialCap int) *Dispatcher {
	if initialCap < 0 {
		initialCap = 0
	}
	return &Dispatcher{
		log:  make([]event.TxEvent, 0, initialCap),
		subs: make(map[uint64]chan Watermark),
	}
}

// Append 由 Ingest 调用：追加事件，分配 Seq，更新 watermark，并广播。
// 注意：Append 不做任何“窗口计算”，保证 ingest 永不被重算拖慢。
func (d *Dispatcher) Append(ev event.TxEvent) (seq uint64, wm Watermark) {
	d.mu.Lock()
	seq = d.nextSeq
	d.nextSeq++

	ev.Seq = seq
	d.log = append(d.log, ev)

	d.wm = Watermark{Seq: ev.Seq, Ts: ev.Ts}
	wm = d.wm
	d.mu.Unlock()

	d.broadcast(wm)
	return seq, wm
}

// Watermark 返回当前 watermark。
func (d *Dispatcher) Watermark() Watermark {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.wm
}

// Len 返回当前 log 长度（= nextSeq）。
func (d *Dispatcher) Len() uint64 {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.nextSeq
}

// ReadBySeq 返回 [from, to] 区间事件（包含两端），用于窗口 runner 增量推进。
// 若 to < from，返回空；若越界，会截断到现有范围。
func (d *Dispatcher) ReadBySeq(from, to uint64) []event.TxEvent {
	if to < from {
		return nil
	}

	d.mu.RLock()
	defer d.mu.RUnlock()

	if d.nextSeq == 0 {
		return nil
	}

	// clamp
	if from >= d.nextSeq {
		return nil
	}
	if to >= d.nextSeq {
		to = d.nextSeq - 1
	}

	// 这里做拷贝，避免窗口拿到内部 slice 造成数据竞争。
	n := int(to-from) + 1
	out := make([]event.TxEvent, 0, n)
	for i := from; i <= to; i++ {
		out = append(out, d.log[i])
	}
	return out
}

// SubscribeWatermark：窗口 runner 调用后，会收到 watermark 更新。
// 返回一个只读 channel 和取消函数（释放资源很关键）。
func (d *Dispatcher) SubscribeWatermark(buffer int) (<-chan Watermark, func()) {
	if buffer <= 0 {
		buffer = 1
	}

	d.subMu.Lock()
	id := d.subID
	d.subID++
	ch := make(chan Watermark, buffer)
	d.subs[id] = ch
	d.subMu.Unlock()

	// 订阅后先发当前 watermark，避免窗口启动时卡住。
	ch <- d.Watermark()

	cancel := func() {
		d.subMu.Lock()
		if c, ok := d.subs[id]; ok {
			delete(d.subs, id)
			close(c)
		}
		d.subMu.Unlock()
	}
	return ch, cancel
}

// WaitUntil：窗口 runner 常用工具，等 watermark.Seq >= targetSeq 或 ctx 取消。
func (d *Dispatcher) WaitUntil(ctx context.Context, targetSeq uint64) error {
	// 快路径
	if d.Watermark().Seq >= targetSeq {
		return nil
	}
	ch, cancel := d.SubscribeWatermark(8)
	defer cancel()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case wm, ok := <-ch:
			if !ok {
				return context.Canceled
			}
			if wm.Seq >= targetSeq {
				return nil
			}
		}
	}
}

func (d *Dispatcher) broadcast(wm Watermark) {
	d.subMu.Lock()
	defer d.subMu.Unlock()

	for _, ch := range d.subs {
		// 不阻塞 Append：慢订阅者丢更新也没关系（窗口可用最新 watermark）。
		select {
		case ch <- wm:
		default:
		}
	}
}
