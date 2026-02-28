package fetcher

import (
	"log"
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// FetchBench: 低侵入 perf/stat 采样器（类似 rpcbench 风格）
type FetchBench struct {
	tag         string
	reportEvery time.Duration

	// counters
	rpcOKPages  int64
	rpcErrPages int64
	rpcBlocks   int64
	rpcLatSumNS int64
	rpcLatMaxNS int64

	enqBlocks int64
	ackBlocks int64

	prodErr  int64
	lagFatal int64
	ckptSave int64

	// optional: input block time (if you instrument enqueue)
	inputBlockNS    int64 // total blocked ns in window
	inputBlockEv    int64 // blocked events in window
	inputBlockMaxNS int64

	// latency samples (per-tick window, bounded)
	mu      sync.Mutex
	samples []int64 // rpc latency ns, per-window
	stopCh  chan struct{}
	sampler atomic.Value // stores QueueSampler
}

type QueueSnapshot struct {
	PgReqLen  int
	PgReqCap  int
	PgRespLen int
	PgRespCap int
}

type QueueSampler func() QueueSnapshot

func (b *FetchBench) SetQueueSampler(fn QueueSampler) {
	if b == nil {
		return
	}
	b.sampler.Store(fn)
}

func NewFetchBench(tag string, every time.Duration) *FetchBench {
	if every <= 0 {
		every = 1 * time.Second
	}
	b := &FetchBench{
		tag:         tag,
		reportEvery: every,
		samples:     make([]int64, 0, 200000),
		stopCh:      make(chan struct{}),
	}
	go b.reportLoop()
	return b
}

func (b *FetchBench) Stop() {
	select {
	case <-b.stopCh:
	default:
		close(b.stopCh)
	}
}

func (b *FetchBench) ObserveRPC(d time.Duration, err error, blocks int) {
	ns := d.Nanoseconds()
	atomic.AddInt64(&b.rpcLatSumNS, ns)
	atomic.AddInt64(&b.rpcBlocks, int64(blocks))

	for {
		old := atomic.LoadInt64(&b.rpcLatMaxNS)
		if ns <= old {
			break
		}
		if atomic.CompareAndSwapInt64(&b.rpcLatMaxNS, old, ns) {
			break
		}
	}

	if err != nil {
		atomic.AddInt64(&b.rpcErrPages, 1)
		return
	}
	atomic.AddInt64(&b.rpcOKPages, 1)

	// per-window samples for percentiles
	b.mu.Lock()
	if len(b.samples) < 200000 { // bound per-window
		b.samples = append(b.samples, ns)
	}
	b.mu.Unlock()
}

func (b *FetchBench) AddEnqBlocks(n int) { atomic.AddInt64(&b.enqBlocks, int64(n)) }
func (b *FetchBench) AddAckBlocks(n int) { atomic.AddInt64(&b.ackBlocks, int64(n)) }
func (b *FetchBench) AddProdErr()        { atomic.AddInt64(&b.prodErr, 1) }
func (b *FetchBench) AddLagFatal()       { atomic.AddInt64(&b.lagFatal, 1) }
func (b *FetchBench) AddCkptSave()       { atomic.AddInt64(&b.ckptSave, 1) }
func (b *FetchBench) AddInputBlocked(d time.Duration) {
	ns := d.Nanoseconds()
	atomic.AddInt64(&b.inputBlockNS, ns)
	atomic.AddInt64(&b.inputBlockEv, 1)

	for {
		old := atomic.LoadInt64(&b.inputBlockMaxNS)
		if ns <= old {
			break
		}
		if atomic.CompareAndSwapInt64(&b.inputBlockMaxNS, old, ns) {
			break
		}
	}
}

func (b *FetchBench) reportLoop() {
	t := time.NewTicker(b.reportEvery)
	defer t.Stop()
	for {
		select {
		case <-b.stopCh:
			return
		case <-t.C:
			b.printTick()
		}
	}
}

func (b *FetchBench) printTick() {
	snap := QueueSnapshot{}
	if v := b.sampler.Load(); v != nil {
		if fn, ok := v.(QueueSampler); ok && fn != nil {
			snap = fn()
		}
	}
	ok := atomic.SwapInt64(&b.rpcOKPages, 0)
	er := atomic.SwapInt64(&b.rpcErrPages, 0)
	bl := atomic.SwapInt64(&b.rpcBlocks, 0)
	sum := atomic.SwapInt64(&b.rpcLatSumNS, 0)
	mx := atomic.SwapInt64(&b.rpcLatMaxNS, 0)

	enq := atomic.SwapInt64(&b.enqBlocks, 0)
	ack := atomic.SwapInt64(&b.ackBlocks, 0)

	pe := atomic.SwapInt64(&b.prodErr, 0)
	lf := atomic.SwapInt64(&b.lagFatal, 0)
	ck := atomic.SwapInt64(&b.ckptSave, 0)

	inp := atomic.SwapInt64(&b.inputBlockNS, 0)
	inpNS := atomic.SwapInt64(&b.inputBlockNS, 0)
	inpEv := atomic.SwapInt64(&b.inputBlockEv, 0)
	inpMx := atomic.SwapInt64(&b.inputBlockMaxNS, 0)

	inpAvg := time.Duration(0)
	if inpEv > 0 {
		inpAvg = time.Duration(inpNS / inpEv)
	}

	// snapshot samples then reset window
	b.mu.Lock()
	s := append([]int64(nil), b.samples...)
	b.samples = b.samples[:0]
	b.mu.Unlock()

	p50, p90, p99 := percentiles(s)

	totalPages := ok + er
	avg := time.Duration(0)
	if totalPages > 0 {
		avg = time.Duration(sum / totalPages)
	}

	sec := b.reportEvery.Seconds()
	rpcPPS := float64(totalPages) / sec
	rpcBPS := float64(bl) / sec

	enqBPS := float64(enq) / sec
	ackBPS := float64(ack) / sec

	log.Printf("[fetchbench] tag=%s rpc_pps=%.1f rpc_bps=%.1f rpc_ok=%d rpc_err=%d rpc_avg=%s rpc_p50=%s rpc_p90=%s rpc_p99=%s rpc_max=%s "+
		"enq_bps=%.1f ack_bps=%.1f prod_err=%d lag_fatal=%d ckpt_save=%d input_block=%s window=%s "+
		"req=%d/%d resp=%d/%d input_block_ev=%d input_block_sum=%s input_block_avg=%s input_block_max=%s",
		b.tag,
		rpcPPS, rpcBPS, ok, er, avg, p50, p90, p99, time.Duration(mx),
		enqBPS, ackBPS, pe, lf, ck, time.Duration(inp), b.reportEvery,
		snap.PgReqLen, snap.PgReqCap, snap.PgRespLen, snap.PgRespCap,
		inpEv, time.Duration(inpNS), inpAvg, time.Duration(inpMx),
	)
}

func percentiles(ns []int64) (p50, p90, p99 time.Duration) {
	if len(ns) == 0 {
		return 0, 0, 0
	}
	sort.Slice(ns, func(i, j int) bool { return ns[i] < ns[j] })
	p50 = time.Duration(ns[idx(ns, 0.50)])
	p90 = time.Duration(ns[idx(ns, 0.90)])
	p99 = time.Duration(ns[idx(ns, 0.99)])
	return
}
func idx(ns []int64, q float64) int {
	if len(ns) == 0 {
		return 0
	}
	if q <= 0 {
		return 0
	}
	if q >= 1 {
		return len(ns) - 1
	}
	x := int(math.Ceil(float64(len(ns))*q)) - 1
	if x < 0 {
		x = 0
	}
	if x >= len(ns) {
		x = len(ns) - 1
	}
	return x
}
