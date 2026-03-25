package bench

import (
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/chenzhangda16/web3-logpipe/internal/logpipe/sysstat"
)

// FetchBench: 低侵入 perf/stat 采样器（类似 rpcbench 风格）
type FetchBench struct {
	tag         string
	tick        int64
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

	cpuReader *sysstat.CPUReader
	netReader *sysstat.NetReader
	iface     string
}

type QueueSampler func() QueueSnapshot

func (b *FetchBench) SetQueueSampler(fn QueueSampler) {
	if b == nil {
		return
	}
	b.sampler.Store(fn)
}

func NewFetchBench(tag string, every time.Duration, iface string, capacityBytesPS float64) *FetchBench {
	if every <= 0 {
		every = 1 * time.Second
	}
	b := &FetchBench{
		tag:         tag,
		reportEvery: every,
		samples:     make([]int64, 0, 200000),
		stopCh:      make(chan struct{}),
		cpuReader:   sysstat.NewCPUReader(),
		iface:       iface,
		netReader:   sysstat.NewNetReader(iface, capacityBytesPS),
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

	inpNS := atomic.SwapInt64(&b.inputBlockNS, 0)
	inpEv := atomic.SwapInt64(&b.inputBlockEv, 0)
	inpMx := atomic.SwapInt64(&b.inputBlockMaxNS, 0)

	inpAvgNS := int64(0)
	if inpEv > 0 {
		inpAvgNS = inpNS / inpEv
	}

	// snapshot samples then reset window
	b.mu.Lock()
	s := append([]int64(nil), b.samples...)
	b.samples = b.samples[:0]
	b.mu.Unlock()

	p50, p90, p99 := percentiles(s)

	totalPages := ok + er
	avgNS := int64(0)
	if totalPages > 0 {
		avgNS = sum / totalPages
	}

	sec := b.reportEvery.Seconds()

	rpcPPS := 0.0
	rpcBPS := 0.0
	enqBPS := 0.0
	ackBPS := 0.0
	if sec > 0 {
		rpcPPS = float64(totalPages) / sec
		rpcBPS = float64(bl) / sec
		enqBPS = float64(enq) / sec
		ackBPS = float64(ack) / sec
	}

	// core
	cpuPct := 0.0
	if b.cpuReader != nil {
		if v, ok, err := b.cpuReader.ReadPct(); err == nil && ok {
			cpuPct = v
		}
	}
	// wire
	rxBps, txBps, rxPct, txPct := 0.0, 0.0, 0.0, 0.0
	if b.netReader != nil {
		if rx, tx, rp, tp, ok, err := b.netReader.Read(); err == nil && ok {
			rxBps, txBps, rxPct, txPct = rx, tx, rp, tp
		}
	}

	fetchJson := FetchJson{
		Tag:  b.tag,
		Tick: atomic.AddInt64(&b.tick, 1),
		TsMs: time.Now().UnixMilli(),
		Core: CoreJSON{
			CpuPct:     cpuPct,
			Gomaxprocs: runtime.GOMAXPROCS(0),
			Goroutines: runtime.NumGoroutine(),
		},
		Wire: WireJSON{
			Iface: b.iface,
			RxBps: rxBps,
			TxBps: txBps,
			RxPct: rxPct,
			TxPct: txPct,
		},
		Flow: FetchFlowJSON{
			RPC: FetchRPCJSON{
				PPS:   rpcPPS,
				BPS:   rpcBPS,
				Ok:    ok,
				Err:   er,
				AvgNs: avgNS,
				P50Ns: p50.Nanoseconds(),
				P90Ns: p90.Nanoseconds(),
				P99Ns: p99.Nanoseconds(),
				MaxNs: mx,
			},

			Blk: FetchBlkJSON{
				EnqBPS: enqBPS,
				AckBPS: ackBPS,
			},

			Event: FetchEventJSON{
				ProdErr:  pe,
				LagFatal: lf,
				CkptSave: ck,
			},

			InputBlock: FetchBlockStageJSON{
				SumNs: inpNS,
				Ev:    inpEv,
				AvgNs: inpAvgNS,
				MaxNs: inpMx,
			},

			Q: FetchQueueJSON{
				Req: FetchQueueDepthJSON{
					Len: snap.PgReqLen,
					Cap: snap.PgReqCap,
				},
				Resp: FetchQueueDepthJSON{
					Len: snap.PgRespLen,
					Cap: snap.PgRespCap,
				},
			},
		},
	}
	EmitBench("fetcher", fetchJson)
}
