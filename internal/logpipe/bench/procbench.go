package bench

import (
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/chenzhangda16/web3-logpipe/internal/logpipe/sysstat"
)

// ProcBench：processor 低侵入 perf/stat 采样器（统一 ingest/dispatcher/window）
type ProcBench struct {
	tag         string
	tick        int64
	reportEvery time.Duration
	stopCh      chan struct{}

	// ---- ingest / consume ----
	consumeMsgs int64

	spoolOK   int64
	spoolErr  int64
	spoolLatS int64 // ns sum
	spoolLatM int64 // ns max

	decodeOK   int64
	decodeErr  int64
	decodeLatS int64
	decodeLatM int64

	// optional: blocked on rawCh send (backpressure)
	phaseSteady int32 // 0=cold 1=steady

	lastReOffset      int64
	lastBlkNum        int64
	rawSendBlockNS    int64
	rawSendBlockEv    int64
	rawSendBlockMaxNS int64

	// ---- dispatcher ----
	winMoveN int64
	// dispatcher winmove send blocking (per window)
	winMoveBlockNS    int64
	winMoveBlockEv    int64
	winMoveBlockMaxNS int64
	winMoveWNS        [4]int64
	winMoveWEv        [4]int64
	winMoveWMaxNS     [4]int64

	// ---- snapshots (best-effort) ----
	snapMu sync.Mutex
	snap   ProcSnapshot

	// ---- latency samples (bounded per window) ----
	mu            sync.Mutex
	decodeSamples []int64

	// ---- window perf (per winIdx) ----
	winMu   sync.Mutex
	winPerf map[int]WinPerf // last 1s aggregated by runner itself
	winAgg  [4]WinAgg
	started int32

	cpuReader *sysstat.CPUReader
	netReader *sysstat.NetReader
	iface     string
}

func NewProcBench(tag string, every time.Duration, iface string, capacityBytesPS float64) *ProcBench {
	if every <= 0 {
		every = 1 * time.Second
	}
	b := &ProcBench{
		tag:           tag,
		reportEvery:   every,
		stopCh:        make(chan struct{}),
		decodeSamples: make([]int64, 0, 200000),
		winPerf:       make(map[int]WinPerf, 8),
		cpuReader:     sysstat.NewCPUReader(),
		netReader:     sysstat.NewNetReader(iface, capacityBytesPS),
		iface:         iface,
	}
	go b.reportLoop()
	return b
}

func (b *ProcBench) Start() {
	if b == nil {
		return
	}
	if atomic.CompareAndSwapInt32(&b.started, 0, 1) {
		b.resetAll()
	}
}

func (b *ProcBench) resetAll() {
	atomic.StoreInt64(&b.consumeMsgs, 0)
	atomic.StoreInt64(&b.spoolOK, 0)
	atomic.StoreInt64(&b.spoolErr, 0)
	atomic.StoreInt64(&b.spoolLatS, 0)
	atomic.StoreInt64(&b.spoolLatM, 0)

	atomic.StoreInt64(&b.decodeOK, 0)
	atomic.StoreInt64(&b.decodeErr, 0)
	atomic.StoreInt64(&b.decodeLatS, 0)
	atomic.StoreInt64(&b.decodeLatM, 0)

	atomic.StoreInt64(&b.rawSendBlockNS, 0)
	atomic.StoreInt64(&b.rawSendBlockEv, 0)
	atomic.StoreInt64(&b.rawSendBlockMaxNS, 0)

	atomic.StoreInt64(&b.winMoveN, 0)

	b.mu.Lock()
	b.decodeSamples = b.decodeSamples[:0]
	b.mu.Unlock()

	b.snapMu.Lock()
	b.snap = ProcSnapshot{}
	b.snapMu.Unlock()

	// winAgg 也清一下
	for i := range b.winAgg {
		b.winAgg[i].Reset()
	}
}

func (b *ProcBench) Stop() {
	select {
	case <-b.stopCh:
	default:
		close(b.stopCh)
	}
}

// ---- ingest hooks ----

func (b *ProcBench) AddConsumeMsg(n int) { atomic.AddInt64(&b.consumeMsgs, int64(n)) }

func (b *ProcBench) ObserveSpool(d time.Duration, err error) {
	ns := d.Nanoseconds()
	atomic.AddInt64(&b.spoolLatS, ns)
	maxCAS(&b.spoolLatM, ns)
	if err != nil {
		atomic.AddInt64(&b.spoolErr, 1)
		return
	}
	atomic.AddInt64(&b.spoolOK, 1)
}

func (b *ProcBench) ObserveDecode(d time.Duration, err error) {
	ns := d.Nanoseconds()
	atomic.AddInt64(&b.decodeLatS, ns)
	maxCAS(&b.decodeLatM, ns)
	if err != nil {
		atomic.AddInt64(&b.decodeErr, 1)
		return
	}
	atomic.AddInt64(&b.decodeOK, 1)

	// per-window samples for percentiles
	b.mu.Lock()
	if len(b.decodeSamples) < 200000 {
		b.decodeSamples = append(b.decodeSamples, ns)
	}
	b.mu.Unlock()
}

func (b *ProcBench) AddRawSendBlocked(d time.Duration) {
	ns := d.Nanoseconds()
	atomic.AddInt64(&b.rawSendBlockNS, ns)
	atomic.AddInt64(&b.rawSendBlockEv, 1)
	maxCAS(&b.rawSendBlockMaxNS, ns)
}

// ---- dispatcher hooks ----

func (b *ProcBench) AddWinMove(n int) { atomic.AddInt64(&b.winMoveN, int64(n)) }

func (b *ProcBench) SetSnapshot(s ProcSnapshot) {
	b.snapMu.Lock()
	b.snap = s
	b.snapMu.Unlock()
}

// ---- window hooks ----
// runner 每秒算完一份 WinPerf，喂回来
func (b *ProcBench) SetWinPerf(winIdx int, p WinPerf) {
	b.winMu.Lock()
	b.winPerf[winIdx] = p
	b.winMu.Unlock()
}

// ---- report loop ----

func (b *ProcBench) reportLoop() {
	t := time.NewTicker(b.reportEvery)
	defer t.Stop()

	for {
		select {
		case <-b.stopCh:
			return
		case <-t.C:
			if atomic.LoadInt32(&b.started) == 0 {
				continue
			}
			b.printTick()
		}
	}
}

func (b *ProcBench) printTick() {
	sec := b.reportEvery.Seconds()

	// counters
	msgs := atomic.SwapInt64(&b.consumeMsgs, 0)

	spOK := atomic.SwapInt64(&b.spoolOK, 0)
	spEr := atomic.SwapInt64(&b.spoolErr, 0)
	spSum := atomic.SwapInt64(&b.spoolLatS, 0)
	spMax := atomic.SwapInt64(&b.spoolLatM, 0)

	deOK := atomic.SwapInt64(&b.decodeOK, 0)
	deEr := atomic.SwapInt64(&b.decodeErr, 0)
	deSum := atomic.SwapInt64(&b.decodeLatS, 0)
	deMax := atomic.SwapInt64(&b.decodeLatM, 0)

	rawBlkNS := atomic.SwapInt64(&b.rawSendBlockNS, 0)
	rawBlkEv := atomic.SwapInt64(&b.rawSendBlockEv, 0)
	rawBlkMx := atomic.SwapInt64(&b.rawSendBlockMaxNS, 0)

	winMv := atomic.SwapInt64(&b.winMoveN, 0)

	wmBlkNS := atomic.SwapInt64(&b.winMoveBlockNS, 0)
	wmBlkEv := atomic.SwapInt64(&b.winMoveBlockEv, 0)
	wmBlkMx := atomic.SwapInt64(&b.winMoveBlockMaxNS, 0)

	wmBlkAvg := time.Duration(0)
	if wmBlkEv > 0 {
		wmBlkAvg = time.Duration(wmBlkNS / wmBlkEv)
	}

	phase := "cold"
	if atomic.LoadInt32(&b.phaseSteady) == 1 {
		phase = "steady"
	}

	off := atomic.LoadInt64(&b.lastReOffset)
	bn := atomic.LoadInt64(&b.lastBlkNum)

	// samples
	b.mu.Lock()
	s := append([]int64(nil), b.decodeSamples...)
	b.decodeSamples = b.decodeSamples[:0]
	b.mu.Unlock()
	p50, p90, p99 := percentiles(s)

	// snapshot
	b.snapMu.Lock()
	snap := b.snap
	b.snapMu.Unlock()

	// derived
	msgPS := float64(msgs) / sec

	spTot := spOK + spEr
	spAvg := time.Duration(0)
	if spTot > 0 {
		spAvg = time.Duration(spSum / spTot)
	}

	deTot := deOK + deEr
	deAvg := time.Duration(0)
	if deTot > 0 {
		deAvg = time.Duration(deSum / deTot)
	}

	rawBlkAvg := time.Duration(0)
	if rawBlkEv > 0 {
		rawBlkAvg = time.Duration(rawBlkNS / rawBlkEv)
	}

	elapsedNS := b.reportEvery.Nanoseconds()

	var wEv [4]int64
	var wNS [4]int64
	var wMx [4]int64
	var wAvg [4]time.Duration

	winMoveW := make(map[string]ProcBlockStageJSON, len(b.winAgg))
	wins := make(map[string]ProcWinJSON, len(b.winAgg))
	winQ := make(map[string]ProcQueueDepthJSON, len(b.winAgg))
	coreW := make(map[string]ProcCoreWin, len(b.winAgg))

	for i := 0; i < len(b.winAgg); i++ {
		t := b.winAgg[i].SwapTick()

		wEv[i] = atomic.SwapInt64(&b.winMoveWEv[i], 0)
		wNS[i] = atomic.SwapInt64(&b.winMoveWNS[i], 0)
		wMx[i] = atomic.SwapInt64(&b.winMoveWMaxNS[i], 0)
		if wEv[i] > 0 {
			wAvg[i] = time.Duration(wNS[i] / wEv[i])
		}

		k := strconv.Itoa(i)

		winMoveW[k] = ProcBlockStageJSON{
			Ev:    wEv[i],
			SumNs: wNS[i],
			AvgNs: wAvg[i].Nanoseconds(),
			MaxNs: wMx[i],
		}

		winQ[k] = ProcQueueDepthJSON{
			Len: snap.WinChLen[i],
			Cap: snap.WinChCap[i],
		}

		busyPct := 0.0
		if elapsedNS > 0 {
			busyPct = float64(t.WorkNS) * 100 / float64(elapsedNS)
			if busyPct < 0 {
				busyPct = 0
			}
			if busyPct > 100 {
				busyPct = 100
			}
		}

		coreW[k] = ProcCoreWin{
			BusyPct:   busyPct,
			WorkCoreS: float64(t.WorkNS) / 1e9,
			WaitCoreS: float64(t.WaitNS) / 1e9,
			Moves:     t.Moves,
		}

		avgWaitNS := int64(0)
		avgWorkNS := int64(0)
		if t.Moves > 0 {
			avgWaitNS = t.WaitNS / t.Moves
			avgWorkNS = t.WorkNS / t.Moves
		}

		wins[k] = ProcWinJSON{
			BusyPct:   busyPct,
			Moves:     t.Moves,
			AvgWaitNs: avgWaitNS,
			AvgWorkNs: avgWorkNS,
			MaxWaitNs: t.MaxWait.Nanoseconds(),
			MaxWorkNs: t.MaxWork.Nanoseconds(),
		}
	}

	flow := ProcFlowJSON{
		Tag:   b.tag,
		Tick:  atomic.AddInt64(&b.tick, 1), // 或者用你现有 tick 来源
		TsMs:  time.Now().UnixMilli(),
		Phase: phase,

		ReOff: off,
		Blk:   bn,

		MsgPS: msgPS,
		Msgs:  msgs,

		Spool: ProcSpoolJSON{
			Ok:    spOK,
			Err:   spEr,
			AvgNs: spAvg.Nanoseconds(),
			MaxNs: spMax,
		},

		Decode: ProcDecodeJSON{
			Ok:    deOK,
			Err:   deEr,
			AvgNs: deAvg.Nanoseconds(),
			P50Ns: p50.Nanoseconds(),
			P90Ns: p90.Nanoseconds(),
			P99Ns: p99.Nanoseconds(),
			MaxNs: deMax,
		},

		RawSendBlock: ProcBlockStageJSON{
			Ev:    rawBlkEv,
			SumNs: rawBlkNS,
			AvgNs: rawBlkAvg.Nanoseconds(),
			MaxNs: rawBlkMx,
		},

		WinMove: ProcWinMoveJSON{
			N: winMv,
			Block: ProcBlockStageJSON{
				Ev:    wmBlkEv,
				SumNs: wmBlkNS,
				AvgNs: wmBlkAvg.Nanoseconds(),
				MaxNs: wmBlkMx,
			},
			W: winMoveW,
		},

		Q: ProcQueueJSON{
			Raw: ProcQueueDepthJSON{
				Len: snap.RawChLen,
				Cap: snap.RawChCap,
			},
			Win: winQ,
		},

		Wins: wins,
	}

	EmitBench("processor", "flow", flow)

	cpuPct := 0.0
	if b.cpuReader != nil {
		if v, ok, err := b.cpuReader.ReadPct(); err == nil && ok {
			cpuPct = v
		}
	}

	core := ProcCoreJSON{
		CoreJSON: CoreJSON{
			Tag:        b.tag,
			Tick:       flow.Tick,
			TsMs:       flow.TsMs,
			CpuPct:     cpuPct,
			Gomaxprocs: runtime.GOMAXPROCS(0),
			Goroutines: runtime.NumGoroutine(),
		},
		W: coreW,
	}

	EmitBench("processor", "core", core)

	rxBps, txBps, rxPct, txPct := 0.0, 0.0, 0.0, 0.0
	if b.netReader != nil {
		if rx, tx, rp, tp, ok, err := b.netReader.Read(); err == nil && ok {
			rxBps, txBps, rxPct, txPct = rx, tx, rp, tp
		}
	}

	wire := WireJSON{
		Tag:   flow.Tag,
		Tick:  flow.Tick,
		TsMs:  flow.TsMs,
		Iface: b.iface,
		RxBps: rxBps,
		TxBps: txBps,
		RxPct: rxPct,
		TxPct: txPct,
	}

	EmitBench("processor", "wire", wire)
}

func (b *ProcBench) AddWinMoveBlock(d time.Duration) {
	if b == nil {
		return
	}
	ns := d.Nanoseconds()
	atomic.AddInt64(&b.winMoveBlockNS, ns)
	atomic.AddInt64(&b.winMoveBlockEv, 1)
	maxCAS(&b.winMoveBlockMaxNS, ns)
}

func formatWin(i int, p WinPerf) string {
	// 紧凑：w0:busy=31% mv=123 aw=2ms ak=5ms mw=20ms mk=40ms
	return "w" + itoa(i) +
		":busy=" + f1(p.BusyPct) + "%" +
		" mv=" + itoa64(p.Moves) +
		" aw=" + p.AvgWait.String() +
		" ak=" + p.AvgWork.String() +
		" mw=" + p.MaxWait.String() +
		" mk=" + p.MaxWork.String()
}

// tiny itoa helpers（避免 fmt 带来的额外开销；也可以直接用 strconv）
func itoa(i int) string     { return itoa64(int64(i)) }
func itoa64(i int64) string { return strconvFormatInt(i) }
func f1(v float64) string   { return strconvFormatFloat1(v) }

// 你可以用 strconv.FormatInt / FormatFloat；这里写成函数只是为了占位
func strconvFormatInt(i int64) string      { return strconv.FormatInt(i, 10) }
func strconvFormatFloat1(v float64) string { return strconv.FormatFloat(v, 'f', 1, 64) }

func (b *ProcBench) WinAddWait(winIdx int, d time.Duration) {
	if b == nil || winIdx < 0 || winIdx >= len(b.winAgg) {
		return
	}
	b.winAgg[winIdx].AddWait(d)
}
func (b *ProcBench) WinAddWork(winIdx int, d time.Duration) {
	if b == nil || winIdx < 0 || winIdx >= len(b.winAgg) {
		return
	}
	b.winAgg[winIdx].AddWork(d)
}
func (b *ProcBench) WinAddMove(winIdx int, n int64) {
	if b == nil || winIdx < 0 || winIdx >= len(b.winAgg) {
		return
	}
	b.winAgg[winIdx].AddMove(n)
}

func (b *ProcBench) AddWinMoveBlockWin(winIdx int, d time.Duration) {
	if b == nil || winIdx < 0 || winIdx >= 4 {
		return
	}
	ns := d.Nanoseconds()

	atomic.AddInt64(&b.winMoveWNS[winIdx], ns)
	atomic.AddInt64(&b.winMoveWEv[winIdx], 1)

	// maxCAS: 你之前已有类似 maxCAS(&x, ns) 工具就复用
	maxCAS(&b.winMoveWMaxNS[winIdx], ns)
}

func (b *ProcBench) MarkSteady() {
	if b == nil {
		return
	}
	atomic.StoreInt32(&b.phaseSteady, 1)
}

func (b *ProcBench) SetLastProgress(reOffset, blkNum int64) {
	if b == nil {
		return
	}
	atomic.StoreInt64(&b.lastReOffset, reOffset)
	atomic.StoreInt64(&b.lastBlkNum, blkNum)
}
