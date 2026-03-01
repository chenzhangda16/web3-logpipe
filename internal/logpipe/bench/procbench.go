package bench

import (
	"fmt"
	"log"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// ProcBench：processor 低侵入 perf/stat 采样器（统一 ingest/dispatcher/window）
type ProcBench struct {
	tag         string
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
}

func NewProcBench(tag string, every time.Duration) *ProcBench {
	if every <= 0 {
		every = 1 * time.Second
	}
	b := &ProcBench{
		tag:           tag,
		reportEvery:   every,
		stopCh:        make(chan struct{}),
		decodeSamples: make([]int64, 0, 200000),
		winPerf:       make(map[int]WinPerf, 8),
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

	// window perf snapshot
	b.winMu.Lock()
	wp := make(map[int]WinPerf, len(b.winPerf))
	for k, v := range b.winPerf {
		wp[k] = v
	}
	b.winMu.Unlock()

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

	// format window perf compactly
	// 例：w0:busy32% mv120 avgW=8ms avgK=1ms | w3:busy95% ...
	winStr := sep

	elapsed := b.reportEvery
	var wEv [4]int64
	var wNS [4]int64
	var wMx [4]int64
	var wAvg [4]time.Duration

	for i := 0; i < len(b.winAgg); i++ {
		t := b.winAgg[i].SwapTick()
		wEv[i] = atomic.SwapInt64(&b.winMoveWEv[i], 0)
		wNS[i] = atomic.SwapInt64(&b.winMoveWNS[i], 0)
		wMx[i] = atomic.SwapInt64(&b.winMoveWMaxNS[i], 0)
		if wEv[i] > 0 {
			wAvg[i] = time.Duration(wNS[i] / wEv[i])
		}

		busy := 0.0
		if elapsed > 0 {
			busy = float64(t.WorkNS) / float64(elapsed.Nanoseconds())
			if busy < 0 {
				busy = 0
			}
			if busy > 1 {
				busy = 1
			}
		}

		avgWait := time.Duration(0)
		avgWork := time.Duration(0)
		if t.Moves > 0 {
			avgWait = time.Duration(t.WaitNS / t.Moves)
			avgWork = time.Duration(t.WorkNS / t.Moves)
		}

		winStr += fmt.Sprintf(
			"w%d:busy=%.1f%% mv=%d aw=%s ak=%s mw=%s mk=%s"+sep,
			i,
			busy*100,
			t.Moves,
			avgWait,
			avgWork,
			t.MaxWait,
			t.MaxWork,
		)
	}

	log.Printf("[procbench][%s] tag=%s re_off=%d blk=%d msg_ps=%.1f msgs=%d spool_ok=%d spool_err=%d spool_avg=%s spool_max=%s "+
		"decode_ok=%d decode_err=%d decode_avg=%s decode_p50=%s decode_p90=%s decode_p99=%s decode_max=%s "+
		"raw_send_block_ev=%d raw_send_block_sum=%s raw_send_block_avg=%s raw_send_block_max=%s "+
		"winmove=%d winmove_block_ev=%d winmove_block_sum=%s winmove_block_avg=%s winmove_block_max=%s"+sep+
		"wm0_ev=%d wm0_sum=%s wm0_avg=%s wm0_max=%s"+sep+
		"wm1_ev=%d wm1_sum=%s wm1_avg=%s wm1_max=%s"+sep+
		"wm1_ev=%d wm1_sum=%s wm1_avg=%s wm1_max=%s"+sep+
		"wm1_ev=%d wm1_sum=%s wm1_avg=%s wm1_max=%s"+sep+
		"rawCh=%d/%d winCh0=%d/%d winCh1=%d/%d winCh2=%d/%d winCh3=%d/%d wins={%s}",
		phase, b.tag, off, bn,
		msgPS, msgs,
		spOK, spEr, spAvg, time.Duration(spMax),
		deOK, deEr, deAvg, p50, p90, p99, time.Duration(deMax),
		rawBlkEv, time.Duration(rawBlkNS), rawBlkAvg, time.Duration(rawBlkMx),
		winMv, wmBlkEv, time.Duration(wmBlkNS), wmBlkAvg, time.Duration(wmBlkMx),
		wEv[0], time.Duration(wNS[0]), wAvg[0], time.Duration(wMx[0]),
		wEv[1], time.Duration(wNS[1]), wAvg[1], time.Duration(wMx[1]),
		wEv[2], time.Duration(wNS[2]), wAvg[2], time.Duration(wMx[2]),
		wEv[3], time.Duration(wNS[3]), wAvg[3], time.Duration(wMx[3]),
		snap.RawChLen, snap.RawChCap,
		snap.WinChLen[0], snap.WinChCap[0],
		snap.WinChLen[1], snap.WinChCap[1],
		snap.WinChLen[2], snap.WinChCap[2],
		snap.WinChLen[3], snap.WinChCap[3],
		winStr,
	)
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
