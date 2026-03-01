package bench

import (
	"sync/atomic"
	"time"
)

type QueueSnapshot struct {
	PgReqLen  int
	PgReqCap  int
	PgRespLen int
	PgRespCap int
}

type ProcSnapshot struct {
	RawChLen int
	RawChCap int

	WinChLen [4]int
	WinChCap [4]int
}

type WinPerf struct {
	Moves   int64
	BusyPct float64

	AvgWait time.Duration
	AvgWork time.Duration

	MaxWait time.Duration
	MaxWork time.Duration
}

type WinAgg struct {
	waitNS int64
	workNS int64
	moves  int64

	maxWaitNS int64
	maxWorkNS int64
}

func (a *WinAgg) Reset() {
	atomic.StoreInt64(&a.waitNS, 0)
	atomic.StoreInt64(&a.workNS, 0)
	atomic.StoreInt64(&a.moves, 0)
	atomic.StoreInt64(&a.maxWaitNS, 0)
	atomic.StoreInt64(&a.maxWorkNS, 0)
}

func (a *WinAgg) AddWait(d time.Duration) {
	ns := d.Nanoseconds()
	atomic.AddInt64(&a.waitNS, ns)
	maxCAS(&a.maxWaitNS, ns)
}
func (a *WinAgg) AddWork(d time.Duration) {
	ns := d.Nanoseconds()
	atomic.AddInt64(&a.workNS, ns)
	maxCAS(&a.maxWorkNS, ns)
}
func (a *WinAgg) AddMove(n int64) { atomic.AddInt64(&a.moves, n) }

// 每次 bench tick 取一份并清零
type WinTick struct {
	Moves int64

	WaitNS int64
	WorkNS int64

	MaxWait time.Duration
	MaxWork time.Duration
}

func (a *WinAgg) SwapTick() WinTick {
	return WinTick{
		Moves:   atomic.SwapInt64(&a.moves, 0),
		WaitNS:  atomic.SwapInt64(&a.waitNS, 0),
		WorkNS:  atomic.SwapInt64(&a.workNS, 0),
		MaxWait: time.Duration(atomic.SwapInt64(&a.maxWaitNS, 0)),
		MaxWork: time.Duration(atomic.SwapInt64(&a.maxWorkNS, 0)),
	}
}

// 复用上次给你的 maxCAS
func maxCAS(dst *int64, v int64) {
	for {
		old := atomic.LoadInt64(dst)
		if v <= old {
			return
		}
		if atomic.CompareAndSwapInt64(dst, old, v) {
			return
		}
	}
}
