package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math"
	"os"
	"os/signal"
	"sort"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/chenzhangda16/web3-logpipe/internal/logpipe/fetcher"
)

type config struct {
	baseURL     string
	concurrency int
	pageSize    int64
	startHeight int64
	pages       int64
	reportEvery time.Duration
	timeout     time.Duration
	warmup      time.Duration
}

func main() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds | log.Lshortfile)
	var cfg config
	flag.StringVar(&cfg.baseURL, "rpc", "http://127.0.0.1:18080", "RPC base URL (mockchain server)")
	flag.IntVar(&cfg.concurrency, "c", 8, "concurrency (number of in-flight RPC calls)")
	flag.Int64Var(&cfg.pageSize, "page", 200, "BlocksRange page size (to=from+page-1)")
	flag.Int64Var(&cfg.startHeight, "from", 1, "start height for BlocksRange")
	flag.Int64Var(&cfg.pages, "n", 1000, "number of pages to request (total requests). -1 for infinite")
	flag.DurationVar(&cfg.reportEvery, "report", 1*time.Second, "report interval")
	flag.DurationVar(&cfg.timeout, "timeout", 10*time.Second, "per-request timeout")
	flag.DurationVar(&cfg.warmup, "warmup", 2*time.Second, "warmup duration before stats reset")
	flag.Parse()

	if cfg.concurrency <= 0 {
		cfg.concurrency = 1
	}
	if cfg.pageSize <= 0 {
		cfg.pageSize = 200
	}
	if cfg.timeout <= 0 {
		cfg.timeout = 10 * time.Second
	}
	if cfg.reportEvery <= 0 {
		cfg.reportEvery = 1 * time.Second
	}

	log.Printf("[rpcbench] start rpc=%s c=%d page=%d from=%d n=%d timeout=%s warmup=%s report=%s",
		cfg.baseURL, cfg.concurrency, cfg.pageSize, cfg.startHeight, cfg.pages, cfg.timeout, cfg.warmup, cfg.reportEvery)

	// Root context with Ctrl+C cancel
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		ch := make(chan os.Signal, 2)
		signal.Notify(ch, os.Interrupt, syscall.SIGTERM)
		<-ch
		log.Printf("[rpcbench] signal received, stopping...")
		cancel()
	}()

	client := fetcher.NewRPCClient(cfg.baseURL)

	// Optional: quick head call so you know server is alive
	{
		t0 := time.Now()
		h, err := client.ChainHead(ctx)
		if err != nil {
			log.Printf("[rpcbench] ChainHead error: %v", err)
		} else {
			log.Printf("[rpcbench] ChainHead ok: head=%d empty=%v (cost=%s)", h.HeadNum, h.Empty, time.Since(t0))
		}
	}

	bench := newBench(cfg.reportEvery)

	// Warmup phase (optional): run requests but do not count, then reset stats.
	if cfg.warmup > 0 {
		log.Printf("[rpcbench] warmup %s ...", cfg.warmup)
		warmCtx, warmCancel := context.WithTimeout(ctx, cfg.warmup)
		_ = runLoad(warmCtx, client, cfg, nil)
		warmCancel()
		bench.reset()
		log.Printf("[rpcbench] warmup done, stats reset")
	}

	// Main run
	err := runLoad(ctx, client, cfg, bench)
	bench.stop()
	if err != nil && ctx.Err() == nil {
		log.Printf("[rpcbench] run error: %v", err)
	}

	// Final report
	bench.printFinal()
}

func runLoad(ctx context.Context, client *fetcher.RPCClient, cfg config, bench *benchState) error {
	var (
		reqID int64 = 0
	)

	// Stop condition: if cfg.pages >= 0, we issue exactly cfg.pages requests total.
	totalLimit := cfg.pages

	workCh := make(chan int64, cfg.concurrency*4)
	var wg sync.WaitGroup

	worker := func() {
		defer wg.Done()
		for idx := range workCh {
			if ctx.Err() != nil {
				return
			}

			from := cfg.startHeight + idx*cfg.pageSize
			to := from + cfg.pageSize - 1

			reqCtx, cancel := context.WithTimeout(ctx, cfg.timeout)
			t0 := time.Now()
			resp, err := client.BlocksRange(reqCtx, from, to)
			dur := time.Since(t0)
			cancel()

			if bench != nil {
				bench.observe(dur, err, respBlocksLen(resp))
			}
		}
	}

	wg.Add(cfg.concurrency)
	for i := 0; i < cfg.concurrency; i++ {
		go worker()
	}

	// Feeder
	feederErr := func() error {
		defer close(workCh)

		for {
			if ctx.Err() != nil {
				return ctx.Err()
			}

			cur := atomic.AddInt64(&reqID, 1) - 1
			if totalLimit >= 0 && cur >= totalLimit {
				return nil
			}

			select {
			case workCh <- cur:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}()

	wg.Wait()
	return feederErr
}

// ---- bench stats ----

type benchState struct {
	reportEvery time.Duration
	start       time.Time

	// counters
	reqOK    int64
	reqErr   int64
	blocks   int64
	latSumNS int64
	latMaxNS int64

	// latency samples (store a bounded window per report for percentiles)
	mu       sync.Mutex
	samples  []int64 // ns
	lastTick time.Time

	stopCh chan struct{}
}

func newBench(reportEvery time.Duration) *benchState {
	b := &benchState{
		reportEvery: reportEvery,
		start:       time.Now(),
		lastTick:    time.Now(),
		samples:     make([]int64, 0, 200000),
		stopCh:      make(chan struct{}),
	}
	go b.reportLoop()
	return b
}

func (b *benchState) reset() {
	atomic.StoreInt64(&b.reqOK, 0)
	atomic.StoreInt64(&b.reqErr, 0)
	atomic.StoreInt64(&b.blocks, 0)
	atomic.StoreInt64(&b.latSumNS, 0)
	atomic.StoreInt64(&b.latMaxNS, 0)

	b.mu.Lock()
	b.samples = b.samples[:0]
	b.mu.Unlock()

	b.start = time.Now()
	b.lastTick = time.Now()
}

func (b *benchState) stop() {
	select {
	case <-b.stopCh:
	default:
		close(b.stopCh)
	}
}

func (b *benchState) observe(d time.Duration, err error, blocks int) {
	ns := d.Nanoseconds()
	atomic.AddInt64(&b.latSumNS, ns)
	atomic.AddInt64(&b.blocks, int64(blocks))

	// max
	for {
		old := atomic.LoadInt64(&b.latMaxNS)
		if ns <= old {
			break
		}
		if atomic.CompareAndSwapInt64(&b.latMaxNS, old, ns) {
			break
		}
	}

	if err != nil {
		atomic.AddInt64(&b.reqErr, 1)
	} else {
		atomic.AddInt64(&b.reqOK, 1)
	}

	// store sample (bounded-ish: keep at most 500k)
	b.mu.Lock()
	if len(b.samples) < cap(b.samples) {
		b.samples = append(b.samples, ns)
	} else if len(b.samples) < 500000 {
		// grow slowly if needed (rare)
		b.samples = append(b.samples, ns)
	}
	b.mu.Unlock()
}

func (b *benchState) reportLoop() {
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

func (b *benchState) printTick() {
	now := time.Now()
	elapsed := now.Sub(b.lastTick)
	if elapsed <= 0 {
		return
	}
	b.lastTick = now

	ok := atomic.LoadInt64(&b.reqOK)
	er := atomic.LoadInt64(&b.reqErr)
	bl := atomic.LoadInt64(&b.blocks)
	sum := atomic.LoadInt64(&b.latSumNS)
	mx := atomic.LoadInt64(&b.latMaxNS)

	// snapshot samples for percentile
	b.mu.Lock()
	s := append([]int64(nil), b.samples...)
	// reset per-tick samples so percentiles reflect recent window, not whole run
	b.samples = b.samples[:0]
	b.mu.Unlock()

	p50, p90, p99 := percentiles(s)

	totalReq := ok + er
	qps := float64(totalReq) / elapsed.Seconds()
	bps := float64(bl) / elapsed.Seconds()
	avg := time.Duration(0)
	if totalReq > 0 {
		avg = time.Duration(sum / totalReq)
	}

	log.Printf("[rpcbench] qps=%.1f bps=%.1f ok=%d err=%d avg=%s p50=%s p90=%s p99=%s max=%s window=%s",
		qps, bps, ok, er, avg, p50, p90, p99, time.Duration(mx), elapsed)
}

func (b *benchState) printFinal() {
	now := time.Now()
	totalElapsed := now.Sub(b.start)

	ok := atomic.LoadInt64(&b.reqOK)
	er := atomic.LoadInt64(&b.reqErr)
	bl := atomic.LoadInt64(&b.blocks)
	sum := atomic.LoadInt64(&b.latSumNS)
	mx := atomic.LoadInt64(&b.latMaxNS)

	totalReq := ok + er
	qps := 0.0
	bps := 0.0
	if totalElapsed > 0 {
		qps = float64(totalReq) / totalElapsed.Seconds()
		bps = float64(bl) / totalElapsed.Seconds()
	}
	avg := time.Duration(0)
	if totalReq > 0 {
		avg = time.Duration(sum / totalReq)
	}

	log.Printf("[rpcbench] final elapsed=%s qps=%.1f bps=%.1f req=%d ok=%d err=%d avg=%s max=%s",
		totalElapsed, qps, bps, totalReq, ok, er, avg, time.Duration(mx))
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

// respBlocksLen adapts to your BlocksRange response type.
// If your response struct differs, change ONLY this function.
func respBlocksLen(resp any) int {
	// In your code: rangeResp.Blocks (slice)
	// Here, we do a type assertion to the expected type.
	rr, ok := resp.(*fetcher.BlocksRangeResp)
	if ok && rr != nil {
		return len(rr.Blocks)
	}
	// If your RPCClient returns a non-pointer response, add another assertion.
	if rr2, ok := resp.(fetcher.BlocksRangeResp); ok {
		return len(rr2.Blocks)
	}
	return 0
}

// ---- compile-time guard / helpful message ----

// If your fetcher package doesn't export BlocksRangeResp, just delete respBlocksLen()
// and replace the call site:
//
//	resp, err := client.BlocksRange(...)
//	blocksLen := len(resp.Blocks)
//
// and pass blocksLen into observe().
var _ = fmt.Sprintf
