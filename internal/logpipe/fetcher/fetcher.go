package fetcher

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/chenzhangda16/web3-logpipe/internal/logpipe/retry"
)

var ErrRangeUnacceptable = errors.New("range response unacceptable")

type Config struct {
	RPCBaseURL string

	Brokers string // comma-separated
	Topic   string

	BackfillSec int64 // -1 disables

	PageSize int

	PollHeadEvery time.Duration

	CheckpointPath string
	CheckpointTick time.Duration

	RPCConcurrency int // 上游 worker 数（P）
	Partitions     int // Kafka 分区数（用于 height%P）
}

type Fetcher struct {
	cfg Config

	p            perf
	errCh        chan error
	nextHeadPoll time.Time
	pgReqCh      chan pageReq
	pgOutCh      []chan struct{}
	pgRespCh     chan pageResp
	rpc          *RPCClient
	prod         *Producer
	ckpt         Checkpoint
	close        func() error
}

type pageReq struct {
	seq  int64
	from int64
	to   int64
}

type pageResp struct {
	seq  int64
	resp BlocksRangeResp
}

func New(cfg Config) (*Fetcher, error) {
	if cfg.RPCBaseURL == "" {
		return nil, errors.New("rpc base url is empty")
	}
	if cfg.Topic == "" {
		return nil, errors.New("kafka topic is empty")
	}
	if cfg.PageSize <= 0 {
		cfg.PageSize = 200
	}
	if cfg.PollHeadEvery <= 0 {
		cfg.PollHeadEvery = 2 * time.Second
	}
	if cfg.CheckpointPath == "" {
		cfg.CheckpointPath = "./data/fetcher.ckpt"
	}

	rpc := NewRPCClient(cfg.RPCBaseURL)

	ckpt, err := NewFileCheckpoint(cfg.CheckpointPath)
	if err != nil {
		return nil, err
	}

	prod, err := NewProducer(cfg.Brokers, cfg.Topic)
	if err != nil {
		return nil, err
	}

	f := &Fetcher{
		cfg:      cfg,
		rpc:      rpc,
		prod:     prod,
		ckpt:     ckpt,
		pgReqCh:  make(chan pageReq, cfg.RPCConcurrency*4),
		pgRespCh: make(chan pageResp, cfg.RPCConcurrency*4),
	}
	for i := 0; i < cfg.RPCConcurrency; i++ {
		f.pgOutCh = append(f.pgOutCh, make(chan struct{}, 1))
	}
	f.pgOutCh[0] <- struct{}{}
	f.close = func() error {
		_ = prod.Close()
		return nil
	}
	return f, nil
}

func (f *Fetcher) Close() error { return f.close() }

type perf struct {
	loops  int64
	pages  int64
	blocks int64
	retryN int64

	// "wait"
	sleepTotal time.Duration
	maxSleep   time.Duration

	// "work"
	workTotal time.Duration
	maxWork   time.Duration

	// breakdown
	rpcTotal  time.Duration
	maxRPC    time.Duration
	headTotal time.Duration
	maxHead   time.Duration
	headCalls int64

	prodTotal time.Duration
	maxProd   time.Duration

	ckptTotal time.Duration
	maxCkpt   time.Duration

	sampleStart time.Time
}

func (p *perf) sleep(d time.Duration) {
	if d <= 0 {
		return
	}
	time.Sleep(d)
	p.sleepTotal += d
	if d > p.maxSleep {
		p.maxSleep = d
	}
}

func (p *perf) avg(d time.Duration, n int64) time.Duration {
	if n <= 0 {
		return 0
	}
	return time.Duration(int64(d) / n)
}

func (p *perf) flush(now time.Time) {
	elapsed := now.Sub(p.sampleStart)
	if elapsed <= 0 {
		return
	}

	total := p.sleepTotal + p.workTotal
	busyPct := 0.0
	if total > 0 {
		busyPct = float64(p.workTotal) / float64(total) * 100.0
	}

	pps := 0.0
	bps := 0.0
	if elapsed.Seconds() > 0 {
		pps = float64(p.pages) / elapsed.Seconds()
		bps = float64(p.blocks) / elapsed.Seconds()
	}

	log.Printf(
		"[fetcher][perf] loops=%d pages=%d blocks=%d retries=%d busy=%.1f%% pps=%.1f bps=%.1f "+
			"avg_sleep=%s avg_work=%s "+
			"avg_range=%s avg_prod=%s avg_ckpt=%s "+
			"avg_head=%s head_calls=%d "+
			"max_sleep=%s max_work=%s max_range=%s max_prod=%s max_ckpt=%s max_head=%s "+
			"elapsed=%s",
		p.loops, p.pages, p.blocks, p.retryN, busyPct, pps, bps,
		p.avg(p.sleepTotal, p.loops),
		p.avg(p.workTotal, p.loops),
		p.avg(p.rpcTotal-p.headTotal, p.pages),
		p.avg(p.prodTotal, p.blocks),
		p.avg(p.ckptTotal, p.blocks),
		p.avg(p.headTotal, p.headCalls), p.headCalls,
		p.maxSleep, p.maxWork, p.maxRPC, p.maxProd, p.maxCkpt, p.maxHead,
		elapsed,
	)

	*p = perf{
		sampleStart: now,
	}
}

func (f *Fetcher) pollHead(ctx context.Context) <-chan int64 {
	ch := make(chan int64, 1)

	go func() {
		ticker := time.NewTicker(f.cfg.PollHeadEvery)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				t0 := time.Now()
				h, err := f.rpc.ChainHead(ctx)
				d := time.Since(t0)

				// ⚠️ 这里先不碰 perf，避免并发写 f.p 的 race。
				// 你想保留 head RPC 的统计，后面再统一做并发安全 stats。

				if err != nil {
					log.Printf("[fetcher] head poll err: %v", err)
					continue
				}

				// non-blocking: 只保留最新 head
				select {
				case ch <- h.HeadNum:
				default:
					// channel 满：丢掉旧值，塞新值
					select {
					case <-ch:
					default:
					}
					select {
					case ch <- h.HeadNum:
					default:
					}
				}

				_ = d // 暂时不用，保留变量避免你以后加 perf 时再改结构
			}
		}
	}()

	return ch
}

func (f *Fetcher) schedule(ctx context.Context, next, chainHead int64) error {
	headCh := f.pollHead(ctx)
	seq := int64(0)
	for {
		for next > chainHead {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case hn := <-headCh:
				if hn > chainHead {
					chainHead = hn
				}
			}
		}

		to := min(next+int64(f.cfg.PageSize)-1, chainHead)

		select {
		case <-ctx.Done():
			return ctx.Err()
		case f.pgReqCh <- pageReq{seq: seq, from: next, to: to}:
			seq++
		}

		next = to + 1
	}
}

func (f *Fetcher) getPage(ctx context.Context) error {
	P := int64(f.cfg.RPCConcurrency)
	var tSum time.Duration

	for pgReq := range f.pgReqCh {
		seq := pgReq.seq
		next := pgReq.from
		to := pgReq.to
		lane := seq % P
		nextLane := (seq + 1) % P

		pgResp := pageResp{seq: seq}

		err := retry.Do(ctx, retry.Policy{
			MaxAttempts: 5,
			BaseDelay:   100 * time.Millisecond,
			MaxDelay:    5 * time.Second,
			Jitter:      100 * time.Millisecond,
			Classify: func(err error) retry.Class {
				if errors.Is(err, ErrRangeUnacceptable) {
					return retry.Retryable
				}
				// TODO: 你未来可以把某些错误判为 Fatal
				return retry.Retryable
			},
			OnRetry: func(attempt int, wait time.Duration, err error) {
				f.p.retryN++
				log.Printf("[fetcher] range retry: attempt=%d wait=%s err=%v", attempt, wait, err)
			},
		}, func(ctx context.Context) error {
			var err error
			t0 := time.Now()
			pgResp.resp, err = f.rpc.BlocksRange(ctx, next, to)
			tSum += time.Since(t0)
			if err != nil {
				return err
			}
			if len(pgResp.resp.Blocks) == 0 && !pgResp.resp.Partial {
				return ErrRangeUnacceptable
			}
			return nil
		})

		if err != nil {
			return err
		}

		// 只有成功拿到 resp 才进入 lane/token 流程
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-f.pgOutCh[lane]:
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case f.pgRespCh <- pgResp:
		}

		// token 传递也要尊重 ctx，避免 cancel 时卡死
		select {
		case <-ctx.Done():
			return ctx.Err()
		case f.pgOutCh[nextLane] <- struct{}{}:
		}
	}
	return nil
}

func (f *Fetcher) produceLoop(ctx context.Context) error {
	for pgResp := range f.pgRespCh {
		seq := pgResp.seq
		for _, b := range pgResp.resp.Blocks {
			if err := f.prod.ProduceBlockSinglePartition(ctx, b, seq); err != nil {
				log.Printf("[fetcher] enqueue err: page=%d height=%d err=%v",
					seq, b.Header.Number, err)
				return err
			}
		}
	}
	return nil
}

func (f *Fetcher) Run(parent context.Context) error {
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	errCh := make(chan error, 1)
	var wg sync.WaitGroup

	startH, chainHead, err := f.decideStartHeight(ctx)
	if err != nil {
		return err
	}

	f.nextHeadPoll = time.Now()

	log.Printf("[fetcher] start: next_height=%d topic=%s rpc=%s brokers=%s",
		startH, f.cfg.Topic, f.cfg.RPCBaseURL, f.cfg.Brokers)

	f.p.sampleStart = time.Now()

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := f.schedule(ctx, startH, chainHead); err != nil {
			select {
			case errCh <- err:
			default:
			}
		}
	}()

	for i := 0; i < f.cfg.RPCConcurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := f.getPage(ctx); err != nil {
				select {
				case errCh <- err:
				default:
				}
			}
		}()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := f.produceLoop(ctx); err != nil {
			select {
			case errCh <- err:
			default:
			}
		}
	}()

	ckptCh := make(chan Ckpt, 1)

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := ckptLoopPeriodic(ctx, ckptCh, f.ckpt, f.cfg.CheckpointTick); err != nil {
			select {
			case errCh <- err:
			default:
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := f.barrierLoop(ctx, BarrierCfg{
			PageSize:        f.cfg.PageSize,
			AllowedLagPages: 1,
		}, ckptCh); err != nil {
			select {
			case errCh <- err:
			default:
			}
		}
	}()

	select {
	case <-parent.Done():
		err = parent.Err()
	case err = <-errCh:
		// 收到第一个错误，触发全局退出
	}

	cancel()  // 关键：让所有 goroutine 尽快退出
	wg.Wait() // 等大家都收敛

	return err
}

func (f *Fetcher) decideStartHeight(ctx context.Context) (int64, int64, error) {
	head, err := f.rpc.ChainHead(ctx)
	if err != nil {
		return 0, 0, err
	}
	// A) checkpoint wins, but must be validated against canonical: (height, hash)
	if ck, ok, err := f.ckpt.load(); err != nil {
		return 0, 0, err
	} else if ok && ck.LastHeight > 0 {
		if ck.LastHash == "" {
			// strict: checkpoint without hash is treated as invalid
			log.Printf("[fetcher] checkpoint missing hash -> cold start: last=%d", ck.LastHeight)
		} else {
			blk, err := f.rpc.BlockByNumber(ctx, ck.LastHeight)
			if err == nil {
				gotHash := blk.Hash.Hex()
				if equalHex(gotHash, ck.LastHash) {
					next := ck.LastHeight + 1
					log.Printf("[fetcher] resume from checkpoint: last=%d hash=%s next=%d", ck.LastHeight, ck.LastHash, next)
					return next, head.HeadNum, nil
				}
				log.Printf("[fetcher] checkpoint hash mismatch -> cold start: last=%d ckpt_hash=%s got_hash=%s",
					ck.LastHeight, ck.LastHash, gotHash)
			} else {
				// block not found / rpc error -> cold start
				log.Printf("[fetcher] checkpoint height not found or rpc error -> cold start: last=%d hash=%s err=%v",
					ck.LastHeight, ck.LastHash, err)
			}
		}
	}

	// B) no valid checkpoint: use head + backfill if enabled
	if head.Empty || head.HeadNum <= 0 {
		return 1, head.HeadNum, fmt.Errorf("empty chain")
	}

	// backfill disabled -> start at head (only tailing new blocks)
	if f.cfg.BackfillSec < 0 {
		log.Printf("[fetcher] no checkpoint, backfill disabled -> start from head=%d", head.HeadNum)
		return head.HeadNum, head.HeadNum, nil
	}

	targetTs := head.HeadTimestamp - f.cfg.BackfillSec
	if targetTs < 0 {
		targetTs = 0
	}

	pos, err := f.rpc.BlockAtOrAfter(ctx, targetTs)
	if err != nil {
		log.Printf("[fetcher] at-or-after failed -> fallback to 1: err=%v", err)
		return 1, 1, nil
	}

	log.Printf("[fetcher] cold start backfill: head_num=%d head_ts=%d target_ts=%d start_num=%d",
		head.HeadNum, head.HeadTimestamp, targetTs, pos.BlockNum)

	return pos.BlockNum, head.HeadNum, nil
}

func equalHex(a, b string) bool {
	// tolerate "0x" prefix and case differences
	a = strings.TrimSpace(a)
	b = strings.TrimSpace(b)
	a = strings.TrimPrefix(a, "0x")
	b = strings.TrimPrefix(b, "0x")
	return strings.EqualFold(a, b)
}
