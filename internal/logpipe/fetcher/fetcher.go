package fetcher

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/chenzhangda16/web3-logpipe/internal/logpipe/bench"
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
	//Partitions     int // Kafka 分区数（用于 height%P）
	PgRespBuf int
	PgReqBuf  int
	PerfMode  PerfMode
}

type Fetcher struct {
	cfg   Config
	bench *bench.FetchBench
	mode  string // "full" / "no_kafka" / "no_rpc"

	errCh        chan error
	nextHeadPoll time.Time
	pgNCh        chan int
	pgReqCh      chan pageReq
	pgOutCh      []chan struct{}
	pgRespCh     chan pageResp
	rpc          *RPCClient
	prod         *Producer
	ckpt         Checkpoint
	close        func() error
}

type PerfMode string

const (
	BigCache PerfMode = "bench" // 测试背压
	Prod     PerfMode = "prod"  // 生产形态
)

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
	if cfg.RPCConcurrency <= 0 {
		cfg.RPCConcurrency = 1
	}
	if cfg.CheckpointTick <= 0 {
		cfg.CheckpointTick = 1 * time.Second
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

	reqBuf := cfg.PgReqBuf
	respBuf := cfg.PgRespBuf

	// 兼容：如果没显式配置，就用 PerfMode 给默认档位
	if reqBuf <= 0 || respBuf <= 0 {
		switch cfg.PerfMode {
		case "bench":
			if reqBuf <= 0 {
				reqBuf = 4096
			}
			if respBuf <= 0 {
				respBuf = 1024
			}
		default: // "prod"/空
			if reqBuf <= 0 {
				reqBuf = cfg.RPCConcurrency * 4
			}
			if respBuf <= 0 {
				respBuf = cfg.RPCConcurrency * 4
			}
		}
	}

	f := &Fetcher{
		cfg:      cfg,
		rpc:      rpc,
		prod:     prod,
		ckpt:     ckpt,
		pgNCh:    make(chan int, 64),
		pgReqCh:  make(chan pageReq, reqBuf),
		pgRespCh: make(chan pageResp, respBuf),
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
		n := int(to - next + 1)

		// 注意：这里也要 ctx-aware，避免 cancel 卡住
		select {
		case <-ctx.Done():
			return ctx.Err()
		case f.pgNCh <- n:
		}
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

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case pgReq, ok := <-f.pgReqCh:
			if !ok {
				return nil
			}
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
					//f.p.retryN++
					log.Printf("[fetcher] range retry: attempt=%d wait=%s err=%v", attempt, wait, err)
				},
			}, func(ctx context.Context) error {
				var err error
				t0 := time.Now()
				pgResp.resp, err = f.rpc.BlocksRange(ctx, next, to)
				dur := time.Since(t0)
				if f.bench != nil {
					blocksN := 0
					if err == nil {
						blocksN = len(pgResp.resp.Blocks)
					}
					f.bench.ObserveRPC(dur, err, blocksN)
				}
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
	}
}

func (f *Fetcher) produceLoop(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case pgResp, ok := <-f.pgRespCh:
			if !ok {
				return nil
			}
			seq := pgResp.seq
			for _, b := range pgResp.resp.Blocks {
				if err := f.prod.ProduceBlockSinglePartition(ctx, b, seq); err != nil {
					log.Printf("[fetcher] enqueue err: page=%d height=%d err=%v",
						seq, b.Header.Number, err)
					return err
				}
			}
			if f.bench != nil {
				f.bench.AddEnqBlocks(len(pgResp.resp.Blocks))
			}
		}
	}
}

func (f *Fetcher) Run(parent context.Context) error {
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	const DefaultLinkCapacityBytesPS = 294 * 1024 * 1024
	f.bench = bench.NewFetchBench("coldstart24h", 1*time.Second, "eth0", DefaultLinkCapacityBytesPS)
	f.bench.SetQueueSampler(func() bench.QueueSnapshot {
		return bench.QueueSnapshot{
			PgReqLen:  len(f.pgReqCh),
			PgReqCap:  cap(f.pgReqCh),
			PgRespLen: len(f.pgRespCh),
			PgRespCap: cap(f.pgRespCh),
		}
	})
	f.prod.SetInputBlockObserver(500*time.Microsecond, f.bench.AddInputBlocked)
	defer f.bench.Stop()
	log.Printf("[fetcher] perf mode=%s", f.mode)

	errCh := make(chan error, 1)
	var wg sync.WaitGroup

	startH, chainHead, err := f.decideStartHeight(ctx)
	if err != nil {
		return err
	}

	f.nextHeadPoll = time.Now()

	log.Printf("[fetcher] start: next_height=%d topic=%s rpc=%s brokers=%s",
		startH, f.cfg.Topic, f.cfg.RPCBaseURL, f.cfg.Brokers)

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
		if err := ckptLoopPeriodic(ctx, ckptCh, f.ckpt, f.cfg.CheckpointTick, f.bench); err != nil {
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
