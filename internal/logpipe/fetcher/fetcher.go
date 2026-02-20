package fetcher

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/chenzhangda16/web3-logpipe/internal/logpipe/retry"
)

type Config struct {
	RPCBaseURL string

	Brokers string // comma-separated
	Topic   string

	BackfillSec int64 // -1 disables

	PageSize int

	PollHeadEvery time.Duration
	IdleSleep     time.Duration

	CheckpointPath string
}

type Fetcher struct {
	cfg Config

	rpc   *RPCClient
	prod  *Producer
	ckpt  Checkpoint
	close func() error
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
	if cfg.IdleSleep <= 0 {
		cfg.IdleSleep = 300 * time.Millisecond
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
		cfg:  cfg,
		rpc:  rpc,
		prod: prod,
		ckpt: ckpt,
	}
	f.close = func() error {
		_ = prod.Close()
		return nil
	}
	return f, nil
}

func (f *Fetcher) Close() error { return f.close() }

func (f *Fetcher) Run(ctx context.Context) error {
	// 1) decide start height
	start, err := f.decideStartHeight(ctx)
	if err != nil {
		return err
	}
	next := start

	// 2) main loop
	var headNum int64 = 0
	nextHeadPoll := time.Now()

	log.Printf("[fetcher] start: next_height=%d topic=%s rpc=%s brokers=%s",
		next, f.cfg.Topic, f.cfg.RPCBaseURL, f.cfg.Brokers)

	// ---- perf sampler (1s) ----
	type perf struct {
		loops  int64
		pages  int64
		blocks int64
		retryN int64

		// "wait" for fetcher：只算显式 Sleep / 阻塞
		sleepTotal time.Duration
		maxSleep   time.Duration

		// "work"：range + produce + ckpt 整体 wall-clock
		workTotal time.Duration
		maxWork   time.Duration

		// breakdown
		rpcTotal  time.Duration // BlocksRange + ChainHead
		maxRPC    time.Duration // max of BlocksRange
		headTotal time.Duration
		maxHead   time.Duration
		headCalls int64

		prodTotal time.Duration
		maxProd   time.Duration

		ckptTotal time.Duration
		maxCkpt   time.Duration
	}

	var p perf
	sampleStart := time.Now()

	sleep := func(d time.Duration) {
		if d <= 0 {
			return
		}
		time.Sleep(d)
		p.sleepTotal += d
		if d > p.maxSleep {
			p.maxSleep = d
		}
	}

	avg := func(d time.Duration, n int64) time.Duration {
		if n <= 0 {
			return 0
		}
		return time.Duration(int64(d) / n)
	}

	flush := func(now time.Time) {
		elapsed := now.Sub(sampleStart)
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
			avg(p.sleepTotal, p.loops), avg(p.workTotal, p.loops),
			avg(p.rpcTotal-p.headTotal, p.pages), // 仅 range 的均值（rpcTotal 里含 head，所以减掉 headTotal）
			avg(p.prodTotal, p.blocks),
			avg(p.ckptTotal, p.blocks),
			avg(p.headTotal, p.headCalls), p.headCalls,
			p.maxSleep, p.maxWork, p.maxRPC, p.maxProd, p.maxCkpt, p.maxHead,
			elapsed,
		)

		p = perf{}
		sampleStart = now
	}
	// ---- end perf sampler ----

	for {
		p.loops++

		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// refresh head periodically
		if time.Now().After(nextHeadPoll) {
			t0 := time.Now()
			h, err := f.rpc.ChainHead(ctx)
			d := time.Since(t0)

			p.rpcTotal += d
			p.headTotal += d
			p.headCalls++
			if d > p.maxHead {
				p.maxHead = d
			}

			if err != nil {
				log.Printf("[fetcher] head poll err: %v", err)
			} else {
				headNum = h.HeadNum
			}
			nextHeadPoll = time.Now().Add(f.cfg.PollHeadEvery)
		}

		// if head unknown, try fetch once
		if headNum == 0 {
			t0 := time.Now()
			h, err := f.rpc.ChainHead(ctx)
			d := time.Since(t0)

			p.rpcTotal += d
			p.headTotal += d
			p.headCalls++
			if d > p.maxHead {
				p.maxHead = d
			}

			if err != nil {
				log.Printf("[fetcher] head err: %v", err)
				sleep(f.cfg.IdleSleep)
				if time.Since(sampleStart) >= time.Second {
					flush(time.Now())
				}
				continue
			}
			headNum = h.HeadNum
		}

		if next > headNum {
			sleep(f.cfg.IdleSleep)
			if time.Since(sampleStart) >= time.Second {
				flush(time.Now())
			}
			continue
		}

		// ---- work starts (range + produce + ckpt) ----
		workStart := time.Now()

		to := next + int64(f.cfg.PageSize) - 1
		if to > headNum {
			to = headNum
		}

		// RPC: BlocksRange timing
		tRPC0 := time.Now()
		rangeResp, err := f.rpc.BlocksRange(ctx, next, to)
		tRPC := time.Since(tRPC0)

		p.rpcTotal += tRPC
		p.pages++
		if tRPC > p.maxRPC {
			p.maxRPC = tRPC
		}

		if err != nil {
			log.Printf("[fetcher] range err: from=%d to=%d err=%v", next, to, err)
			sleep(500 * time.Millisecond)

			// work end
			workDur := time.Since(workStart)
			p.workTotal += workDur
			if workDur > p.maxWork {
				p.maxWork = workDur
			}

			if time.Since(sampleStart) >= time.Second {
				flush(time.Now())
			}
			continue
		}

		blocks := rangeResp.Blocks
		if len(blocks) == 0 {
			if rangeResp.Partial {
				log.Printf("[fetcher] range partial but empty: from=%d to=%d last_ok=%d", next, to, rangeResp.LastOK)
			}
			sleep(f.cfg.IdleSleep)

			// work end
			workDur := time.Since(workStart)
			p.workTotal += workDur
			if workDur > p.maxWork {
				p.maxWork = workDur
			}

			if time.Since(sampleStart) >= time.Second {
				flush(time.Now())
			}
			continue
		}

		producedAny := false
		for _, b := range blocks {
			if b.Header.Number < next {
				continue
			}
			if b.Header.Number > next {
				log.Printf("[fetcher] gap in server response: expected=%d got=%d (from=%d to=%d partial=%v last_ok=%d)",
					next, b.Header.Number, rangeResp.From, rangeResp.To, rangeResp.Partial, rangeResp.LastOK)
				break
			}

			// Produce timing (includes retry wrapper)
			tProd0 := time.Now()
			err := retry.Do(ctx, retry.Policy{
				MaxAttempts: 5,
				BaseDelay:   100 * time.Millisecond,
				MaxDelay:    5 * time.Second,
				Jitter:      100 * time.Millisecond,
				OnRetry: func(attempt int, wait time.Duration, err error) {
					p.retryN++
					log.Printf("[fetcher] produce retry: attempt=%d wait=%s err=%v", attempt, wait, err)
				},
			}, func(ctx context.Context) error {
				return f.prod.ProduceBlock(ctx, b)
			})
			tProd := time.Since(tProd0)

			p.prodTotal += tProd
			if tProd > p.maxProd {
				p.maxProd = tProd
			}

			if err != nil {
				log.Printf("[fetcher] produce err: height=%d err=%v", b.Header.Number, err)
				sleep(300 * time.Millisecond)
				break
			}

			// Checkpoint timing
			tC0 := time.Now()
			if err := f.ckpt.Save(Ckpt{
				LastHeight: b.Header.Number,
				LastHash:   b.Hash.Hex(),
			}); err != nil {
				log.Printf("[fetcher] checkpoint save err: %v", err)
			}
			tC := time.Since(tC0)

			p.ckptTotal += tC
			if tC > p.maxCkpt {
				p.maxCkpt = tC
			}

			p.blocks++
			producedAny = true
			next = b.Header.Number + 1
		}

		if rangeResp.Partial {
			if !producedAny {
				if rangeResp.LastOK >= next {
					log.Printf("[fetcher] partial advance by last_ok: next=%d last_ok=%d", next, rangeResp.LastOK)
					next = rangeResp.LastOK + 1
				} else {
					sleep(200 * time.Millisecond)
				}
			}
		}

		// work end
		workDur := time.Since(workStart)
		p.workTotal += workDur
		if workDur > p.maxWork {
			p.maxWork = workDur
		}

		if time.Since(sampleStart) >= time.Second {
			flush(time.Now())
		}
	}
}

func (f *Fetcher) decideStartHeight(ctx context.Context) (int64, error) {
	// A) checkpoint wins, but must be validated against canonical: (height, hash)
	if ck, ok, err := f.ckpt.Load(); err != nil {
		return 0, err
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
					return next, nil
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
	head, err := f.rpc.ChainHead(ctx)
	if err != nil {
		return 0, err
	}
	if head.Empty || head.HeadNum <= 0 {
		return 1, fmt.Errorf("empty chain")
	}

	// backfill disabled -> start at head (only tailing new blocks)
	if f.cfg.BackfillSec < 0 {
		log.Printf("[fetcher] no checkpoint, backfill disabled -> start from head=%d", head.HeadNum)
		return head.HeadNum, nil
	}

	targetTs := head.HeadTimestamp - f.cfg.BackfillSec
	if targetTs < 0 {
		targetTs = 0
	}

	pos, err := f.rpc.BlockAtOrAfter(ctx, targetTs)
	if err != nil {
		log.Printf("[fetcher] at-or-after failed -> fallback to 1: err=%v", err)
		return 1, nil
	}

	log.Printf("[fetcher] cold start backfill: head_num=%d head_ts=%d target_ts=%d start_num=%d",
		head.HeadNum, head.HeadTimestamp, targetTs, pos.BlockNum)

	return pos.BlockNum, nil
}

func equalHex(a, b string) bool {
	// tolerate "0x" prefix and case differences
	a = strings.TrimSpace(a)
	b = strings.TrimSpace(b)
	a = strings.TrimPrefix(a, "0x")
	b = strings.TrimPrefix(b, "0x")
	return strings.EqualFold(a, b)
}
