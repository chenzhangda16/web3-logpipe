package fetcher

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"
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

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// refresh head periodically
		if time.Now().After(nextHeadPoll) {
			h, err := f.rpc.Head(ctx)
			if err != nil {
				log.Printf("[fetcher] head poll err: %v", err)
			} else {
				headNum = h.HeadNum
			}
			nextHeadPoll = time.Now().Add(f.cfg.PollHeadEvery)
		}

		// if head unknown, try fetch once
		if headNum == 0 {
			h, err := f.rpc.Head(ctx)
			if err != nil {
				log.Printf("[fetcher] head err: %v", err)
				time.Sleep(f.cfg.IdleSleep)
				continue
			}
			headNum = h.HeadNum
		}

		if next > headNum {
			time.Sleep(f.cfg.IdleSleep)
			continue
		}

		to := next + int64(f.cfg.PageSize) - 1
		if to > headNum {
			to = headNum
		}

		blocks, err := f.rpc.BlocksRange(ctx, next, to)
		if err != nil {
			log.Printf("[fetcher] range err: from=%d to=%d err=%v", next, to, err)
			time.Sleep(500 * time.Millisecond)
			continue
		}
		if len(blocks) == 0 {
			// nothing returned; avoid tight loop
			time.Sleep(f.cfg.IdleSleep)
			continue
		}

		// produce blocks sequentially (keeps deterministic order)
		for _, b := range blocks {
			// safety: if server returned earlier blocks, skip
			if b.Header.Number < next {
				continue
			}
			// if server jumps ahead, we still advance by returned number
			if err := f.prod.ProduceBlock(ctx, b); err != nil {
				log.Printf("[fetcher] produce err: height=%d err=%v", b.Header.Number, err)
				// backoff and retry same block
				time.Sleep(300 * time.Millisecond)
				break
			}
			// checkpoint: last successfully produced block height
			if err := f.ckpt.Save(Ckpt{
				LastHeight: b.Header.Number,
				LastHash:   b.Hash.Hex(),
			}); err != nil {
				log.Printf("[fetcher] checkpoint save err: %v", err)
			}
			next = b.Header.Number + 1
		}
	}
}

func (f *Fetcher) decideStartHeight(ctx context.Context) (int64, error) {
	// A) checkpoint wins, but must be validated against chain canonical.
	if ck, ok, err := f.ckpt.Load(); err != nil {
		return 0, err
	} else if ok && ck.LastHeight > 0 {
		blk, err := f.rpc.BlockByNumber(ctx, ck.LastHeight)
		if err == nil {
			// If we have hash in checkpoint, validate it.
			if ck.LastHash != "" {
				gotHash := blk.Hash.Hex()
				if !equalHex(gotHash, ck.LastHash) {
					// hash mismatch => chain rewritten/reorg/rebuild/trim
					// degrade to cold start backfill
					// (do not return error; self-heal)
				} else {
					// valid resume
					next := ck.LastHeight + 1
					log.Printf("[fetcher] resume from checkpoint: last=%d hash=%s next=%d", ck.LastHeight, ck.LastHash, next)
					return next, nil
				}
			} else {
				// old checkpoint format (height only) => accept by height existence
				next := ck.LastHeight + 1
				log.Printf("[fetcher] resume from checkpoint(height-only): last=%d next=%d", ck.LastHeight, next)
				return next, nil
			}
		} else {
			// If block not found (likely 404), checkpoint invalid => cold start backfill.
			// For other errors (RPC down), we can retry by falling through to ChainHead (also RPC).
		}

		log.Printf("[fetcher] checkpoint invalid or mismatched -> cold start: last=%d hash=%s", ck.LastHeight, ck.LastHash)
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
		// fallback: start at 1
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
