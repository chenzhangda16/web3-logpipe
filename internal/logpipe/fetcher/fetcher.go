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
			h, err := f.rpc.ChainHead(ctx)
			if err != nil {
				log.Printf("[fetcher] head poll err: %v", err)
			} else {
				headNum = h.HeadNum
			}
			nextHeadPoll = time.Now().Add(f.cfg.PollHeadEvery)
		}

		// if head unknown, try fetch once
		if headNum == 0 {
			h, err := f.rpc.ChainHead(ctx)
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

		rangeResp, err := f.rpc.BlocksRange(ctx, next, to)
		if err != nil {
			log.Printf("[fetcher] range err: from=%d to=%d err=%v", next, to, err)
			time.Sleep(500 * time.Millisecond)
			continue
		}

		blocks := rangeResp.Blocks
		if len(blocks) == 0 {
			// If server says partial but returns nothing, don't advance.
			// Backoff and retry; also refresh head soon.
			if rangeResp.Partial {
				log.Printf("[fetcher] range partial but empty: from=%d to=%d last_ok=%d", next, to, rangeResp.LastOK)
			}
			time.Sleep(f.cfg.IdleSleep)
			continue
		}

		// Produce blocks sequentially (keeps deterministic order)
		lastProduced := int64(0)
		for _, b := range blocks {
			if b.Header.Number < next {
				continue
			}
			// If there is a gap in returned blocks, stop and retry from 'next'.
			// This avoids silently skipping missing heights.
			if b.Header.Number > next {
				log.Printf("[fetcher] gap in server response: expected=%d got=%d (from=%d to=%d partial=%v last_ok=%d)",
					next, b.Header.Number, rangeResp.From, rangeResp.To, rangeResp.Partial, rangeResp.LastOK)
				break
			}

			if err := f.prod.ProduceBlock(ctx, b); err != nil {
				log.Printf("[fetcher] produce err: height=%d err=%v", b.Header.Number, err)
				time.Sleep(300 * time.Millisecond)
				break
			}

			// checkpoint after each successful produce
			if err := f.ckpt.Save(Ckpt{
				LastHeight: b.Header.Number,
				LastHash:   b.Hash.Hex(),
			}); err != nil {
				log.Printf("[fetcher] checkpoint save err: %v", err)
			}

			lastProduced = b.Header.Number
			next = b.Header.Number + 1
		}

		// If server marked partial, we should be conservative:
		// - If we produced up to lastProduced, continue from next (already advanced).
		// - If we produced nothing, but server has last_ok >= next-1, we can advance to last_ok+1.
		if rangeResp.Partial {
			// produced nothing (e.g., decode ok but gap/produce error happened before first block)
			if lastProduced == 0 {
				if rangeResp.LastOK >= next {
					// NOTE: next here is still the original 'next' because we didn't advance.
					// To be safe, only advance if last_ok is at/after expected next.
					log.Printf("[fetcher] partial advance by last_ok: next=%d last_ok=%d", next, rangeResp.LastOK)
					next = rangeResp.LastOK + 1
				} else {
					// can't advance, retry
					time.Sleep(200 * time.Millisecond)
				}
			}
			// if produced some blocks, next already advanced; just continue
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
