package miner

import (
	"context"
	"log"
	"time"

	"github.com/chenzhangda16/web3-logpipe/internal/mockchain/generator"
	"github.com/chenzhangda16/web3-logpipe/internal/mockchain/model"
	"github.com/chenzhangda16/web3-logpipe/internal/mockchain/store"
	"github.com/chenzhangda16/web3-logpipe/pkg/hash"
	"github.com/chenzhangda16/web3-logpipe/pkg/rng"
)

const (
	TxCount    = "tx_count"
	Choose     = "choose_loop_vs_rand"
	BlockNonce = "block_nonce"
)

type Miner struct {
	store *store.RocksStore
	txgen *generator.TxGen
	rf    *rng.Factory
	tick  time.Duration
}

func NewMiner(st *store.RocksStore, txgen *generator.TxGen, rf *rng.Factory, tick time.Duration) *Miner {
	return &Miner{
		store: st,
		txgen: txgen,
		rf:    rf,
		tick:  tick,
	}
}

func (m *Miner) Warmup(backfillSec int64) error {
	start := time.Now()
	gapSec := m.store.GapRuleSec()
	if backfillSec <= 0 {
		log.Printf("[warmup] skip: backfillSec=%d gapSec=%d tick=%s", backfillSec, m.store.GapRuleSec(), m.tick)
		return nil
	}

	step := int64(m.tick / time.Second)
	if step <= 0 {
		step = 1
	}

	log.Printf("[warmup] begin: backfillSec=%d gapSec=%d tick=%s step=%ds", backfillSec, gapSec, m.tick, step)

	// 1) 决策：REBUILD / TRIM / KEEP_ALL
	curTs := time.Now().Unix()
	action, keep, err := m.store.DecideTailAction(curTs, backfillSec)
	if err != nil {
		log.Printf("[warmup] decide_tail_action failed: curTs=%d backfillSec=%d gapSec=%d err=%v", curTs, backfillSec, gapSec, err)
		return err
	}
	log.Printf("[warmup] tail_action: action=%s keepHeight=%d curTs=%d targetTs=%d",
		action.String(), keep, curTs, curTs-backfillSec)

	switch action {
	case store.TailRebuild:
		log.Printf("[warmup] tail_action=REBUILD: delete canonical after 0")
		if err := m.store.DeleteCanonicalAfter(0); err != nil {
			log.Printf("[warmup] delete_canonical_after failed: keepHeight=0 err=%v", err)
			return err
		}

	case store.TailTrimAfterKeep:
		log.Printf("[warmup] tail_action=TRIM_AFTER_KEEP: delete canonical after keep=%d", keep)
		if err := m.store.DeleteCanonicalAfter(keep); err != nil {
			log.Printf("[warmup] delete_canonical_after failed: keepHeight=%d err=%v", keep, err)
			return err
		}

	case store.TailKeepAllCatchUp:
		log.Printf("[warmup] tail_action=KEEP_ALL_CATCH_UP: no deletion")
	default:
		// 容错：当作 rebuild
		log.Printf("[warmup] tail_action=UNKNOWN: treat as REBUILD, delete canonical after 0")
		if err := m.store.DeleteCanonicalAfter(0); err != nil {
			log.Printf("[warmup] delete_canonical_after failed: keepHeight=0 err=%v", err)
			return err
		}
	}

	// 2) 重新加载 head（因为可能删过）
	parentHash, nextNum, lastTs, hasHead, err := m.loadHead()
	if err != nil {
		log.Printf("[warmup] load_head failed: err=%v", err)
		return err
	}
	log.Printf("[warmup] head_loaded: hasHead=%v nextNum=%d lastTs=%d parent=%s", hasHead, nextNum, lastTs, parentHash.Hex())

	// 3) decide warmup start timestamp
	now := time.Now().Unix()
	minTs := now - backfillSec
	if minTs < 0 {
		minTs = 0
	}

	var ts int64
	if !hasHead {
		// empty or rebuilt: always start from backfill window
		ts = minTs
		log.Printf("[warmup] start_from_empty: ts=%d (now-backfillSec)", ts)
	} else {
		// KEEP_ALL_CATCH_UP or after trim: don't go earlier than backfill window
		ts = lastTs + step
		if ts < minTs {
			ts = minTs
		}
		log.Printf("[warmup] start_from_last: ts=%d (max(lastTs+step, now-backfillSec))", ts)
	}

	// 4) 动态追时间墙：只要 ts+step < dynamicNow，就继续“加速挖”
	var mined int64
	lastLog := time.Now()
	for {
		dynamicNow := time.Now().Unix()
		if ts+step >= dynamicNow {
			break
		}

		if err := m.mineOne(nextNum, &parentHash, ts); err != nil {
			log.Printf("[warmup] mine_one failed: bn=%d ts=%d err=%v (mined=%d cost=%s)",
				nextNum, ts, err, mined, time.Since(start))
			return err
		}
		mined++
		nextNum++
		ts += step

		// 节流：最多每 1s 打一条进度（避免 backfill 很大时刷屏）
		if time.Since(lastLog) >= 1*time.Second {
			lag := dynamicNow - ts
			log.Printf("[warmup] progress: mined=%d nextNum=%d ts=%d dynamicNow=%d lag=%ds cost=%s",
				mined, nextNum, ts, dynamicNow, lag, time.Since(start))
			lastLog = time.Now()
		}
	}

	log.Printf("[warmup] done: mined=%d nextNum=%d endTs=%d cost=%s", mined, nextNum, ts, time.Since(start))
	return nil
}

func (m *Miner) mineOne(bn int64, parentHash *hash.Hash32, ts int64) error {
	nTx := 50 + m.rf.R(TxCount).Intn(50)
	txs := make([]model.Tx, 0, nTx)
	for i := 0; i < nTx; i++ {
		p := m.rf.R(Choose).Float64()
		if p < 0.1 {
			txs = append(txs, m.txgen.SelfLoopTx(bn, ts))
		} else {
			txs = append(txs, m.txgen.RandomTx(bn, ts))
		}
	}
	nonce := m.rf.R(BlockNonce).Uint64()

	blk := model.BuildBlock(bn, *parentHash, txs, ts, nonce)
	raw, err := model.EncodeBlock(blk)
	if err != nil {
		return err
	}
	if err := m.store.AppendCanonicalBlock(blk, raw); err != nil {
		return err
	}
	*parentHash = blk.Hash
	return nil
}

func (m *Miner) Run(ctx context.Context) error {
	parentHash, nextNum, lastTs, _, err := m.loadHead()
	if err != nil {
		return err
	}

	ticker := time.NewTicker(m.tick)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case now := <-ticker.C:
			bn := nextNum
			nextNum++

			ts := now.Unix()
			if ts <= lastTs {
				ts = lastTs + 1 // 强制单调递增，彻底消灭撞车
			}
			if err := m.mineOne(bn, &parentHash, ts); err != nil {
				return err
			}
			lastTs = ts
		}
	}
}

func (m *Miner) loadHead() (parent hash.Hash32, nextNum int64, lastTs int64, ok bool, err error) {
	if h, ok2, err2 := m.store.HeadHash(); err2 != nil {
		return hash.Hash32{}, 0, 0, false, err2
	} else if ok2 {
		raw, err := m.store.GetBlockByHashRaw(h)
		if err != nil {
			return hash.Hash32{}, 0, 0, false, err
		}
		blk, err := model.DecodeBlock(raw)
		if err != nil {
			return hash.Hash32{}, 0, 0, false, err
		}
		return blk.Hash, blk.Header.Number + 1, blk.Header.Timestamp, true, nil
	}
	return hash.Hash32{}, 1, 0, false, nil
}
