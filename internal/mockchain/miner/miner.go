package miner

import (
	"context"
	"time"

	"github.com/chenzhangda16/web3-logpipe/internal/mockchain/generator"
	"github.com/chenzhangda16/web3-logpipe/internal/mockchain/hash"
	"github.com/chenzhangda16/web3-logpipe/internal/mockchain/model"
	"github.com/chenzhangda16/web3-logpipe/internal/mockchain/store"
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

func (m *Miner) Warmup(backfillSec int64, gapSec int64) error {
	if backfillSec <= 0 {
		return nil
	}
	step := int64(m.tick / time.Second)
	if step <= 0 {
		step = 1
	}
	if gapSec <= 0 {
		// 连续阈值建议绑 tick，别用很大的秒数
		gapSec = 3 * step
	}

	// 1) 决策：REBUILD / TRIM / KEEP_ALL
	curTs := time.Now().Unix()
	action, keep, err := m.store.DecideTailAction(curTs, backfillSec, gapSec)
	if err != nil {
		return err
	}

	switch action {
	case store.TailRebuild:
		// 清空 canonical（以及对应 raw），然后从 (now - backfillSec) 开始造
		if err := m.store.DeleteCanonicalAfter(0); err != nil {
			return err
		}

	case store.TailTrimAfterKeep:
		// 保留到 keepHeight，抹掉后面的零散块
		if err := m.store.DeleteCanonicalAfter(keep); err != nil {
			return err
		}

	case store.TailKeepAllCatchUp:
		// 不删
	default:
		// 容错：当作 rebuild
		if err := m.store.DeleteCanonicalAfter(0); err != nil {
			return err
		}
	}

	// 2) 重新加载 head（因为可能删过）
	parentHash, nextNum, lastTs, hasHead, err := m.loadHead()
	if err != nil {
		return err
	}

	// 3) 决定 warmup 起点时间 ts
	// - 如果是空库（或刚 rebuild），从 now-backfillSec 开始造
	// - 否则从 lastTs+step 开始补洞
	ts := int64(0)
	if !hasHead {
		// 注意：起点基于“当前时刻”，而终点用动态墙
		ts = time.Now().Unix() - backfillSec
		if ts < 0 {
			ts = 0
		}
	} else {
		ts = lastTs + step
	}

	// 4) 动态追时间墙：只要 ts+step < dynamicNow，就继续“加速挖”
	for {
		dynamicNow := time.Now().Unix()
		if ts+step >= dynamicNow {
			break
		}
		if err := m.mineOne(nextNum, &parentHash, ts); err != nil {
			return err
		}
		nextNum++
		ts += step
	}

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
