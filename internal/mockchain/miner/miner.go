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

func (m *Miner) Run(ctx context.Context) error {
	var (
		parentHash hash.Hash32
		nextNum    int64 = 1
	)

	// 1) init from store head
	if h, ok, err := m.store.HeadHash(); err != nil {
		return err
	} else if ok {
		raw, err := m.store.GetBlockByHashRaw(h)
		if err != nil {
			return err
		}
		blk, err := model.DecodeBlock(raw)
		if err != nil {
			return err
		}
		parentHash = blk.Hash
		nextNum = blk.Header.Number + 1
	} else {
		// empty DB: genesis parent = zero hash, start at 1
		parentHash = hash.Hash32{}
		nextNum = 1
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

			blk := model.BuildBlock(
				bn,
				parentHash,
				txs,
				ts,
				nonce,
			)

			raw, err := model.EncodeBlock(blk)
			if err != nil {
				return err
			}
			if err := m.store.AppendCanonicalBlock(blk, raw); err != nil {
				return err
			}

			// advance
			parentHash = blk.Hash
		}
	}
}
