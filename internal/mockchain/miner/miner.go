package miner

import (
	"context"
	"time"

	"github.com/chenzhangda16/web3-logpipe/internal/mockchain/generator"
	"github.com/chenzhangda16/web3-logpipe/internal/mockchain/model"
	"github.com/chenzhangda16/web3-logpipe/internal/mockchain/store"
	"github.com/chenzhangda16/web3-logpipe/pkg/rng"
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
	head, err := m.store.Head()
	if err != nil {
		return err
	}
	next := head + 1

	ticker := time.NewTicker(m.tick)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case now := <-ticker.C:
			bn := next
			next++

			ts := now.Unix()
			nTx := 50 + m.rf.R(rng.TxCount).Intn(50)
			txs := make([]model.Tx, 0, nTx)
			for i := 0; i < nTx; i++ {
				p := m.rf.R(rng.Choose).Float64()
				if p < 0.1 {
					txs = append(txs, m.txgen.SelfLoopTx(bn, ts, i))
				} else {
					txs = append(txs, m.txgen.RandomTx(bn, ts, i))
				}
			}

			blk := model.Block{Number: bn, Timestamp: ts, Txs: txs}
			raw, err := model.EncodeBlock(blk)
			if err != nil {
				return err
			}
			if err := m.store.AppendBlock(bn, raw); err != nil {
				return err
			}
		}
	}
}
