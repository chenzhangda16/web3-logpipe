package miner

import (
	"context"
	"time"

	"github.com/chenzhangda16/web3-logpipe/internal/mockchain/generator"
	"github.com/chenzhangda16/web3-logpipe/internal/mockchain/model"
	"github.com/chenzhangda16/web3-logpipe/internal/mockchain/store"
)

type Miner struct {
	store   *store.RocksStore
	txgen   *generator.TxGen
	tick    time.Duration
	startAt int64 // 可选：从 head+1 开始
}

func NewMiner(st *store.RocksStore, txgen *generator.TxGen, tick time.Duration) *Miner {
	return &Miner{store: st, txgen: txgen, tick: tick}
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
			nTx := 50 + m.txgenRngIntn(50) // 小技巧：下面实现避免暴露 rng
			txs := make([]model.Tx, 0, nTx)
			for i := 0; i < nTx; i++ {
				p := m.txgenRngFloat64()
				if p < 0.1 {
					txs = append(txs, m.txgen.SelfLoopTx(bn, ts))
				} else {
					txs = append(txs, m.txgen.RandomTx(bn, ts))
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

// 下面两个方法是为了不让 Miner 直接碰 txgen.rng（你也可以直接把 rng 暴露出来，骨架阶段无所谓）
func (m *Miner) txgenRngIntn(n int) int {
	// 复用 txgen 的 RandomTx 会消耗 rng，不好；这里先简单点：用时间扰动不是你想要的。
	// 更推荐：把 TxGen 里 rng 提供方法 Intn/Float64 或在 Miner 里单独放一个 rng。
	return int(time.Now().UnixNano() % int64(n))
}
func (m *Miner) txgenRngFloat64() float64 {
	return float64(time.Now().UnixNano()%1000) / 1000.0
}
