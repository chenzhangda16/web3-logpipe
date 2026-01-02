package generator

import (
	"math/rand"

	"github.com/chenzhangda16/web3-logpipe/internal/mockchain/model"
	"github.com/chenzhangda16/web3-logpipe/pkg/rng"
)

const (
	FromPick = "from_pick"
	ToPick   = "to_pick"
	Amount   = "amount"
	Nonce    = "nonce"
)

type TxGen struct {
	addrs []string

	rFrom  *rand.Rand
	rTo    *rand.Rand
	rAmt   *rand.Rand
	rNonce *rand.Rand
}

func NewTxGen(addrs []string, rf *rng.Factory) *TxGen {
	return &TxGen{
		addrs:  addrs,
		rFrom:  rf.R(FromPick),
		rTo:    rf.R(ToPick),
		rAmt:   rf.R(Amount),
		rNonce: rf.R(Nonce),
	}
}

func (g *TxGen) RandomTx(blockNum, ts int64) model.Tx {
	fromIdx := g.rFrom.Intn(len(g.addrs))
	toIdx := g.rTo.Intn(len(g.addrs))
	for toIdx == fromIdx {
		toIdx = g.rTo.Intn(len(g.addrs))
	}
	amt := 1 + g.rAmt.Int63n(1000)
	nonce := uint64(g.rNonce.Int63())

	return model.BuildTx(
		model.TxBody{
			From:      g.addrs[fromIdx],
			To:        g.addrs[toIdx],
			Token:     "MOCK",
			Amount:    amt,
			Timestamp: ts,
			Nonce:     nonce,
		},
		blockNum,
	)
}

func (g *TxGen) SelfLoopTx(blockNum, ts int64) model.Tx {
	a := g.addrs[g.rFrom.Intn(len(g.addrs))]
	amt := 1 + g.rAmt.Int63n(1000)
	nonce := uint64(g.rNonce.Int63())

	return model.BuildTx(
		model.TxBody{
			From:      a,
			To:        a,
			Token:     "MOCK",
			Amount:    amt,
			Timestamp: ts,
			Nonce:     nonce,
		},
		blockNum,
	)
}
