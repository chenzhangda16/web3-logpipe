package generator

import (
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"math/rand"

	"github.com/chenzhangda16/web3-logpipe/internal/mockchain/model"
	"github.com/chenzhangda16/web3-logpipe/pkg/rng"
)

type TxGen struct {
	addrs []string

	rFrom *rand.Rand
	rTo   *rand.Rand
	rAmt  *rand.Rand
}

func NewTxGen(addrs []string, rf *rng.Factory) *TxGen {
	return &TxGen{
		addrs: addrs,
		rFrom: rf.R(rng.FromPick),
		rTo:   rf.R(rng.ToPick),
		rAmt:  rf.R(rng.Amount),
	}
}

func (g *TxGen) RandomTx(blockNum, ts int64, txIndex int) model.Tx {
	fromIdx := g.rFrom.Intn(len(g.addrs))
	toIdx := g.rTo.Intn(len(g.addrs))
	for toIdx == fromIdx {
		toIdx = g.rTo.Intn(len(g.addrs))
	}
	amt := 1 + g.rAmt.Int63n(1000)

	return model.Tx{
		Hash:      g.hash(g.addrs[fromIdx], g.addrs[toIdx], blockNum, ts, amt, txIndex),
		From:      g.addrs[fromIdx],
		To:        g.addrs[toIdx],
		Token:     "MOCK",
		Amount:    amt,
		Timestamp: ts,
		BlockNum:  blockNum,
	}
}

func (g *TxGen) SelfLoopTx(blockNum, ts int64, txIndex int) model.Tx {
	a := g.addrs[g.rFrom.Intn(len(g.addrs))]
	amt := 1 + g.rAmt.Int63n(1000)
	return model.Tx{
		Hash:      g.hash(a, a, blockNum, ts, amt, txIndex),
		From:      a,
		To:        a,
		Token:     "MOCK",
		Amount:    amt,
		Timestamp: ts,
		BlockNum:  blockNum,
	}
}

func (g *TxGen) hash(from, to string, bn, ts, amt int64, txIndex int) string {
	h := sha1.Sum([]byte(fmt.Sprintf("%s|%s|%d|%d|%d|%d", from, to, bn, ts, amt, txIndex)))
	return "0x" + hex.EncodeToString(h[:])
}
