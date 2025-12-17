package generator

import (
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"math/rand"

	"github.com/chenzhangda16/web3-logpipe/internal/mockchain/model"
)

type TxGen struct {
	addrs []string
	rng   *rand.Rand
}

func NewTxGen(addrs []string, seed int64) *TxGen {
	return &TxGen{addrs: addrs, rng: rand.New(rand.NewSource(seed))}
}

func (g *TxGen) RandomTx(blockNum, ts int64) model.Tx {
	from := g.addrs[g.rng.Intn(len(g.addrs))]
	to := g.addrs[g.rng.Intn(len(g.addrs))]
	amt := 1 + g.rng.Int63n(1000)

	return model.Tx{
		Hash:      g.hash(from, to, blockNum, ts, amt),
		From:      from,
		To:        to,
		Token:     "MOCK",
		Amount:    amt,
		Timestamp: ts,
		BlockNum:  blockNum,
	}
}

func (g *TxGen) SelfLoopTx(blockNum, ts int64) model.Tx {
	a := g.addrs[g.rng.Intn(len(g.addrs))]
	amt := 1 + g.rng.Int63n(1000)
	return model.Tx{
		Hash:      g.hash(a, a, blockNum, ts, amt),
		From:      a,
		To:        a,
		Token:     "MOCK",
		Amount:    amt,
		Timestamp: ts,
		BlockNum:  blockNum,
	}
}

func (g *TxGen) hash(from, to string, bn, ts, amt int64) string {
	h := sha1.Sum([]byte(fmt.Sprintf("%s|%s|%d|%d|%d|%d", from, to, bn, ts, amt, g.rng.Int63())))
	return "0x" + hex.EncodeToString(h[:])
}
