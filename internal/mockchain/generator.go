package mockchain

import (
	"math/rand"
	"strconv"

	"github.com/chenzhangda16/web3-logpipe/internal/logpipe/model"
)

func (mc *MockChain) makeRandomTx(blockNum, ts int64) model.Tx {
	fromIdx := rand.Intn(len(mc.addrs))
	toIdx := rand.Intn(len(mc.addrs))
	for toIdx == fromIdx {
		toIdx = rand.Intn(len(mc.addrs))
	}
	token := "TOKEN-" + strconv.Itoa(rand.Intn(5)) // 5 种 Token
	amount := int64(1+rand.Intn(1000)) * 1e6       // 随便搞个金额

	return model.Tx{
		Hash:      randomHash(),
		From:      mc.addrs[fromIdx],
		To:        mc.addrs[toIdx],
		Token:     token,
		Amount:    amount,
		Timestamp: ts,
		BlockNum:  blockNum,
	}
}

func (mc *MockChain) makeLoopTx(blockNum, ts int64) []model.Tx {
	// 随机从 2 环或者 3 环组中选一组
	if len(mc.loopPairs) == 0 && len(mc.loopTriples) == 0 {
		return []model.Tx{mc.makeRandomTx(blockNum, ts)} // fallback
	}

	if len(mc.loopPairs) > 0 && (len(mc.loopTriples) == 0 || rand.Float64() < 0.6) {
		// 用 2 环 A<->B
		pair := mc.loopPairs[rand.Intn(len(mc.loopPairs))]
		a, b := mc.addrs[pair[0]], mc.addrs[pair[1]]
		token := "TOKEN-LOOP"
		amount := int64(10+rand.Intn(100)) * 1e8

		return []model.Tx{
			{
				Hash:      randomHash(),
				From:      a,
				To:        b,
				Token:     token,
				Amount:    amount,
				Timestamp: ts,
				BlockNum:  blockNum,
			},
			{
				Hash:      randomHash(),
				From:      b,
				To:        a,
				Token:     token,
				Amount:    amount,
				Timestamp: ts,
				BlockNum:  blockNum,
			},
		}
	}

	// 否则用 3 环 A->B->C->A
	triple := mc.loopTriples[rand.Intn(len(mc.loopTriples))]
	a, b, c := mc.addrs[triple[0]], mc.addrs[triple[1]], mc.addrs[triple[2]]
	token := "TOKEN-LOOP3"
	amount := int64(20+rand.Intn(100)) * 1e8

	return []model.Tx{
		{Hash: randomHash(), From: a, To: b, Token: token, Amount: amount, Timestamp: ts, BlockNum: blockNum},
		{Hash: randomHash(), From: b, To: c, Token: token, Amount: amount, Timestamp: ts, BlockNum: blockNum},
		{Hash: randomHash(), From: c, To: a, Token: token, Amount: amount, Timestamp: ts, BlockNum: blockNum},
	}
}
