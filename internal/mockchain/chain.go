package mockchain

import (
	"math/rand"
	"sync"
	"time"

	"github.com/chenzhangda16/web3-logpipe/internal/logpipe/model"
)

type MockChain struct {
	mu          sync.Mutex
	nextBlock   int64
	addrs       []string
	loopPairs   [][2]int
	loopTriples [][3]int
}

func (mc *MockChain) NewMockChain(blockNum int64, ts int64) *MockChain {}

func (mc *MockChain) NextBlock() model.Block {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	blockNum := mc.nextBlock
	mc.nextBlock++

	now := time.Now().Unix()
	var txs []model.Tx

	// 每块随便来个 50～100 笔交易
	n := 50 + rand.Intn(50)

	for i := 0; i < n; i++ {
		p := rand.Float64()
		switch {
		case p < 0.2:
			// 20% 环交易
			txs = append(txs, mc.makeLoopTx(blockNum, now)...)
		default:
			// 80% 普通随机转账
			txs = append(txs, mc.makeRandomTx(blockNum, now))
		}
	}

	return model.Block{
		Number:    blockNum,
		Timestamp: now,
		Txs:       txs,
	}
}
