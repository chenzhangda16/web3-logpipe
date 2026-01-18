package ingest

import (
	"log"

	"github.com/chenzhangda16/web3-logpipe/internal/logpipe/event"
	"github.com/chenzhangda16/web3-logpipe/internal/logpipe/ids"
	mc "github.com/chenzhangda16/web3-logpipe/internal/mockchain/model"
)

type MockChainAdapter struct {
	Addrs  *ids.AddressID
	Tokens *ids.TokenID

	// 可选：统计丢弃量
	DropBadAddr bool
	DropNoToken bool
}

func NewMockChainAdapter(addrs *ids.AddressID, tokens *ids.TokenID) *MockChainAdapter {
	return &MockChainAdapter{
		Addrs:       addrs,
		Tokens:      tokens,
		DropBadAddr: true,
		DropNoToken: false, // token 为空可以映射成 0（看你后续策略）
	}
}

// EmitTxEventsFromBlock：把协议 Block 翻译成分析 TxEvent。
// emit 必须是 "Append 到 Dispatcher" 的函数；它会返回 seq（或不返回也行，看你实现）。
func (a *MockChainAdapter) EmitTxEventsFromBlock(blk mc.Block, emit func(ev event.TxEvent) uint64) {
	// 统一时间：建议用 blk.Header.Timestamp（你现在是 int64）
	blockTs := blk.Header.Timestamp

	for _, tx := range blk.Txs {
		body := tx.TxBody

		fromID, ok := a.Addrs.ID(body.From)
		if !ok {
			if a.DropBadAddr {
				log.Printf("[ingest] drop tx: bad from addr=%q", body.From)
				continue
			}
			fromID = 0
		}

		toID, ok := a.Addrs.ID(body.To)
		if !ok {
			if a.DropBadAddr {
				log.Printf("[ingest] drop tx: bad to addr=%q", body.To)
				continue
			}
			toID = 0
		}

		tokenID := a.Tokens.ID(body.Token)
		if tokenID == 0 && a.DropNoToken {
			log.Printf("[ingest] drop tx: empty token")
			continue
		}

		ev := event.TxEvent{
			Ts:     blockTs,
			From:   fromID,
			To:     toID,
			Token:  tokenID,
			Amount: body.Amount,
		}

		_ = emit(ev) // seq 如需冷路径 meta，可用 emit 返回值
	}
}
