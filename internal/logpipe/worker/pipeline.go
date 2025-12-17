package worker

import (
	"github.com/chenzhangda16/web3-logpipe/internal/logpipe/model"
	"github.com/chenzhangda16/web3-logpipe/internal/logpipe/state"
)

func EnrichTx(st *state.ShardState, tx model.Tx) model.EnrichedEvent {
	// 1. 更新滑动窗口
	// 2. 做高频进出检测
	// 3. 做短环 / 自转检测（简单版）
	// 4. 打标 + 计算 RiskScore
}
