package window

import (
	"context"

	"github.com/chenzhangda16/web3-logpipe/internal/logpipe/dispatcher"
	"github.com/chenzhangda16/web3-logpipe/internal/logpipe/out"
)

type Strategy interface {
	OnMove(ctx context.Context, r *Runner, mv dispatcher.TxWinMarginInfo, sink out.Sink) error
}
