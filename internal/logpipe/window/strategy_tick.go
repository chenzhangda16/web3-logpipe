package window

import (
	"context"

	"github.com/chenzhangda16/web3-logpipe/internal/logpipe/dispatcher"
	"github.com/chenzhangda16/web3-logpipe/internal/logpipe/out"
)

type EmitTick struct {
	Every int64
	n     int64
}

func (s *EmitTick) OnMove(ctx context.Context, r *Runner, mv dispatcher.TxWinMarginInfo, sink out.Sink) error {
	if s.Every <= 0 {
		s.Every = 200
	}
	s.n++
	if s.n%s.Every != 0 {
		return nil
	}

	tick := out.WinTick{
		WinIdx:  r.winIdx,
		Head:    mv.TxHead,
		Tail:    mv.TxTail,
		OpenWin: mv.OpenWin,
	}
	return sink.Emit(ctx, "win_tick", tick)
}
