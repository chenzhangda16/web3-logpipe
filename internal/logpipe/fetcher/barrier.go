package fetcher

import (
	"context"
	"errors"
	"fmt"
	"log"
)

type pageStat struct {
	n    int
	maxH int64
	// 你现在 block 没带 hash 给 meta 的话，这里就先留空
	maxHash string
}

type BarrierCfg struct {
	PageSize        int
	AllowedLagPages int64 // 1
}

func (f *Fetcher) barrierLoop(
	ctx context.Context,
	cfg BarrierCfg,
	ckptCh chan Ckpt, // capacity=1, overwrite
) error {
	if f.prod == nil {
		return errors.New("producer nil")
	}
	succ := f.prod.Successes()
	fail := f.prod.Errors()
	if succ == nil || fail == nil {
		return errors.New("producer successes/errors not enabled")
	}

	stats := make(map[int64]*pageStat, 64)

	var minOpen int64 = -1
	var maxSeen int64 = -1

	advance := func() {
		for minOpen >= 0 {
			st, ok := stats[minOpen]
			if ok && st.n >= cfg.PageSize {
				// page minOpen 完成：推 ckpt（覆盖式，不阻塞）
				pushLatestCkpt(ckptCh, Ckpt{
					LastHeight: st.maxH,
					LastHash:   st.maxHash,
				})
				delete(stats, minOpen)
				minOpen++
				continue
			}
			break
		}
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case pe, ok := <-fail:
			if !ok {
				return nil
			}
			// 真正的 produce 错误：直接 fatal
			// pe.Msg 里也能拿到 Metadata（如果还在）
			if pe.Msg != nil {
				if meta, ok := pe.Msg.Metadata.(ProduceMeta); ok {
					log.Printf("[fetcher] producer error: page=%d height=%d err=%v",
						meta.PageSeq, meta.Height, pe.Err)
				} else {
					log.Printf("[fetcher] producer error: err=%v", pe.Err)
				}
			}
			return pe.Err

		case msg, ok := <-succ:
			if !ok {
				return nil
			}
			meta, ok := msg.Metadata.(ProduceMeta)
			if !ok {
				return errors.New("producer success meta missing")
			}

			ps := meta.PageSeq
			if minOpen < 0 {
				minOpen = ps // 或者强制从0开始：minOpen=0
			}
			if ps > maxSeen {
				maxSeen = ps
			}

			st := stats[ps]
			if st == nil {
				st = &pageStat{}
				stats[ps] = st
			}
			st.n++
			if meta.Height > st.maxH {
				st.maxH = meta.Height
				st.maxHash = meta.Hash.Hex()
			}

			advance()

			// fatal：落后超过 AllowedLagPages 页
			if minOpen >= 0 && (maxSeen-minOpen) > cfg.AllowedLagPages {
				old := stats[minOpen]
				have := 0
				if old != nil {
					have = old.n
				}
				return fmt.Errorf("FATAL lag: minOpen=%d have=%d/%d maxSeen=%d lag=%d",
					minOpen, have, cfg.PageSize, maxSeen, maxSeen-minOpen)
			}
		}
	}
}
