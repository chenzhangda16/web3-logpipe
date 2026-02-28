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
	var expQ []int

	// 非阻塞从 f.pgNCh 拉一些 expectedN 进 expQ
	drainExpected := func() {
		for {
			select {
			case n := <-f.pgNCh:
				expQ = append(expQ, n)
			default:
				return
			}
		}
	}

	// 现在 advance() 需要知道 minOpen 对应的 expectedN：它在 expQ[0]
	advance := func() {
		for minOpen >= 0 {
			drainExpected()
			if len(expQ) == 0 {
				// 没有 expectedN，先别推进（但绝不阻塞）
				return
			}
			need := expQ[0] // minOpen 对应的 expectedN

			st, ok := stats[minOpen]
			if ok && st.n >= need {
				// page 完成：推 ckpt
				pushLatestCkpt(ckptCh, Ckpt{LastHeight: st.maxH, LastHash: st.maxHash})
				delete(stats, minOpen)
				minOpen++
				expQ = expQ[1:] // 消耗一个 expectedN
				continue
			}
			return
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
			if f.bench != nil {
				f.bench.AddProdErr()
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

			if f.bench != nil {
				f.bench.AddAckBlocks(1)
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
				have0 := 0
				if old != nil {
					have0 = old.n
				}
				expected := -1
				expectedKnown := false
				if minOpen >= 0 && len(expQ) > 0 {
					expected = expQ[0]
					expectedKnown = true
				}

				have1 := 0
				if st := stats[minOpen+1]; st != nil {
					have1 = st.n
				}

				if f.bench != nil {
					f.bench.AddLagFatal()
				}

				return fmt.Errorf(
					"FATAL lag: minOpen=%d have0=%d expected0=%d known0=%t | next=%d have1=%d | maxSeen=%d lag=%d",
					minOpen, have0, expected, expectedKnown, minOpen+1, have1, maxSeen, maxSeen-minOpen,
				)
			}
		}
	}
}
