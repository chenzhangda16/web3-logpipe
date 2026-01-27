package window

import (
	"context"

	"github.com/chenzhangda16/web3-logpipe/internal/logpipe/dispatcher"
	"github.com/chenzhangda16/web3-logpipe/internal/logpipe/out"
)

type Runner struct {
	winIdx     int
	disp       *dispatcher.Dispatcher
	strategies []Strategy
	sink       out.Sink

	head int64
	tail int64

	adj          map[uint32]map[uint32]struct{}
	rev          map[uint32]map[uint32]struct{}
	edgeEvidence map[uint64]*I64Queue

	allOpen *bool // 所有窗口共享，由最长窗打开
	maxWin  bool  // 这个 runner 是否最长窗
}

func NewRunner(winIdx int, disp *dispatcher.Dispatcher, sink out.Sink, allOpen *bool, maxWin bool, strategies ...Strategy) *Runner {
	const (
		adjCap     = 1 << 12
		eviEdgeCap = 1 << 14
	)
	return &Runner{
		winIdx:       winIdx,
		disp:         disp,
		sink:         sink,
		allOpen:      allOpen,
		maxWin:       maxWin,
		strategies:   strategies,
		adj:          make(map[uint32]map[uint32]struct{}, adjCap),
		rev:          make(map[uint32]map[uint32]struct{}, adjCap),
		edgeEvidence: make(map[uint64]*I64Queue, eviEdgeCap),
	}
}

func (r *Runner) Run(ctx context.Context) error {
	ch := r.disp.WinMoveCh(r.winIdx)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case mv, ok := <-ch:
			if !ok {
				return nil
			}
			if err := r.handleMove(ctx, mv); err != nil {
				return err
			}
		}
	}
}

func (r *Runner) handleMove(ctx context.Context, mv dispatcher.TxWinMarginInfo) error {
	// 1) 永远维护窗口语义（短窗不偷跑 ≠ 不维护）
	r.addEdges(mv.TxHead)
	r.delEdges(mv.TxTail)

	// 2) 仅靠 mv.OpenWin：最长窗打开全局 gate
	if r.maxWin && mv.OpenWin && r.allOpen != nil {
		*r.allOpen = true
	}

	// 3) gate 未开：不输出/不跑重算法
	if r.allOpen != nil && !*r.allOpen {
		return nil
	}

	// 4) gate 已开：跑策略
	for _, st := range r.strategies {
		if err := st.OnMove(ctx, r, mv, r.sink); err != nil {
			return err
		}
	}
	return nil
}

func (r *Runner) addEdges(head int64) {
	if head <= r.head {
		return
	}
	for i := r.tail; i < head; i++ {
		ev := r.disp.Get(i)

		from := uint32(ev.From)
		to := uint32(ev.To)

		// 1) 简单图：只保留连通关系（Tarjan 用）
		r.ensureAdj(r.adj, from, to)
		r.ensureAdj(r.rev, to, from)

		// 2) 证据：边 -> idx 队列（可随时揪出该边所有交易）
		k := edgeKey(from, to)
		q := r.edgeEvidence[k]
		if q == nil {
			q = &I64Queue{}
			r.edgeEvidence[k] = q
		}
		q.Push(i)
	}

	r.head = head
}

func edgeKey(from, to uint32) uint64 {
	return (uint64(from) << 32) | uint64(to)
}

func (r *Runner) ensureAdj(m map[uint32]map[uint32]struct{}, a, b uint32) {
	row := m[a]
	if row == nil {
		row = make(map[uint32]struct{}, 8)
		m[a] = row
	}
	row[b] = struct{}{}
}

func (r *Runner) delEdges(newTail int64) {
	if newTail <= r.tail {
		return
	}
	if newTail > r.head {
		// 防御：理论上不该发生（tail 不应超过 head）
		newTail = r.head
	}

	for i := r.tail; i < newTail; i++ {
		ev := r.disp.Get(i)
		from := uint32(ev.From)
		to := uint32(ev.To)

		k := edgeKey(from, to)
		q := r.edgeEvidence[k]
		if q == nil {
			// 这代表你的索引和事件流不同步（通常是 bug）
			// 为了不中断运行，这里直接跳过；你调试期也可以 panic
			continue
		}

		// 队列必须严格按时间递增，如果这里不相等，说明你有乱序/重复删等 bug
		if !q.PopFrontIfEq(i) {
			// 调试期建议直接 panic，能最快抓到并发/顺序问题
			// panic(fmt.Sprintf("edgeEvidence desync: edge=%d front=%v want=%d", k, front, i))
			// 这里我按“不中断”写：尝试把所有 < i 的弹掉（容错），然后再试一次
			for {
				front, ok := q.Front()
				if !ok || front >= i {
					break
				}
				q.PopFront()
			}
			_ = q.PopFrontIfEq(i)
		}

		// 如果这条边在窗口内已经没有任何 tx 了，删简单图中的连边
		if q.Empty() {
			delete(r.edgeEvidence, k)
			r.delAdjEdge(r.adj, from, to)
			r.delAdjEdge(r.rev, to, from)
		}
	}

	r.tail = newTail
}

func (r *Runner) delAdjEdge(m map[uint32]map[uint32]struct{}, a, b uint32) {
	row := m[a]
	if row == nil {
		return
	}
	delete(row, b)
	if len(row) == 0 {
		delete(m, a)
	}
}
