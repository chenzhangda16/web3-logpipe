package processor

import "sort"

type WindowLike interface {
	Name() string
	WindowSec() int64
	Add(e TxEvent)
	Evict(nowTs int64)
	// optional: Snapshot/Report hooks
}

type MultiWindow struct {
	wins   []WindowLike
	maxSec int64
}

func NewMultiWindow(wins []WindowLike) *MultiWindow {
	// deterministic order: sort by window size asc (or name)
	sort.Slice(wins, func(i, j int) bool {
		return wins[i].WindowSec() < wins[j].WindowSec()
	})
	var max int64
	for _, w := range wins {
		if w.WindowSec() > max {
			max = w.WindowSec()
		}
	}
	return &MultiWindow{wins: wins, maxSec: max}
}

func (mw *MultiWindow) MaxWindowSec() int64 { return mw.maxSec }

func (mw *MultiWindow) Evict(nowTs int64) {
	for _, w := range mw.wins {
		w.Evict(nowTs)
	}
}

func (mw *MultiWindow) Add(e TxEvent) {
	for _, w := range mw.wins {
		w.Add(e)
	}
}

func (mw *MultiWindow) Windows() []WindowLike { return mw.wins }
