package processor

//import "github.com/chenzhangda16/web3-logpipe/internal/logpipe/event"
//
//type Window struct {
//	name      string
//	windowSec int64
//
//	// queue for eviction
//	evq  []event.TxEvent
//	head int
//
//	// example state: degrees in the window
//	inDeg  map[string]int64
//	outDeg map[string]int64
//}
//
//func NewWindow(name string, windowSec int64) *Window {
//	if windowSec <= 0 {
//		windowSec = 1
//	}
//	return &Window{
//		name:      name,
//		windowSec: windowSec,
//		evq:       make([]event.TxEvent, 0, 1024),
//		inDeg:     make(map[string]int64),
//		outDeg:    make(map[string]int64),
//	}
//}
//
//func (w *Window) Name() string     { return w.name }
//func (w *Window) WindowSec() int64 { return w.windowSec }
//
//func (w *Window) Add(e event.TxEvent) {
//	w.evq = append(w.evq, e)
//	w.outDeg[e.From]++
//	w.inDeg[e.To]++
//}
//
//func (w *Window) Evict(nowTs int64) {
//	cut := nowTs - w.windowSec
//	for w.head < len(w.evq) {
//		e := w.evq[w.head]
//		if e.Timestamp >= cut {
//			break
//		}
//		// remove effect
//		if w.outDeg[e.From] > 1 {
//			w.outDeg[e.From]--
//		} else {
//			delete(w.outDeg, e.From)
//		}
//		if w.inDeg[e.To] > 1 {
//			w.inDeg[e.To]--
//		} else {
//			delete(w.inDeg, e.To)
//		}
//		w.head++
//	}
//	// compact queue
//	if w.head > 4096 && w.head*2 > len(w.evq) {
//		newQ := make([]TxEvent, 0, len(w.evq)-w.head)
//		newQ = append(newQ, w.evq[w.head:]...)
//		w.evq = newQ
//		w.head = 0
//	}
//}
//
//// Example query method (you'll replace with graph metrics)
//func (w *Window) SnapshotTopOutDeg(n int) []KV {
//	// naive O(N) sort omitted; keep as placeholder
//	out := make([]KV, 0, n)
//	_ = out
//	return nil
//}
//
//type KV struct {
//	K string
//	V int64
//}
