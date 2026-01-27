package window

type I64Queue struct {
	buf  []int64
	head int
}

func (q *I64Queue) Push(x int64) {
	q.buf = append(q.buf, x)
}

func (q *I64Queue) Empty() bool {
	return q.head >= len(q.buf)
}

func (q *I64Queue) Front() (int64, bool) {
	if q.Empty() {
		return 0, false
	}
	return q.buf[q.head], true
}

func (q *I64Queue) PopFront() (int64, bool) {
	if q.Empty() {
		return 0, false
	}
	v := q.buf[q.head]
	q.head++
	q.maybeCompact()
	return v, true
}

// PopFrontIfEq pops front only if it equals x.
// Returns true if popped.
func (q *I64Queue) PopFrontIfEq(x int64) bool {
	v, ok := q.Front()
	if !ok || v != x {
		return false
	}
	_, _ = q.PopFront()
	return true
}

func (q *I64Queue) maybeCompact() {
	if q.head < 4096 {
		return
	}
	if q.head*2 < len(q.buf) {
		return
	}
	n := len(q.buf) - q.head
	newBuf := make([]int64, n)
	copy(newBuf, q.buf[q.head:])
	q.buf = newBuf
	q.head = 0
}
