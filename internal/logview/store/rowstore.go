package store

type RowStore[T any] struct {
	rows    []T
	maxRows int
	keep    int
}

func NewRowStore[T any](maxRows, keep int) *RowStore[T] {
	if maxRows < 0 {
		maxRows = 0
	}
	if keep <= 0 || (maxRows > 0 && keep > maxRows) {
		keep = maxRows
	}
	return &RowStore[T]{
		rows:    make([]T, 0, max(keep, 16)),
		maxRows: maxRows,
		keep:    keep,
	}
}

func (s *RowStore[T]) Append(v T) (dropped int) {
	s.rows = append(s.rows, v)

	if s.maxRows > 0 && len(s.rows) > s.maxRows {
		drop := len(s.rows) - s.keep
		s.rows = append([]T(nil), s.rows[drop:]...)
		return drop
	}
	return 0
}

func (s *RowStore[T]) Len() int {
	return len(s.rows)
}

func (s *RowStore[T]) At(i int) T {
	return s.rows[i]
}

func (s *RowStore[T]) Slice(lo, hi int) []T {
	return s.rows[lo:hi]
}

func (s *RowStore[T]) Clear() {
	s.rows = s.rows[:0]
}

func (s *RowStore[T]) LastIndex() int {
	if len(s.rows) == 0 {
		return -1
	}
	return len(s.rows) - 1
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
