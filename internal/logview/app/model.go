package app

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/chenzhangda16/web3-logpipe/internal/logview/render"

	"github.com/chenzhangda16/web3-logpipe/internal/logpipe/bench"
	"github.com/chenzhangda16/web3-logpipe/internal/logview/schema"
	"github.com/chenzhangda16/web3-logpipe/internal/logview/source"
	"github.com/chenzhangda16/web3-logpipe/internal/logview/store"
)

type Config struct {
	FIFOPath   string
	Schema     string
	SamplePath string
}

type Model[T any] struct {
	width  int
	height int

	topRow int
	follow bool
	paused bool

	ctx      context.Context
	cancel   context.CancelFunc
	fifoPath string
	lastErr  string

	rowCh chan T
	errCh chan error

	mouseZone      string
	hoverScrollbar bool
	hold           holdState

	lastSeekTsMs  int64
	lastSeekRatio float64

	rows *store.RowStore[T]

	schemaRoot   *schema.Node
	schemaLeaves []*schema.Leaf
	layoutRoot   *schema.LayoutNode
	layoutMeta   schema.LayoutMeta
	styler       *render.Styler

	rowTsMs func(T) int64
	rowTick func(T) int64
}

func loadSchemaSample[T any](path string, zero T) (T, error) {
	if path == "" {
		return zero, nil
	}

	b, err := os.ReadFile(path)
	if err != nil {
		return zero, fmt.Errorf("read sample file: %w", err)
	}

	v := zero
	if err := json.Unmarshal(b, &v); err != nil {
		return zero, fmt.Errorf("unmarshal sample file: %w", err)
	}
	return v, nil
}

func newModel[T any](
	cfg Config,
	zero T,
	readFn func(context.Context, string, chan<- T) error,
	rowTsMs func(T) int64,
	rowTick func(T) int64,
) (Model[T], error) {
	schemaSample, err := loadSchemaSample(cfg.SamplePath, zero)
	if err != nil {
		return Model[T]{}, err
	}

	root, leaves, err := schema.BuildSchemaTreeWithKeys(
		schemaSample,
		schema.DefaultOverrideSet(),
	)
	if err != nil {
		return Model[T]{}, err
	}

	schema.AssignLeafX(leaves, 1)
	layoutRoot, layoutMeta := schema.BuildLayoutTree(root)

	ctx, cancel := context.WithCancel(context.Background())

	m := Model[T]{
		follow:       true,
		ctx:          ctx,
		cancel:       cancel,
		fifoPath:     cfg.FIFOPath,
		rowCh:        make(chan T, 128),
		errCh:        make(chan error, 1),
		rows:         store.NewRowStore[T](1000, 800),
		schemaRoot:   root,
		schemaLeaves: leaves,
		layoutRoot:   layoutRoot,
		layoutMeta:   layoutMeta,
		styler:       render.NewStyler(),
		rowTsMs:      rowTsMs,
		rowTick:      rowTick,
	}

	go func() {
		defer close(m.rowCh)

		if err := readFn(m.ctx, m.fifoPath, m.rowCh); err != nil {
			select {
			case <-m.ctx.Done():
			case m.errCh <- err:
			default:
			}
		}
	}()

	return m, nil
}

func NewModel(cfg Config) (tea.Model, error) {
	switch cfg.Schema {

	case "", "proc":
		return newModel(
			cfg,
			bench.ProcJson{},
			source.ReadJSON[bench.ProcJson],
			func(v bench.ProcJson) int64 { return v.TsMs },
			func(v bench.ProcJson) int64 { return v.Tick },
		)

	case "fetch":
		return newModel(
			cfg,
			bench.FetchJson{},
			source.ReadJSON[bench.FetchJson],
			func(v bench.FetchJson) int64 { return v.TsMs },
			func(v bench.FetchJson) int64 { return v.Tick },
		)

	default:
		return nil, fmt.Errorf("unsupported schema: %s", cfg.Schema)
	}
}

func Run(cfg Config) error {
	m, err := NewModel(cfg)
	if err != nil {
		return err
	}

	p := tea.NewProgram(m, tea.WithAltScreen(), tea.WithMouseCellMotion())
	_, err = p.Run()
	return err
}

func (m Model[T]) Init() tea.Cmd {
	return m.waitRowMsgCmd()
}

func (m *Model[T]) visibleRows() int {
	return m.bodyRows()
}

func (m *Model[T]) maxTopRow() int {
	maxRow := m.rows.Len() - m.visibleRows()
	if maxRow < 0 {
		return 0
	}
	return maxRow
}

func (m *Model[T]) scrollToBottom() {
	m.topRow = m.maxTopRow()
	m.follow = true
}

func (m *Model[T]) scrollBy(n int) {
	m.topRow += n

	if m.topRow < 0 {
		m.topRow = 0
	}
	if m.topRow > m.maxTopRow() {
		m.topRow = m.maxTopRow()
	}

	m.follow = (m.topRow == m.maxTopRow())
}

func (m *Model[T]) appendRow(v T) {
	dropped := m.rows.Append(v)

	if m.follow {
		m.topRow = m.maxTopRow()
	} else if dropped > 0 {
		m.topRow -= dropped
		if m.topRow < 0 {
			m.topRow = 0
		}
	}
}

func (m Model[T]) waitRowMsgCmd() tea.Cmd {
	return func() tea.Msg {
		select {
		case row, ok := <-m.rowCh:
			if !ok {
				return nil
			}
			return rowMsg[T]{Row: row}

		case err := <-m.errCh:
			return errMsg{Err: err}
		}
	}
}

func (m *Model[T]) statusRows() int {
	return 1
}

func (m *Model[T]) bodyRows() int {
	h := m.height - m.layoutMeta.HeaderRows - m.statusRows()
	if h < 1 {
		return 1
	}
	return h
}

func (m *Model[T]) bodyYRange() (startY, endY int) {
	startY = m.layoutMeta.HeaderRows
	endY = startY + m.bodyRows() - 1
	return
}

func (m *Model[T]) statusYRange() (startY, endY int) {
	endY = m.height - 1
	startY = endY - m.statusRows() + 1
	return
}

func (m *Model[T]) isHeaderY(y int) bool {
	return y >= 0 && y < m.layoutMeta.HeaderRows
}

func (m *Model[T]) isStatusY(y int) bool {
	sy0, sy1 := m.statusYRange()
	return y >= sy0 && y <= sy1
}

func (m *Model[T]) isBodyY(y int) bool {
	by0, by1 := m.bodyYRange()
	return y >= by0 && y <= by1
}

func (m *Model[T]) scrollbarWidth() int {
	return 1
}

func (m *Model[T]) scrollbarGapWidth() int {
	return 1
}

func (m *Model[T]) bodyContentWidth() int {
	w := m.width - m.scrollbarGapWidth() - m.scrollbarWidth()
	//w := m.width - m.scrollbarWidth()
	if w < 0 {
		return 0
	}
	return w
}

func (m *Model[T]) scrollbarXRange() (startX, endX int) {
	if m.width <= 0 {
		return -1, -1
	}

	endX = m.width - 1
	startX = m.width - m.scrollbarWidth()

	if startX < 0 {
		startX = 0
	}
	return
}

func (m *Model[T]) bodyContentXRange() (startX, endX int) {
	startX = 0
	endX = m.bodyContentWidth() - 1
	if endX < startX {
		endX = startX - 1
	}
	return
}

func (m *Model[T]) isScrollbarX(x int) bool {
	sx0, sx1 := m.scrollbarXRange()
	return x >= sx0 && x <= sx1
}

func (m *Model[T]) isBodyContentX(x int) bool {
	cx0, cx1 := m.bodyContentXRange()
	return x >= cx0 && x <= cx1
}

func (m *Model[T]) isScrollbarHit(x, y int) bool {
	return m.isBodyY(y) && m.isScrollbarX(x)
}

func (m *Model[T]) isBodyContentHit(x, y int) bool {
	return m.isBodyY(y) && m.isBodyContentX(x)
}

func (m *Model[T]) mouseZoneName(x, y int) string {
	switch {
	case m.isHeaderY(y):
		return "header"
	case m.isStatusY(y):
		return "status"
	case m.isScrollbarHit(x, y):
		return "scrollbar"
	case m.isBodyContentHit(x, y):
		return "body"
	case m.isBodyY(y):
		return "body-unknown"
	default:
		return "outside"
	}
}

func (m *Model[T]) bodyLocalY(y int) int {
	by0, _ := m.bodyYRange()
	return y - by0
}

func (m *Model[T]) isScrollbarBottomHot(y int) bool {
	if !m.isBodyY(y) {
		return false
	}
	localY := m.bodyLocalY(y)
	sm := m.buildScrollbarMetrics()
	return sm.isBottomHot(localY)
}

func (m *Model[T]) isScrollbarTopHot(y int) bool {
	if !m.isBodyY(y) {
		return false
	}
	localY := m.bodyLocalY(y)
	sm := m.buildScrollbarMetrics()
	return sm.isTopHot(localY)
}

func (m *Model[T]) seekToRatio(r float64) {
	if r < 0 {
		r = 0
	}
	if r > 1 {
		r = 1
	}

	maxTop := m.maxTopRow()
	if maxTop <= 0 {
		m.topRow = 0
		m.follow = true
		return
	}

	targetRow := int(r * float64(m.rows.Len()-1))
	m.seekToRow(targetRow)
}

func (m *Model[T]) seekToRow(targetRow int) {
	if m.rows.Len() <= 0 {
		m.topRow = 0
		m.follow = true
		return
	}

	if targetRow < 0 {
		targetRow = 0
	}
	if targetRow >= m.rows.Len() {
		targetRow = m.rows.Len() - 1
	}

	vis := m.visibleRows()
	anchor := vis / 2
	if anchor > 2 {
		anchor = vis / 3
	}

	top := targetRow - anchor
	if top < 0 {
		top = 0
	}
	if top > m.maxTopRow() {
		top = m.maxTopRow()
	}

	m.topRow = top
	m.follow = (m.topRow == m.maxTopRow())
}

func (m *Model[T]) seekFromScrollbarY(y int) {
	if !m.isBodyY(y) {
		return
	}
	localY := m.bodyLocalY(y)
	sm := m.buildScrollbarMetrics()
	r := sm.ratioForLocalY(localY)
	m.seekToTimeRatio(r)
}

func (m *Model[T]) seekToTimeRatio(r float64) {
	if r < 0 {
		r = 0
	}
	if r > 1 {
		r = 1
	}

	n := m.rows.Len()
	if n <= 0 {
		m.topRow = 0
		m.follow = true
		m.lastSeekTsMs = 0
		m.lastSeekRatio = r
		return
	}
	if n == 1 {
		m.lastSeekTsMs = m.rowTsMs(m.rows.At(0))
		m.lastSeekRatio = r
		m.seekToRow(0)
		return
	}

	first := m.rows.At(0)
	last := m.rows.At(m.rows.LastIndex())

	minTs := m.rowTsMs(first)
	maxTs := m.rowTsMs(last)

	if minTs <= 0 || maxTs <= 0 || maxTs <= minTs {
		m.lastSeekTsMs = 0
		m.lastSeekRatio = r
		m.seekToRatio(r)
		return
	}

	targetTs := minTs + int64(r*float64(maxTs-minTs))
	m.lastSeekTsMs = targetTs
	m.lastSeekRatio = r

	idx := m.findFirstRowAtOrAfterTs(targetTs)
	m.seekToRow(idx)
}

func (m *Model[T]) findFirstRowAtOrAfterTs(targetTs int64) int {
	n := m.rows.Len()
	if n <= 0 {
		return 0
	}

	lo, hi := 0, n
	for lo < hi {
		mid := lo + (hi-lo)/2
		ts := m.rowTsMs(m.rows.At(mid))
		if ts < targetTs {
			lo = mid + 1
		} else {
			hi = mid
		}
	}

	if lo >= n {
		return n - 1
	}
	return lo
}

func (m *Model[T]) windowTimeRange() (startTsMs, endTsMs int64, ok bool) {
	if m.rows.Len() == 0 {
		return 0, 0, false
	}

	lo := m.topRow
	if lo < 0 {
		lo = 0
	}
	if lo >= m.rows.Len() {
		lo = m.rows.Len() - 1
	}

	hi := lo + m.visibleRows() - 1
	if hi < lo {
		hi = lo
	}
	if hi >= m.rows.Len() {
		hi = m.rows.Len() - 1
	}

	startTsMs = m.rowTsMs(m.rows.At(lo))
	endTsMs = m.rowTsMs(m.rows.At(hi))
	return startTsMs, endTsMs, true
}
