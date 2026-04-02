package app

import (
	"context"
	"fmt"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/chenzhangda16/web3-logpipe/internal/logview/render"

	"github.com/chenzhangda16/web3-logpipe/internal/logpipe/bench"
	"github.com/chenzhangda16/web3-logpipe/internal/logview/schema"
	"github.com/chenzhangda16/web3-logpipe/internal/logview/source"
	"github.com/chenzhangda16/web3-logpipe/internal/logview/store"
)

type Config struct {
	FIFOPath string
	Schema   string
}

type Model struct {
	width  int
	height int

	topRow int
	follow bool
	paused bool

	ctx      context.Context
	cancel   context.CancelFunc
	fifoPath string
	lastErr  string

	procCh chan bench.ProcJson
	errCh  chan error

	mouseZone      string
	hoverScrollbar bool
	hold           holdState

	lastSeekTsMs  int64
	lastSeekRatio float64

	rows *store.RowStore[bench.ProcJson]

	schemaRoot   *schema.Node
	schemaLeaves []*schema.Leaf
	layoutRoot   *schema.LayoutNode
	layoutMeta   schema.LayoutMeta
	styler       *render.Styler
}

func NewModel(cfg Config) (Model, error) {
	var (
		root       *schema.Node
		leaves     []*schema.Leaf
		layoutRoot *schema.LayoutNode
		layoutMeta schema.LayoutMeta
		err        error
	)

	switch cfg.Schema {
	case "", "proc":
		root, leaves, err = schema.BuildSchemaTreeWithKeys(
			bench.ProcJson{},
			schema.DefaultOverrideSet(),
			schema.ProcMapKeyProvider(),
		)
		if err != nil {
			return Model{}, err
		}

	default:
		return Model{}, fmt.Errorf("unsupported schema: %s", cfg.Schema)
	}

	schema.AssignLeafX(leaves, 1)
	layoutRoot, layoutMeta = schema.BuildLayoutTree(root)

	ctx, cancel := context.WithCancel(context.Background())

	m := Model{
		follow:       true,
		ctx:          ctx,
		cancel:       cancel,
		fifoPath:     cfg.FIFOPath,
		procCh:       make(chan bench.ProcJson, 128),
		errCh:        make(chan error, 1),
		rows:         store.NewRowStore[bench.ProcJson](1000, 800),
		schemaRoot:   root,
		schemaLeaves: leaves,
		layoutRoot:   layoutRoot,
		layoutMeta:   layoutMeta,
		styler:       render.NewStyler(),
	}

	go func() {
		defer close(m.procCh)

		if err := source.ReadProcJSON(m.ctx, m.fifoPath, m.procCh); err != nil {
			select {
			case <-m.ctx.Done():
			case m.errCh <- err:
			default:
			}
		}
	}()

	return m, nil
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

func (m Model) Init() tea.Cmd {
	return m.waitProcMsgCmd()
}

func (m *Model) visibleRows() int {
	return m.bodyRows()
}

func (m *Model) maxTopRow() int {
	maxRow := m.rows.Len() - m.visibleRows()
	if maxRow < 0 {
		return 0
	}
	return maxRow
}

func (m *Model) scrollToBottom() {
	m.topRow = m.maxTopRow()
	m.follow = true
}

func (m *Model) scrollBy(n int) {
	m.topRow += n

	if m.topRow < 0 {
		m.topRow = 0
	}
	if m.topRow > m.maxTopRow() {
		m.topRow = m.maxTopRow()
	}

	if m.topRow == m.maxTopRow() {
		m.follow = true
	} else {
		m.follow = false
	}
}

func (m *Model) appendRow(v bench.ProcJson) {
	dropped := m.rows.Append(v)
	if dropped > 0 {
		m.topRow -= dropped
		if m.topRow < 0 {
			m.topRow = 0
		}
	}
	if m.follow {
		m.topRow = m.maxTopRow()
	}
}

func (m Model) waitProcMsgCmd() tea.Cmd {
	return func() tea.Msg {
		select {
		case <-m.ctx.Done():
			return procErrMsg{Err: m.ctx.Err()}

		case err := <-m.errCh:
			if err != nil {
				return procErrMsg{Err: err}
			}
			return nil

		case row, ok := <-m.procCh:
			if !ok {
				return nil
			}
			return procRowMsg{Row: row}
		}
	}
}

func (m *Model) statusRows() int {
	return 1
}

func (m *Model) bodyRows() int {
	h := m.height - m.layoutMeta.HeaderRows - m.statusRows()
	if h < 1 {
		return 1
	}
	return h
}

func (m *Model) bodyYRange() (startY, endY int) {
	startY = m.layoutMeta.HeaderRows
	endY = startY + m.bodyRows() - 1
	return
}

func (m *Model) statusYRange() (startY, endY int) {
	endY = m.height - 1
	startY = endY - m.statusRows() + 1
	return
}

func (m *Model) isHeaderY(y int) bool {
	return y >= 0 && y < m.layoutMeta.HeaderRows
}

func (m *Model) isStatusY(y int) bool {
	sy0, sy1 := m.statusYRange()
	return y >= sy0 && y <= sy1
}

func (m *Model) isBodyY(y int) bool {
	by0, by1 := m.bodyYRange()
	return y >= by0 && y <= by1
}

func (m *Model) scrollbarWidth() int {
	// 当前先固定 1 列
	return 1
}

func (m *Model) bodyContentWidth() int {
	w := m.width - m.scrollbarWidth()
	if w < 0 {
		return 0
	}
	return w
}

func (m *Model) scrollbarXRange() (startX, endX int) {
	if m.width <= 0 {
		return -1, -1
	}
	endX = m.width - 1
	startX = endX - m.scrollbarWidth() + 1
	if startX < 0 {
		startX = 0
	}
	return
}

func (m *Model) bodyContentXRange() (startX, endX int) {
	startX = 0
	endX = m.bodyContentWidth() - 1
	if endX < startX {
		endX = startX - 1
	}
	return
}

func (m *Model) isScrollbarX(x int) bool {
	sx0, sx1 := m.scrollbarXRange()
	return x >= sx0 && x <= sx1
}

func (m *Model) isBodyContentX(x int) bool {
	cx0, cx1 := m.bodyContentXRange()
	return x >= cx0 && x <= cx1
}

func (m *Model) isScrollbarHit(x, y int) bool {
	return m.isBodyY(y) && m.isScrollbarX(x)
}

func (m *Model) isBodyContentHit(x, y int) bool {
	return m.isBodyY(y) && m.isBodyContentX(x)
}

func (m *Model) mouseZoneName(x, y int) string {
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

func (m *Model) bodyLocalY(y int) int {
	by0, _ := m.bodyYRange()
	return y - by0
}

func (m *Model) isScrollbarBottomHot(y int) bool {
	if !m.isBodyY(y) {
		return false
	}
	localY := m.bodyLocalY(y)
	sm := m.buildScrollbarMetrics()
	return sm.isBottomHot(localY)
}

func (m *Model) isScrollbarTopHot(y int) bool {
	if !m.isBodyY(y) {
		return false
	}
	localY := m.bodyLocalY(y)
	sm := m.buildScrollbarMetrics()
	return sm.isTopHot(localY)
}

func (m *Model) seekToRatio(r float64) {
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

	// 先算“目标行”，再把它放到屏幕中间偏上
	targetRow := int(r * float64(m.rows.Len()-1))
	m.seekToRow(targetRow)
}

func (m *Model) seekToRow(targetRow int) {
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
		// 稍微偏上，给下方多一点空间看后续变化
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
	if m.topRow == m.maxTopRow() {
		m.follow = true
	} else {
		m.follow = false
	}
}

func (m *Model) seekFromScrollbarY(y int) {
	if !m.isBodyY(y) {
		return
	}
	localY := m.bodyLocalY(y)
	sm := m.buildScrollbarMetrics()
	r := sm.ratioForLocalY(localY)
	m.seekToTimeRatio(r)
}

func (m *Model) seekToTimeRatio(r float64) {
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
		m.lastSeekTsMs = m.rows.At(0).TsMs
		m.lastSeekRatio = r
		m.seekToRow(0)
		return
	}

	first := m.rows.At(0)
	last := m.rows.At(m.rows.LastIndex())

	minTs := first.TsMs
	maxTs := last.TsMs

	// 时间戳无效或不递增时，退化成按行 seek
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

func (m *Model) findFirstRowAtOrAfterTs(targetTs int64) int {
	n := m.rows.Len()
	if n <= 0 {
		return 0
	}

	lo, hi := 0, n
	for lo < hi {
		mid := lo + (hi-lo)/2
		ts := m.rows.At(mid).TsMs
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

func (m *Model) windowTimeRange() (startTsMs, endTsMs int64, ok bool) {
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

	startTsMs = m.rows.At(lo).TsMs
	endTsMs = m.rows.At(hi).TsMs
	return startTsMs, endTsMs, true
}
