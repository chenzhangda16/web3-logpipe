package schema

import (
	"strings"

	"github.com/charmbracelet/lipgloss"
)

type HeaderRenderConfig struct {
	Gap int

	// 是否画组边界
	DrawTopLevelDivider bool
	DrawInnerDivider    bool

	// 边界字符
	TopLevelDivider string
	InnerDivider    string

	// 是否对叶子列之间保留 gap
	PreserveLeafGap bool
}

func DefaultHeaderRenderConfig() HeaderRenderConfig {
	return HeaderRenderConfig{
		Gap:                 1,
		DrawTopLevelDivider: true,
		DrawInnerDivider:    true,
		TopLevelDivider:     "┃",
		InnerDivider:        "│",
		PreserveLeafGap:     true,
	}
}

type HeaderTheme struct {
	TopLevelBase []lipgloss.Color
	InnerBase    []lipgloss.Color
	LeafBase     []lipgloss.Color

	TopDividerStyle   lipgloss.Style
	InnerDividerStyle lipgloss.Style
}

func DefaultHeaderTheme() HeaderTheme {
	return HeaderTheme{
		// 顶层：更稳、更显眼
		TopLevelBase: []lipgloss.Color{
			lipgloss.Color("111"), // 灰蓝
			lipgloss.Color("117"), // 青蓝
			lipgloss.Color("180"), // 黄褐
			lipgloss.Color("141"), // 紫
			lipgloss.Color("174"), // 粉紫
			lipgloss.Color("150"), // 绿
		},

		// 二层/三层：稍微淡一点
		InnerBase: []lipgloss.Color{
			lipgloss.Color("109"),
			lipgloss.Color("116"),
			lipgloss.Color("179"),
			lipgloss.Color("140"),
			lipgloss.Color("181"),
			lipgloss.Color("149"),
		},

		// 叶子：再收一点，避免太吵
		LeafBase: []lipgloss.Color{
			lipgloss.Color("102"),
			lipgloss.Color("109"),
			lipgloss.Color("144"),
			lipgloss.Color("139"),
			lipgloss.Color("145"),
			lipgloss.Color("108"),
		},

		TopDividerStyle:   lipgloss.NewStyle().Foreground(lipgloss.Color("245")).Bold(true),
		InnerDividerStyle: lipgloss.NewStyle().Foreground(lipgloss.Color("240")),
	}
}

type styledCell struct {
	ch    rune
	style lipgloss.Style
	set   bool
}

type headerCanvas struct {
	rows [][]styledCell
}

func newHeaderCanvas(h, w int) *headerCanvas {
	rows := make([][]styledCell, h)
	for i := range rows {
		rows[i] = make([]styledCell, w)
		for j := range rows[i] {
			rows[i][j] = styledCell{ch: ' '}
		}
	}
	return &headerCanvas{rows: rows}
}

func (c *headerCanvas) width() int {
	if len(c.rows) == 0 {
		return 0
	}
	return len(c.rows[0])
}

func (c *headerCanvas) height() int {
	return len(c.rows)
}

func (c *headerCanvas) putRune(y, x int, r rune, st lipgloss.Style) {
	if y < 0 || y >= len(c.rows) {
		return
	}
	if x < 0 || x >= len(c.rows[y]) {
		return
	}
	c.rows[y][x] = styledCell{
		ch:    r,
		style: st,
		set:   true,
	}
}

func (c *headerCanvas) putTextCentered(y, x0, x1 int, text string, st lipgloss.Style) {
	if y < 0 || y >= c.height() || x1 < x0 {
		return
	}

	rs := []rune(text)
	if len(rs) == 0 {
		return
	}

	spanW := x1 - x0 + 1
	if spanW <= 0 {
		return
	}
	if len(rs) > spanW {
		rs = rs[:spanW]
	}

	mid := (x0 + x1) / 2
	start := mid - len(rs)/2
	if start < x0 {
		start = x0
	}
	if start+len(rs)-1 > x1 {
		start = x1 - len(rs) + 1
	}
	if start < 0 {
		start = 0
	}

	for i, r := range rs {
		c.putRune(y, start+i, r, st)
	}
}

func (c *headerCanvas) render() []string {
	out := make([]string, 0, len(c.rows))
	for _, row := range c.rows {
		var b strings.Builder
		for _, cell := range row {
			s := string(cell.ch)
			if cell.set {
				b.WriteString(cell.style.Render(s))
			} else {
				b.WriteString(s)
			}
		}
		out = append(out, rtrimSpaces(b.String()))
	}
	return out
}

// RenderTreeHeader 升级版：
// 1. 顶层/二层/叶子分层着色
// 2. 顶层边界、内层边界更明显
// 3. 叶子统一沉到底层
// 4. 非叶子按自身 depth-1 行渲染
func RenderTreeHeader(root *LayoutNode, leaves []*Leaf, cfg HeaderRenderConfig) []string {
	return RenderTreeHeaderWithTheme(root, leaves, cfg, DefaultHeaderTheme())
}

func RenderTreeHeaderWithTheme(
	root *LayoutNode,
	leaves []*Leaf,
	cfg HeaderRenderConfig,
	theme HeaderTheme,
) []string {
	if root == nil || len(leaves) == 0 {
		return nil
	}

	maxDepth := HeaderMaxDepth(root)
	if maxDepth <= 0 {
		maxDepth = 1
	}

	totalWidth := totalLeafSpanWidth(leaves)
	if totalWidth <= 0 {
		return nil
	}

	canvas := newHeaderCanvas(maxDepth, totalWidth)

	// 先画分隔，再画标签，让标签可以覆盖部分分隔线，避免太碎。
	if cfg.DrawTopLevelDivider {
		drawTopLevelDividers(canvas, root, cfg, theme)
	}
	if cfg.DrawInnerDivider {
		for _, ch := range root.Children {
			drawInnerDividers(canvas, ch, cfg, theme)
		}
	}

	// root 自身不渲染，只渲染 children
	for i, ch := range root.Children {
		drawHeaderNode(canvas, ch, maxDepth, theme, 1, i)
	}

	return canvas.render()
}

func drawHeaderNode(
	canvas *headerCanvas,
	n *LayoutNode,
	maxDepth int,
	theme HeaderTheme,
	level int,
	siblingIdx int,
) {
	if n == nil {
		return
	}

	if n.Leaf != nil {
		rowIdx := maxDepth - 1
		style := leafStyle(theme, siblingIdx)
		canvas.putTextCentered(rowIdx, n.X0, n.X1, n.Leaf.Title, style)
		return
	}

	rowIdx := n.Depth - 1
	if rowIdx < 0 {
		rowIdx = 0
	}
	if rowIdx >= maxDepth {
		rowIdx = maxDepth - 1
	}

	style := groupStyle(theme, level, siblingIdx)
	canvas.putTextCentered(rowIdx, n.X0, n.X1, n.Name, style)

	for i, ch := range n.Children {
		drawHeaderNode(canvas, ch, maxDepth, theme, level+1, i)
	}
}

func drawTopLevelDividers(
	canvas *headerCanvas,
	root *LayoutNode,
	cfg HeaderRenderConfig,
	theme HeaderTheme,
) {
	if root == nil || len(root.Children) <= 1 {
		return
	}

	divRune := []rune(cfg.TopLevelDivider)
	if len(divRune) == 0 {
		divRune = []rune("┃")
	}
	r := divRune[0]

	for i := 0; i < len(root.Children)-1; i++ {
		left := root.Children[i]
		right := root.Children[i+1]
		x := dividerXBetween(left, right)
		if x < 0 {
			continue
		}
		for y := 0; y < canvas.height(); y++ {
			canvas.putRune(y, x, r, theme.TopDividerStyle)
		}
	}
}

func drawInnerDividers(
	canvas *headerCanvas,
	n *LayoutNode,
	cfg HeaderRenderConfig,
	theme HeaderTheme,
) {
	if n == nil || len(n.Children) <= 1 {
		for _, ch := range n.Children {
			drawInnerDividers(canvas, ch, cfg, theme)
		}
		return
	}

	divRune := []rune(cfg.InnerDivider)
	if len(divRune) == 0 {
		divRune = []rune("│")
	}
	r := divRune[0]

	// 只在当前节点覆盖的有效 header 行里画，不要一路贯穿到底层太吵。
	// 当前节点自身所在行为 n.Depth-1，子节点从更下一层开始有意义。
	y0 := n.Depth - 1
	if y0 < 0 {
		y0 = 0
	}
	y1 := minInt(canvas.height()-1, HeaderMaxDepth(n)-1)
	if y1 < y0 {
		y1 = y0
	}

	for i := 0; i < len(n.Children)-1; i++ {
		left := n.Children[i]
		right := n.Children[i+1]
		x := dividerXBetween(left, right)
		if x < 0 {
			continue
		}
		for y := y0; y <= y1; y++ {
			canvas.putRune(y, x, r, theme.InnerDividerStyle)
		}
	}

	for _, ch := range n.Children {
		drawInnerDividers(canvas, ch, cfg, theme)
	}
}

func groupStyle(theme HeaderTheme, level int, siblingIdx int) lipgloss.Style {
	switch {
	case level <= 1:
		return lipgloss.NewStyle().Foreground(pickColor(theme.TopLevelBase, siblingIdx)).Bold(true)
	case level == 2:
		return lipgloss.NewStyle().Foreground(pickColor(theme.InnerBase, siblingIdx)).Bold(true)
	default:
		return lipgloss.NewStyle().Foreground(pickColor(theme.InnerBase, siblingIdx))
	}
}

func leafStyle(theme HeaderTheme, siblingIdx int) lipgloss.Style {
	return lipgloss.NewStyle().Foreground(pickColor(theme.LeafBase, siblingIdx))
}

func pickColor(base []lipgloss.Color, idx int) lipgloss.Color {
	if len(base) == 0 {
		return lipgloss.Color("252")
	}
	return base[idx%len(base)]
}

func dividerXBetween(left, right *LayoutNode) int {
	if left == nil || right == nil {
		return -1
	}
	if right.X0 <= left.X1 {
		return left.X1
	}
	return (left.X1 + right.X0) / 2
}

func totalLeafSpanWidth(leaves []*Leaf) int {
	if len(leaves) == 0 {
		return 0
	}
	last := leaves[len(leaves)-1]
	return last.X1 + 1
}

func rtrimSpaces(s string) string {
	return strings.TrimRight(s, " ")
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
