package app

import (
	"strings"

	"github.com/charmbracelet/lipgloss"
)

type scrollbarMetrics struct {
	BodyHeight  int
	TotalRows   int
	VisibleRows int
	TopRow      int
	MaxTopRow   int
	ThumbTop    int
	ThumbHeight int
}

func (m Model[T]) buildScrollbarMetrics() scrollbarMetrics {
	bodyH := m.visibleRows()
	total := m.rows.Len()
	vis := m.visibleRows()
	maxTop := m.maxTopRow()

	sm := scrollbarMetrics{
		BodyHeight:  bodyH,
		TotalRows:   total,
		VisibleRows: vis,
		TopRow:      m.topRow,
		MaxTopRow:   maxTop,
	}

	if bodyH <= 0 {
		return sm
	}

	if total <= 0 || total <= vis || maxTop <= 0 {
		sm.ThumbTop = 0
		sm.ThumbHeight = bodyH
		return sm
	}

	thumbH := bodyH * vis / total
	if thumbH < 1 {
		thumbH = 1
	}
	if thumbH > bodyH {
		thumbH = bodyH
	}

	trackMovable := bodyH - thumbH
	var thumbTop int
	if trackMovable <= 0 {
		thumbTop = 0
	} else {
		thumbTop = m.topRow * trackMovable / maxTop
	}

	sm.ThumbTop = thumbTop
	sm.ThumbHeight = thumbH
	return sm
}

func renderScrollbarColumn(sm scrollbarMetrics, hovered bool) []string {
	if sm.BodyHeight <= 0 {
		return nil
	}

	trackStyle, thumbStyle, topHotStyle, bottomHotStyle := scrollbarStyles(hovered)

	out := make([]string, 0, sm.BodyHeight)
	for i := 0; i < sm.BodyHeight; i++ {
		isThumb := i >= sm.ThumbTop && i < sm.ThumbTop+sm.ThumbHeight
		isTopHot := sm.isTopHot(i)
		isBottomHot := sm.isBottomHot(i)

		switch {
		case isThumb:
			out = append(out, thumbStyle.Render("█"))
		case isTopHot:
			out = append(out, topHotStyle.Render("╷"))
		case isBottomHot:
			out = append(out, bottomHotStyle.Render("╵"))
		default:
			out = append(out, trackStyle.Render("│"))
		}
	}
	return out
}

func scrollbarStyles(hovered bool) (trackStyle, thumbStyle, topHotStyle, bottomHotStyle lipgloss.Style) {
	if hovered {
		return lipgloss.NewStyle().
				Foreground(lipgloss.Color("248")).
				Bold(true),
			lipgloss.NewStyle().
				Foreground(lipgloss.Color("255")).
				Bold(true),
			lipgloss.NewStyle().
				Foreground(lipgloss.Color("252")).
				Bold(true),
			lipgloss.NewStyle().
				Foreground(lipgloss.Color("252")).
				Bold(true)
	}

	return lipgloss.NewStyle().
			Foreground(lipgloss.Color("240")),
		lipgloss.NewStyle().
			Foreground(lipgloss.Color("252")),
		lipgloss.NewStyle().
			Foreground(lipgloss.Color("244")),
		lipgloss.NewStyle().
			Foreground(lipgloss.Color("244"))
}

func joinBodyWithScrollbar(bodyLines []string, scrollCol []string, contentWidth int, gapWidth int) []string {
	n := len(bodyLines)
	if len(scrollCol) > n {
		n = len(scrollCol)
	}

	gap := ""
	if gapWidth > 0 {
		gap = strings.Repeat(" ", gapWidth)
	}

	out := make([]string, 0, n)
	for i := 0; i < n; i++ {
		left := ""
		if i < len(bodyLines) {
			left = bodyLines[i]
		}

		if contentWidth > 0 {
			left = clipRightDisplay(left, contentWidth)
			left = padRightDisplay(left, contentWidth)
		} else {
			left = ""
		}

		sb := " "
		if i < len(scrollCol) {
			sb = scrollCol[i]
		}

		out = append(out, left+gap+sb)
	}
	return out
}

func padRightDisplay(s string, width int) string {
	w := lipgloss.Width(s)
	if w >= width {
		return s
	}
	return s + strings.Repeat(" ", width-w)
}

func clipRightDisplay(s string, width int) string {
	if width <= 0 || s == "" {
		return ""
	}
	if lipgloss.Width(s) <= width {
		return s
	}

	rs := []rune(s)
	var b strings.Builder
	displayW := 0

	inEscape := false
	for i := 0; i < len(rs); i++ {
		r := rs[i]

		// ANSI escape 开始
		if r == '\x1b' {
			inEscape = true
			b.WriteRune(r)
			continue
		}

		if inEscape {
			b.WriteRune(r)
			// CSI 序列通常以字母结尾，如 m
			if (r >= 'A' && r <= 'Z') || (r >= 'a' && r <= 'z') {
				inEscape = false
			}
			continue
		}

		rw := lipgloss.Width(string(r))
		if displayW+rw > width {
			break
		}

		b.WriteRune(r)
		displayW += rw
	}

	// 避免颜色泄漏到后面 scrollbar
	return b.String() + "\x1b[0m"
}

func (sm scrollbarMetrics) topHotRows() int {
	if sm.BodyHeight <= 0 {
		return 0
	}
	if sm.BodyHeight <= 4 {
		return 1
	}
	return 2
}

func (sm scrollbarMetrics) bottomHotRows() int {
	if sm.BodyHeight <= 0 {
		return 0
	}
	if sm.BodyHeight <= 4 {
		return 1
	}
	return 2
}

func (sm scrollbarMetrics) isTopHot(localY int) bool {
	if sm.BodyHeight <= 0 {
		return false
	}
	hot := sm.topHotRows()
	return localY >= 0 && localY < hot
}

func (sm scrollbarMetrics) isBottomHot(localY int) bool {
	if sm.BodyHeight <= 0 {
		return false
	}
	hot := sm.bottomHotRows()
	return localY >= sm.BodyHeight-hot && localY < sm.BodyHeight
}

func (sm scrollbarMetrics) ratioForLocalY(localY int) float64 {
	if sm.BodyHeight <= 1 {
		return 0
	}
	if localY < 0 {
		localY = 0
	}
	if localY >= sm.BodyHeight {
		localY = sm.BodyHeight - 1
	}
	return float64(localY) / float64(sm.BodyHeight-1)
}
