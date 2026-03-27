package app

import (
	"fmt"
	"time"

	"github.com/charmbracelet/lipgloss"
)

type viewMode int

const (
	viewModeFollow viewMode = iota
	viewModeScroll
	viewModePaused
)

type statusInfo struct {
	mode       viewMode
	rows       int
	topRow     int
	bottomRow  int
	visible    int
	latestTick int64
	latestTsMs int64
	fifoPath   string
	lastErr    string
	mouseZone  string
	holdDir    string
	holdStep   int

	winStartTsMs int64
	winEndTsMs   int64
	seekTsMs     int64
	seekRatio    float64
}

func (m Model) buildStatus() statusInfo {
	info := statusInfo{
		rows:      m.rows.Len(),
		topRow:    m.topRow,
		visible:   m.visibleRows(),
		fifoPath:  m.fifoPath,
		lastErr:   m.lastErr,
		mouseZone: m.mouseZone,
		seekTsMs:  m.lastSeekTsMs,
		seekRatio: m.lastSeekRatio,
	}

	if m.paused {
		info.mode = viewModePaused
	} else if m.follow {
		info.mode = viewModeFollow
	} else {
		info.mode = viewModeScroll
	}

	if m.rows.Len() > 0 {
		last := m.rows.At(m.rows.LastIndex())
		info.latestTick = last.Tick
		info.latestTsMs = last.TsMs
	}

	if m.rows.Len() == 0 {
		info.bottomRow = 0
	} else {
		info.bottomRow = minInt(m.topRow+m.visibleRows(), m.rows.Len())
	}

	if ts0, ts1, ok := m.windowTimeRange(); ok {
		info.winStartTsMs = ts0
		info.winEndTsMs = ts1
	}

	if m.hold.Active {
		switch m.hold.Dir {
		case holdUp:
			info.holdDir = "up"
		case holdDown:
			info.holdDir = "down"
		default:
			info.holdDir = "-"
		}
		info.holdStep = m.hold.stepAt(time.Now())
	}

	return info
}

func renderStatusLine(m Model) string {
	info := m.buildStatus()

	modeText, modeStyle := statusModeStyle(info.mode)

	leftParts := []string{
		modeStyle.Render(modeText),
		statusKV("rows", fmt.Sprintf("%d", info.rows)),
		statusKV("view", fmt.Sprintf("%d-%d", info.topRow, info.bottomRow)),
		statusKV("vis", fmt.Sprintf("%d", info.visible)),
		statusKV("zone", info.mouseZone),
		statusKV("seek", "time"),
		statusKV("hold", holdText(info)),
	}

	if info.winStartTsMs > 0 || info.winEndTsMs > 0 {
		leftParts = append(leftParts,
			statusKV("win", fmtWindowRange(info.winStartTsMs, info.winEndTsMs)),
		)
	}

	if info.seekTsMs > 0 {
		leftParts = append(leftParts,
			statusKV("seek_ts", fmtTsShort(info.seekTsMs)),
			statusKV("seek_r", fmt.Sprintf("%.2f", info.seekRatio)),
		)
	}

	if info.latestTick != 0 || info.latestTsMs != 0 {
		leftParts = append(leftParts,
			statusKV("tick", fmt.Sprintf("%d", info.latestTick)),
			statusKV("age", fmtLatestAge(info.latestTsMs)),
		)
	}

	rightParts := []string{
		statusKV("src", shortenMiddle(info.fifoPath, 36)),
	}

	if info.lastErr != "" {
		rightParts = append(rightParts, statusErr(info.lastErr))
	}

	left := lipgloss.JoinHorizontal(lipgloss.Left, leftParts...)
	right := lipgloss.JoinHorizontal(lipgloss.Left, rightParts...)

	width := m.width
	if width <= 0 {
		width = lipgloss.Width(left) + 2 + lipgloss.Width(right)
	}

	spacerWidth := width - lipgloss.Width(left) - lipgloss.Width(right)
	if spacerWidth < 1 {
		spacerWidth = 1
	}
	spacer := stringsRepeat(" ", spacerWidth)

	bar := left + spacer + right
	return statusBarStyle().Width(width).Render(bar)
}

func statusModeStyle(mode viewMode) (string, lipgloss.Style) {
	switch mode {
	case viewModeFollow:
		return " FOLLOW ", lipgloss.NewStyle().
			Foreground(lipgloss.Color("16")).
			Background(lipgloss.Color("114")).
			Bold(true)
	case viewModePaused:
		return " PAUSED ", lipgloss.NewStyle().
			Foreground(lipgloss.Color("16")).
			Background(lipgloss.Color("221")).
			Bold(true)
	default:
		return " SCROLL ", lipgloss.NewStyle().
			Foreground(lipgloss.Color("16")).
			Background(lipgloss.Color("180")).
			Bold(true)
	}
}

func statusKV(k, v string) string {
	kStyle := lipgloss.NewStyle().
		Foreground(lipgloss.Color("245")).
		Bold(true)

	vStyle := lipgloss.NewStyle().
		Foreground(lipgloss.Color("252"))

	return " " + kStyle.Render(k) + "=" + vStyle.Render(v)
}

func statusErr(s string) string {
	return " " + lipgloss.NewStyle().
		Foreground(lipgloss.Color("224")).
		Background(lipgloss.Color("52")).
		Render("err="+shortenMiddle(s, 48))
}

func statusBarStyle() lipgloss.Style {
	return lipgloss.NewStyle().
		Foreground(lipgloss.Color("252")).
		Background(lipgloss.Color("236"))
}

func fmtLatestAge(tsMs int64) string {
	if tsMs <= 0 {
		return "-"
	}
	age := time.Now().UnixMilli() - tsMs
	switch {
	case age < 1000:
		return fmt.Sprintf("%dms", age)
	case age < 60_000:
		return fmt.Sprintf("%.1fs", float64(age)/1000.0)
	default:
		return fmt.Sprintf("%.1fm", float64(age)/60000.0)
	}
}

func shortenMiddle(s string, max int) string {
	if max <= 0 || len([]rune(s)) <= max {
		return s
	}
	rs := []rune(s)
	if max < 5 {
		return string(rs[:max])
	}
	left := (max - 1) / 2
	right := max - 1 - left
	return string(rs[:left]) + "…" + string(rs[len(rs)-right:])
}

func stringsRepeat(s string, n int) string {
	if n <= 0 {
		return ""
	}
	b := make([]byte, 0, len(s)*n)
	for i := 0; i < n; i++ {
		b = append(b, s...)
	}
	return string(b)
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func holdText(info statusInfo) string {
	if info.holdDir == "" {
		return "-"
	}
	return fmt.Sprintf("%s:%d", info.holdDir, info.holdStep)
}

func fmtTsShort(tsMs int64) string {
	if tsMs <= 0 {
		return "-"
	}
	t := time.UnixMilli(tsMs)
	return t.Format("15:04:05.000")
}

func fmtWindowRange(ts0, ts1 int64) string {
	if ts0 <= 0 && ts1 <= 0 {
		return "-"
	}
	if ts0 <= 0 {
		return "…-" + fmtTsShort(ts1)
	}
	if ts1 <= 0 {
		return fmtTsShort(ts0) + "-…"
	}
	return fmtTsShort(ts0) + "-" + fmtTsShort(ts1)
}
