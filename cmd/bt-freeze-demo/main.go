package main

import (
	"bufio"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

type rowInputMsg struct {
	row Row
}

type errMsg struct {
	err error
}

func parseLine(line string) (Row, bool) {
	parts := strings.SplitN(strings.TrimSpace(line), "|", 6)
	if len(parts) != 6 {
		return Row{}, false
	}

	id, err1 := strconv.Atoi(parts[0])
	qps, err2 := strconv.Atoi(parts[3])
	lat, err3 := strconv.Atoi(parts[4])
	if err1 != nil || err2 != nil || err3 != nil {
		return Row{}, false
	}

	return Row{
		ID:      id,
		Node:    parts[1],
		Status:  parts[2],
		QPS:     qps,
		Latency: lat,
		Remark:  parts[5],
	}, true
}

func streamFromFIFO(path string, p *tea.Program) {
	for {
		f, err := os.OpenFile(path, os.O_RDONLY, 0)
		if err != nil {
			p.Send(errMsg{err: err})
			time.Sleep(500 * time.Millisecond)
			continue
		}

		scanner := bufio.NewScanner(f)
		for scanner.Scan() {
			line := scanner.Text()
			row, ok := parseLine(line)
			if !ok {
				continue
			}
			p.Send(rowInputMsg{row: row})
		}

		if err := scanner.Err(); err != nil {
			p.Send(errMsg{err: err})
		}

		_ = f.Close()
		time.Sleep(100 * time.Millisecond)
	}
}

type Row struct {
	ID      int
	Node    string
	Status  string
	QPS     int
	Latency int
	Remark  string
}

type tickMsg time.Time

func nextTick() tea.Cmd {
	//return tea.Tick(300*time.Millisecond, func(t time.Time) tea.Msg {
	//	return tickMsg(t)
	//})
	return nil
}

type Model struct {
	width     int
	height    int
	scroll    int
	rows      []Row
	colWidths []int

	nextID     int
	followTail bool
	paused     bool
	rng        *rand.Rand
}

func initialModel() Model {
	return Model{
		colWidths:  []int{6, 12, 10, 10, 12, 28},
		nextID:     1,
		followTail: true,
		rng:        rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

func (m Model) Init() tea.Cmd {
	//return nextTick()
	return nil
}

func (m Model) bodyHeight() int {
	h := m.height - 5
	if h < 3 {
		return 3
	}
	return h
}

func (m Model) maxScroll() int {
	bh := m.bodyHeight()
	if len(m.rows) <= bh {
		return 0
	}
	return len(m.rows) - bh
}

func (m *Model) clampScroll() {
	if m.scroll < 0 {
		m.scroll = 0
	}
	max := m.maxScroll()
	if m.scroll > max {
		m.scroll = max
	}
}

func (m *Model) snapToTail() {
	m.scroll = m.maxScroll()
}

func (m Model) genRow() Row {
	nodes := []string{"main", "pc127", "m2", "pixel", "writer", "fetcher", "processor"}

	node := nodes[m.rng.Intn(len(nodes))]
	qps := 60 + m.rng.Intn(260)
	lat := 5 + m.rng.Intn(180)

	status := "OK"
	remark := "stable"

	switch {
	case lat >= 120 || qps <= 80:
		status = "ERROR"
		remark = "drop/retry pressure"
	case lat >= 60 || qps <= 120:
		status = "WARN"
		remark = "backpressure rising"
	default:
		status = "OK"
		remark = "stable"
	}

	if node == "fetcher" && qps > 240 {
		remark = "burst fetch spike"
	}
	if node == "processor" && lat > 100 {
		remark = "window compute hot"
	}
	if node == "writer" && lat > 80 {
		remark = "sink flush slow"
	}

	row := Row{
		ID:      m.nextID,
		Node:    node,
		Status:  status,
		QPS:     qps,
		Latency: lat,
		Remark:  remark,
	}
	return row
}

func (m Model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
		m.clampScroll()
		if m.followTail {
			m.snapToTail()
		}
		return m, nil

	case tickMsg:
		if !m.paused {
			row := m.genRow()
			m.rows = append(m.rows, row)
			m.nextID++

			if m.followTail {
				m.snapToTail()
			} else {
				m.clampScroll()
			}
		}
		return m, nextTick()

	case tea.KeyMsg:
		switch msg.String() {
		case "ctrl+c", "q":
			return m, tea.Quit

		case "up", "k":
			m.scroll--
			m.followTail = false
			m.clampScroll()

		case "down", "j":
			m.scroll++
			m.clampScroll()
			if m.scroll >= m.maxScroll() {
				m.followTail = true
				m.snapToTail()
			} else {
				m.followTail = false
			}

		case "pgup", "b":
			m.scroll -= m.bodyHeight()
			m.followTail = false
			m.clampScroll()

		case "pgdown", "f":
			m.scroll += m.bodyHeight()
			m.clampScroll()
			if m.scroll >= m.maxScroll() {
				m.followTail = true
				m.snapToTail()
			} else {
				m.followTail = false
			}

		case "g", "home":
			m.scroll = 0
			m.followTail = false

		case "G", "end":
			m.followTail = true
			m.snapToTail()

		case "t":
			m.followTail = true
			m.snapToTail()

		case " ":
			m.paused = !m.paused

		case "c":
			m.rows = nil
			m.scroll = 0
			m.nextID = 1
			m.followTail = true
		}
		return m, nil

	case rowInputMsg:
		m.rows = append(m.rows, msg.row)
		if m.followTail {
			m.snapToTail()
		} else {
			m.clampScroll()
		}
		return m, nil

	case errMsg:
		// 最简单先忽略，或者后面挂到状态栏
		return m, nil
	}

	return m, nil
}

var (
	titleStyle = lipgloss.NewStyle().
		Bold(true).
		Foreground(lipgloss.Color("230")).
		Background(lipgloss.Color("62")).
		Padding(0, 1)

	headerStyle = lipgloss.NewStyle().
		Bold(true).
		Foreground(lipgloss.Color("229")).
		Background(lipgloss.Color("238"))

	borderStyle = lipgloss.NewStyle().
		Foreground(lipgloss.Color("240"))

	oddRowStyle = lipgloss.NewStyle().
		Background(lipgloss.Color("236"))

	evenRowStyle = lipgloss.NewStyle().
		Background(lipgloss.Color("235"))

	helpStyle = lipgloss.NewStyle().
		Foreground(lipgloss.Color("246"))

	okStyle = lipgloss.NewStyle().
		Foreground(lipgloss.Color("42")).
		Bold(true)

	warnStyle = lipgloss.NewStyle().
		Foreground(lipgloss.Color("214")).
		Bold(true)

	errStyle = lipgloss.NewStyle().
		Foreground(lipgloss.Color("196")).
		Bold(true)

	hotStyle = lipgloss.NewStyle().
		Foreground(lipgloss.Color("220")).
		Bold(true)

	coldStyle = lipgloss.NewStyle().
		Foreground(lipgloss.Color("81"))
)

func pad(s string, width int) string {
	rs := []rune(s)
	if len(rs) > width {
		if width <= 1 {
			return string(rs[:width])
		}
		return string(rs[:width-1]) + "…"
	}
	return s + strings.Repeat(" ", width-len(rs))
}

func renderCell(text string, width int, style lipgloss.Style) string {
	return style.Width(width).Render(pad(text, width))
}

var (
	idHeaderStyle     = headerStyle.Foreground(lipgloss.Color("220")) // 黄
	nodeHeaderStyle   = headerStyle.Foreground(lipgloss.Color("117")) // 蓝
	statusHeaderStyle = headerStyle.Foreground(lipgloss.Color("196")) // 红
	qpsHeaderStyle    = headerStyle.Foreground(lipgloss.Color("42"))  // 绿
)

func (m Model) renderHeader() string {
	cols := []string{
		renderCell("ID", m.colWidths[0], idHeaderStyle),
		renderCell("NODE", m.colWidths[1], nodeHeaderStyle),
		renderCell("STATUS", m.colWidths[2], statusHeaderStyle),
		renderCell("QPS", m.colWidths[3], qpsHeaderStyle),
		renderCell("LAT(ms)", m.colWidths[4], headerStyle),
		renderCell("REMARK", m.colWidths[5], headerStyle),
	}
	return strings.Join(cols, "")
}

func (m Model) renderRow(i int, r Row) string {
	rowBg := evenRowStyle
	if i%2 == 1 {
		rowBg = oddRowStyle
	}

	idCell := renderCell(fmt.Sprintf("%d", r.ID), m.colWidths[0], rowBg)

	nodeStyle := rowBg
	if r.Node == "processor" || r.Node == "fetcher" {
		nodeStyle = rowBg.Foreground(lipgloss.Color("117")).Bold(true)
	}
	nodeCell := renderCell(r.Node, m.colWidths[1], nodeStyle)

	statusStyle := rowBg
	switch r.Status {
	case "OK":
		statusStyle = rowBg.Inherit(okStyle)
	case "WARN":
		statusStyle = rowBg.Inherit(warnStyle)
	case "ERROR":
		statusStyle = rowBg.Inherit(errStyle)
	}
	statusCell := renderCell(r.Status, m.colWidths[2], statusStyle)

	qpsStyle := rowBg
	if r.QPS >= 220 {
		qpsStyle = rowBg.Inherit(hotStyle)
	} else {
		qpsStyle = rowBg.Inherit(coldStyle)
	}
	qpsCell := renderCell(fmt.Sprintf("%d", r.QPS), m.colWidths[3], qpsStyle)

	latStyle := rowBg
	switch {
	case r.Latency >= 120:
		latStyle = rowBg.Inherit(errStyle)
	case r.Latency >= 60:
		latStyle = rowBg.Inherit(warnStyle)
	default:
		latStyle = rowBg.Inherit(okStyle)
	}
	latCell := renderCell(fmt.Sprintf("%d", r.Latency), m.colWidths[4], latStyle)

	remarkCell := renderCell(r.Remark, m.colWidths[5], rowBg)

	return idCell + nodeCell + statusCell + qpsCell + latCell + remarkCell
}

func (m Model) renderBody() string {
	bh := m.bodyHeight()
	start := m.scroll
	end := start + bh
	if end > len(m.rows) {
		end = len(m.rows)
	}

	lines := make([]string, 0, bh)
	for i := start; i < end; i++ {
		lines = append(lines, m.renderRow(i, m.rows[i]))
	}

	totalWidth := 0
	for _, w := range m.colWidths {
		totalWidth += w
	}
	for len(lines) < bh {
		lines = append(lines, evenRowStyle.Render(strings.Repeat(" ", totalWidth)))
	}

	return strings.Join(lines, "\n")
}

func (m Model) View() string {
	title := titleStyle.Render("Bubble Tea Demo · 追加模式 + 锁定表头 + 颜色渲染")
	header := m.renderHeader()
	sep := borderStyle.Render(strings.Repeat("─", lipgloss.Width(header)))
	body := m.renderBody()

	follow := "OFF"
	if m.followTail {
		follow = "ON"
	}
	state := "RUN"
	if m.paused {
		state = "PAUSE"
	}

	info := helpStyle.Render(
		fmt.Sprintf(
			"rows=%d  scroll=%d/%d  follow=%s  state=%s",
			len(m.rows), m.scroll, m.maxScroll(), follow, state,
		),
	)
	help := helpStyle.Render("j/k ↑/↓ 滚动 · f/b 翻页 · g/G 头尾 · t 跟随尾部 · space 暂停 · c 清空 · q 退出")

	return strings.Join([]string{
		title,
		header,
		sep,
		body,
		info,
		help,
	}, "\n")
}

func main() {
	fifoPath := flag.String("fifo", "", "path to input fifo")
	flag.Parse()

	m := initialModel()
	p := tea.NewProgram(m, tea.WithAltScreen())

	if *fifoPath != "" {
		go streamFromFIFO(*fifoPath, p)
	}

	if _, err := p.Run(); err != nil {
		fmt.Println("error:", err)
		os.Exit(1)
	}
}
