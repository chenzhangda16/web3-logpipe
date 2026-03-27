package app

import (
	"math"
	"time"

	tea "github.com/charmbracelet/bubbletea"
)

type holdDir int

const (
	holdNone holdDir = iota
	holdUp
	holdDown
)

type holdState struct {
	Active    bool
	Dir       holdDir
	StartTime time.Time
}

type holdTickMsg struct {
	At time.Time
}

const (
	holdTickInterval = 50 * time.Millisecond
	holdMaxStep      = 20
)

func startHold(dir holdDir) holdState {
	return holdState{
		Active:    true,
		Dir:       dir,
		StartTime: time.Now(),
	}
}

func stopHold() holdState {
	return holdState{}
}

func (h holdState) stepAt(now time.Time) int {
	if !h.Active || h.Dir == holdNone {
		return 0
	}

	held := now.Sub(h.StartTime).Seconds()
	if held < 0 {
		held = 0
	}

	// 温和指数增长：
	// 前段接近线性，后段逐渐拉开，但有上限。
	//
	// 大致观感：
	// 0.0s -> 1
	// 0.5s -> 2~3
	// 1.0s -> 4~5
	// 1.5s -> 7~8
	// 2.0s -> 10+
	// 再往后逐渐逼近上限
	step := int(math.Round(1.0 + 1.6*(math.Exp(0.95*held)-1.0)))
	if step < 1 {
		step = 1
	}
	if step > holdMaxStep {
		step = holdMaxStep
	}
	return step
}

func holdTickCmd() tea.Cmd {
	return tea.Tick(holdTickInterval, func(t time.Time) tea.Msg {
		return holdTickMsg{At: t}
	})
}
