package app

import (
	"context"
	"errors"

	tea "github.com/charmbracelet/bubbletea"
)

const (
	mouseWheelStep = 3
)

func (m Model[T]) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
		if m.follow {
			m.topRow = m.maxTopRow()
		}
		return m, nil

	case rowMsg[T]:
		if !m.paused {
			m.appendRow(msg.Row)
		}
		return m, m.waitRowMsgCmd()

	case errMsg:
		if msg.Err != nil && !errors.Is(msg.Err, context.Canceled) {
			m.lastErr = msg.Err.Error()
		}
		return m, nil

	case holdTickMsg:
		if !m.hold.Active {
			return m, nil
		}

		step := m.hold.stepAt(msg.At)
		switch m.hold.Dir {
		case holdUp:
			m.scrollBy(-step)
		case holdDown:
			m.scrollBy(step)
		}

		if m.hold.Active {
			return m, holdTickCmd()
		}
		return m, nil

	case tea.MouseMsg:
		return m.updateMouse(msg)

	case tea.KeyMsg:
		switch msg.String() {
		case "q", "ctrl+c":
			if m.cancel != nil {
				m.cancel()
			}
			return m, tea.Quit

		case "G":
			m.scrollToBottom()

		case "g":
			m.topRow = 0
			m.follow = false

		case "up", "k":
			m.scrollBy(-1)

		case "down", "j":
			m.scrollBy(1)

		case "pgup":
			m.scrollBy(-m.visibleRows())

		case "pgdown":
			m.scrollBy(m.visibleRows())

		case " ":
			m.paused = !m.paused

		case "c":
			m.rows.Clear()
			m.topRow = 0
			m.follow = true
		}
		return m, nil
	}

	return m, nil
}

func (m Model[T]) updateMouse(msg tea.MouseMsg) (tea.Model, tea.Cmd) {
	m.mouseZone = m.mouseZoneName(msg.X, msg.Y)
	m.hoverScrollbar = m.isScrollbarHit(msg.X, msg.Y)

	// 松开：停止 hold
	if msg.Action == tea.MouseActionRelease {
		if m.hold.Active {
			m.hold = stopHold()
		}
		return m, nil
	}

	if !m.isBodyY(msg.Y) {
		return m, nil
	}

	if m.isScrollbarHit(msg.X, msg.Y) {
		switch msg.Button {
		case tea.MouseButtonLeft:
			// Press：启动长按
			if msg.Action == tea.MouseActionPress {
				if m.isScrollbarTopHot(msg.Y) {
					m.topRow = 0
					m.follow = false
					m.hold = startHold(holdUp)
					return m, holdTickCmd()
				}
				if m.isScrollbarBottomHot(msg.Y) {
					m.scrollToBottom()
					m.hold = startHold(holdDown)
					return m, holdTickCmd()
				}

				// 中间：seek（无长按）
				m.seekFromScrollbarY(msg.Y)
				return m, nil
			}

			// fallback：如果某些终端不给 press，只触发一次
			if msg.Action == 0 {
				if m.isScrollbarTopHot(msg.Y) {
					m.topRow = 0
					m.follow = false
					return m, nil
				}
				if m.isScrollbarBottomHot(msg.Y) {
					m.scrollToBottom()
					return m, nil
				}
				m.seekFromScrollbarY(msg.Y)
				return m, nil
			}

		case tea.MouseButtonWheelUp:
			m.scrollBy(-mouseWheelStep)
			return m, nil

		case tea.MouseButtonWheelDown:
			m.scrollBy(mouseWheelStep)
			return m, nil
		}
		return m, nil
	}

	if m.isBodyContentHit(msg.X, msg.Y) {
		switch msg.Button {
		case tea.MouseButtonWheelUp:
			m.scrollBy(-mouseWheelStep)
			return m, nil

		case tea.MouseButtonWheelDown:
			m.scrollBy(mouseWheelStep)
			return m, nil
		}
	}

	return m, nil
}
