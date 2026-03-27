package render

import (
	"strings"

	"github.com/charmbracelet/lipgloss"
)

type ValueStyleFunc func(v any) lipgloss.Style

type StyleRule struct {
	Match func(path string) bool
	Style ValueStyleFunc
}

type Styler struct {
	rules []StyleRule
}

func NewStyler() *Styler {
	s := &Styler{}
	s.rules = defaultRules()
	return s
}

func (s *Styler) Style(path string, v any) lipgloss.Style {
	for _, r := range s.rules {
		if r.Match(path) {
			return r.Style(v)
		}
	}
	return lipgloss.NewStyle()
}

func defaultRules() []StyleRule {
	return []StyleRule{

		// ===== CPU / Busy =====
		ruleSuffix("cpu_pct", stylePct()),
		ruleSuffix("busy_pct", stylePct()),

		// ===== 网络利用率 =====
		ruleSuffix("rx_pct", stylePct()),
		ruleSuffix("tx_pct", stylePct()),

		// ===== queue 使用率（len/cap）=====
		ruleQueueRatio(),

		// ===== latency（ns）=====
		ruleSuffix("p99_ns", styleLatency()),
		ruleSuffix("max_ns", styleLatency()),

		// ===== error =====
		ruleSuffix("err", styleError()),

		// ===== lag / fatal =====
		ruleSuffix("lag_fatal", styleError()),
	}
}

func ruleSuffix(suffix string, fn ValueStyleFunc) StyleRule {
	return StyleRule{
		Match: func(path string) bool {
			return strings.HasSuffix(path, suffix)
		},
		Style: fn,
	}
}

func stylePct() ValueStyleFunc {
	return func(v any) lipgloss.Style {
		f, ok := toFloat(v)
		if !ok {
			return lipgloss.NewStyle()
		}

		switch {
		case f >= 90:
			return lipgloss.NewStyle().Foreground(lipgloss.Color("196")).Bold(true) // 红
		case f >= 70:
			return lipgloss.NewStyle().Foreground(lipgloss.Color("208")) // 橙
		case f >= 40:
			return lipgloss.NewStyle().Foreground(lipgloss.Color("220")) // 黄
		default:
			return lipgloss.NewStyle().Foreground(lipgloss.Color("118")) // 绿
		}
	}
}

func styleLatency() ValueStyleFunc {
	return func(v any) lipgloss.Style {
		ns, ok := toFloat(v)
		if !ok {
			return lipgloss.NewStyle()
		}

		ms := ns / 1e6

		switch {
		case ms > 100:
			return lipgloss.NewStyle().Foreground(lipgloss.Color("196")).Bold(true)
		case ms > 20:
			return lipgloss.NewStyle().Foreground(lipgloss.Color("208"))
		case ms > 5:
			return lipgloss.NewStyle().Foreground(lipgloss.Color("220"))
		default:
			return lipgloss.NewStyle().Foreground(lipgloss.Color("118"))
		}
	}
}

func styleError() ValueStyleFunc {
	return func(v any) lipgloss.Style {
		f, ok := toFloat(v)
		if !ok {
			return lipgloss.NewStyle()
		}
		if f > 0 {
			return lipgloss.NewStyle().
				Foreground(lipgloss.Color("15")).
				Background(lipgloss.Color("160")).
				Bold(true)
		}
		return lipgloss.NewStyle().Foreground(lipgloss.Color("240"))
	}
}

func ruleQueueRatio() StyleRule {
	return StyleRule{
		Match: func(path string) bool {
			return strings.HasSuffix(path, ".len")
		},
		Style: func(v any) lipgloss.Style {
			// len 本身没意义，需要配 cap
			// 这里先只对 len 做弱提示（后面可升级成跨字段）
			f, ok := toFloat(v)
			if !ok {
				return lipgloss.NewStyle()
			}
			if f > 0 {
				return lipgloss.NewStyle().Foreground(lipgloss.Color("111"))
			}
			return lipgloss.NewStyle().Foreground(lipgloss.Color("240"))
		},
	}
}

func toFloat(v any) (float64, bool) {
	switch x := v.(type) {
	case int:
		return float64(x), true
	case int64:
		return float64(x), true
	case float64:
		return x, true
	case float32:
		return float64(x), true
	default:
		return 0, false
	}
}
