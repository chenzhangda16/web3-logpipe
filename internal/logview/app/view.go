package app

import (
	"strings"

	"github.com/chenzhangda16/web3-logpipe/internal/logview/schema"
)

func (m Model[T]) View() string {
	if m.schemaRoot == nil {
		return "loading..."
	}

	var lines []string

	headerLines := schema.RenderTreeHeader(
		m.layoutRoot,
		m.schemaLeaves,
		schema.DefaultHeaderRenderConfig(),
	)
	lines = append(lines, headerLines...)

	bodyRows := m.bodyRows()

	lo := m.topRow
	hi := lo + bodyRows
	if hi > m.rows.Len() {
		hi = m.rows.Len()
	}
	if lo < 0 {
		lo = 0
	}

	// 先生成 body 内容
	bodyLines := make([]string, 0, bodyRows)
	if lo < hi {
		for _, row := range m.rows.Slice(lo, hi) {
			bodyLines = append(bodyLines, m.renderRowGeneric(row))
		}
	}

	// body 不够高则补空行，保证滚动条与状态栏位置稳定
	for len(bodyLines) < bodyRows {
		bodyLines = append(bodyLines, "")
	}

	// 右侧伪滚动条
	sm := m.buildScrollbarMetrics()
	scrollCol := renderScrollbarColumn(sm, m.hoverScrollbar)

	// 右侧预留 1 列滚动条，所以内容宽度减 1
	contentWidth := m.bodyContentWidth()
	if contentWidth < 0 {
		contentWidth = 0
	}
	bodyWithScrollbar := joinBodyWithScrollbar(bodyLines, scrollCol, contentWidth)
	lines = append(lines, bodyWithScrollbar...)

	// 状态栏固定底部
	lines = append(lines, m.renderStatusLine())
	return strings.Join(lines, "\n")
}
