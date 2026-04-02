package app

import (
	"reflect"
	"strings"

	"github.com/chenzhangda16/web3-logpipe/internal/logview/render"
	"github.com/chenzhangda16/web3-logpipe/internal/logview/schema"
)

func (m *Model[T]) renderRowGeneric(row T) string {
	rv := reflect.ValueOf(row)
	cells := make([]string, 0, len(m.schemaLeaves))

	for _, lf := range m.schemaLeaves {
		val := schema.ResolveLeafValue(rv, lf)

		var raw any
		s := ""

		if val.IsValid() {
			raw = val.Interface()
			s = lf.Format(val)
		}

		style := m.styler.Style(lf.PathKey, raw)
		s = style.Render(s)

		cells = append(cells, render.PadCell(s, lf.Width, lf.Align))
	}

	return strings.Join(cells, " ")
}
