package app

import (
	"reflect"
	"strings"

	"github.com/chenzhangda16/web3-logpipe/internal/logview/render"
	"github.com/chenzhangda16/web3-logpipe/internal/logview/schema"
)

func (m *Model[T]) renderRowGeneric(row T) string {
	flat := schema.FlattenRow(row)
	cells := make([]string, 0, len(m.schemaLeaves))

	for _, lf := range m.schemaLeaves {
		raw, ok := flat[lf.PathKey]

		s := ""
		if ok {
			s = formatLeafRaw(lf, raw)
		}

		s = render.PadCell(s, lf.Width, lf.Align)

		style := m.styler.Style(lf.PathKey, raw)
		s = style.Render(s)

		cells = append(cells, s)
	}
	return strings.Join(cells, " ")
}

func formatLeafRaw(lf *schema.Leaf, raw any) string {
	if raw == nil {
		return ""
	}
	return lf.Format(reflect.ValueOf(raw))
}
