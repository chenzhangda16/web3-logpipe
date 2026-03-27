package app

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/chenzhangda16/web3-logpipe/internal/logpipe/bench"
	"github.com/chenzhangda16/web3-logpipe/internal/logview/render"
)

func (m *Model) renderProcRow(row bench.ProcJson) string {
	cells := make([]string, 0, len(m.schemaLeaves))

	for _, lf := range m.schemaLeaves {
		str, raw := extractAndFormat(row, lf.Path)

		style := m.styler.Style(lf.Path, raw)
		str = style.Render(str)

		cells = append(cells, render.PadCell(str, lf.Width, lf.Align))
	}

	return strings.Join(cells, " ")
}

func extractAndFormat(row bench.ProcJson, path string) (string, any) {
	v := extractValueByPath(row, path)

	switch x := v.(type) {
	case float64:
		return fmt.Sprintf("%.1f", x), x
	case float32:
		return fmt.Sprintf("%.1f", x), float64(x)
	case int:
		return fmt.Sprintf("%d", x), x
	case int64:
		return fmt.Sprintf("%d", x), x
	default:
		return fmt.Sprintf("%v", v), v
	}
}

func extractValueByPath(root any, path string) any {
	parts := strings.Split(path, ".")
	v := reflect.ValueOf(root)

	for _, p := range parts {
		if !v.IsValid() {
			return nil
		}

		// 处理指针
		if v.Kind() == reflect.Pointer {
			v = v.Elem()
		}

		switch v.Kind() {
		case reflect.Struct:
			v = v.FieldByNameFunc(func(name string) bool {
				return strings.EqualFold(name, p)
			})

		case reflect.Map:
			v = v.MapIndex(reflect.ValueOf(p))

		default:
			return nil
		}
	}

	if !v.IsValid() {
		return nil
	}

	return v.Interface()
}
