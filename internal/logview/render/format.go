package render

import (
	"fmt"
	"reflect"
	"strings"
)

type Align int

const (
	AlignLeft Align = iota
	AlignRight
)

func PadCell(s string, width int, align Align) string {
	rs := []rune(s)
	if len(rs) > width {
		return string(rs[:width])
	}
	pad := strings.Repeat(" ", width-len(rs))
	if align == AlignRight {
		return pad + s
	}
	return s + pad
}

func FmtDur(ns int64) string {
	switch {
	case ns < 1000:
		return fmt.Sprintf("%dns", ns)
	case ns < 1_000_000:
		return fmt.Sprintf("%dus", ns/1000)
	case ns < 1_000_000_000:
		return fmt.Sprintf("%.2fms", float64(ns)/1_000_000.0)
	default:
		return fmt.Sprintf("%.2fs", float64(ns)/1_000_000_000.0)
	}
}

func AsFloat64(v reflect.Value) float64 {
	for v.IsValid() && v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return 0
		}
		v = v.Elem()
	}
	switch v.Kind() {
	case reflect.Float32, reflect.Float64:
		return v.Convert(reflect.TypeOf(float64(0))).Float()
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return float64(v.Int())
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return float64(v.Uint())
	default:
		return 0
	}
}

func AsInt64(v reflect.Value) int64 {
	for v.IsValid() && v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return 0
		}
		v = v.Elem()
	}
	switch v.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return v.Int()
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return int64(v.Uint())
	case reflect.Float32, reflect.Float64:
		return int64(v.Float())
	default:
		return 0
	}
}
