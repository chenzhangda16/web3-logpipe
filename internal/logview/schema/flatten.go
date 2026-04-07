package schema

import (
	"reflect"
	"strings"
)

func FlattenRow(row any) map[string]any {
	out := make(map[string]any)
	flattenInto(reflect.ValueOf(row), nil, out)
	return out
}

func flattenInto(v reflect.Value, path []string, out map[string]any) {
	v = indirectValue(v)
	if !v.IsValid() {
		return
	}

	switch v.Kind() {
	case reflect.Struct:
		t := v.Type()
		for i := 0; i < t.NumField(); i++ {
			f := t.Field(i)
			if f.PkgPath != "" {
				continue
			}

			name, omit := jsonFieldName(f)
			if omit || name == "" {
				continue
			}

			fv := v.Field(i)
			flattenInto(fv, appendPath(path, name), out)
		}

	case reflect.Map:
		if v.Type().Key().Kind() != reflect.String {
			return
		}

		keys := v.MapKeys()
		for _, k := range keys {
			mv := v.MapIndex(k)
			flattenInto(mv, appendPath(path, k.String()), out)
		}

	default:
		if len(path) == 0 {
			return
		}
		out[strings.Join(path, ".")] = v.Interface()
	}
}

func appendPath(path []string, s string) []string {
	out := make([]string, len(path), len(path)+1)
	copy(out, path)
	out = append(out, s)
	return out
}
