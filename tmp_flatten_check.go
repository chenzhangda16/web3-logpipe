package main

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Println("usage: go run ./tmp_flatten_check.go <sample.json>")
		os.Exit(1)
	}

	b, err := os.ReadFile(os.Args[1])
	if err != nil {
		panic(err)
	}

	var v any
	if err := json.Unmarshal(b, &v); err != nil {
		panic(err)
	}

	out := map[string]any{}
	flatten(v, "", out)

	keys := make([]string, 0, len(out))
	for k := range out {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, k := range keys {
		fmt.Printf("%s = %#v\n", k, out[k])
	}
}

func flatten(v any, path string, out map[string]any) {
	switch x := v.(type) {
	case map[string]any:
		for k, vv := range x {
			next := k
			if path != "" {
				next = path + "." + k
			}
			flatten(vv, next, out)
		}

	case []any:
		for i, vv := range x {
			next := fmt.Sprintf("%s.%d", path, i)
			if path == "" {
				next = fmt.Sprintf("%d", i)
			}
			flatten(vv, next, out)
		}

	default:
		if path != "" {
			out[path] = x
		}
	}
}
