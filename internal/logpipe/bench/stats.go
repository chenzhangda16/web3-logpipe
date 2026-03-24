package bench

import (
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"time"
)

//var sep string = "\n"

var sep string = " "

func percentiles(ns []int64) (p50, p90, p99 time.Duration) {
	if len(ns) == 0 {
		return 0, 0, 0
	}
	sort.Slice(ns, func(i, j int) bool { return ns[i] < ns[j] })
	p50 = time.Duration(ns[idx(ns, 0.50)])
	p90 = time.Duration(ns[idx(ns, 0.90)])
	p99 = time.Duration(ns[idx(ns, 0.99)])
	return
}

func idx(ns []int64, q float64) int {
	if len(ns) == 0 {
		return 0
	}
	if q <= 0 {
		return 0
	}
	if q >= 1 {
		return len(ns) - 1
	}
	x := int(math.Ceil(float64(len(ns))*q)) - 1
	if x < 0 {
		x = 0
	}
	if x >= len(ns) {
		x = len(ns) - 1
	}
	return x
}

func EmitBench(role, kind string, v any) {
	b, err := json.Marshal(v)
	if err != nil {
		return
	}
	fmt.Printf("BENCHv1\t%s\t%s\t%s\n", role, kind, b)
}
