package schema

import (
	"reflect"
	"strings"

	"github.com/chenzhangda16/web3-logpipe/internal/logview/render"
)

type Override struct {
	Include *bool
	Title   string
	Width   int
	Align   *render.Align
	Format  func(v reflect.Value) string
}

type OverrideSet struct {
	Exact    map[string]Override
	Prefix   map[string]Override
	Wildcard []WildcardOverride
}

type WildcardOverride struct {
	Pattern string
	Rule    Override
}

func DefaultOverrideSet() OverrideSet {
	return OverrideSet{
		Exact:  map[string]Override{},
		Prefix: map[string]Override{},
		Wildcard: []WildcardOverride{
			{Pattern: "flow.wins.*.busy_pct", Rule: Override{Title: "busy", Width: 6}},
			{Pattern: "flow.wins.*.moves", Rule: Override{Title: "mv", Width: 6}},
			{Pattern: "flow.wins.*.avg_wait_ns", Rule: Override{Title: "aw", Width: 7}},
			{Pattern: "flow.wins.*.avg_work_ns", Rule: Override{Title: "ak", Width: 7}},
			{Pattern: "flow.wins.*.max_wait_ns", Rule: Override{Title: "mw", Width: 7}},
			{Pattern: "flow.wins.*.max_work_ns", Rule: Override{Title: "mk", Width: 7}},
		},
	}
}

func MatchOverride(path string, ovs OverrideSet) Override {
	if ov, ok := ovs.Exact[path]; ok {
		return ov
	}
	for _, w := range ovs.Wildcard {
		if wildcardMatch(w.Pattern, path) {
			return mergeOverride(Override{}, w.Rule)
		}
	}

	longest := ""
	var best Override
	for prefix, ov := range ovs.Prefix {
		if strings.HasPrefix(path, prefix) && len(prefix) > len(longest) {
			longest = prefix
			best = ov
		}
	}
	return best
}

func wildcardMatch(pattern, path string) bool {
	pp := strings.Split(pattern, ".")
	sp := strings.Split(path, ".")
	if len(pp) != len(sp) {
		return false
	}
	for i := range pp {
		if pp[i] == "*" {
			continue
		}
		if pp[i] != sp[i] {
			return false
		}
	}
	return true
}

func mergeOverride(a, b Override) Override {
	out := a
	if b.Include != nil {
		out.Include = b.Include
	}
	if b.Title != "" {
		out.Title = b.Title
	}
	if b.Width != 0 {
		out.Width = b.Width
	}
	if b.Align != nil {
		out.Align = b.Align
	}
	if b.Format != nil {
		out.Format = b.Format
	}
	return out
}
