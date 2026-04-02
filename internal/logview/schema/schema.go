package schema

import (
	"fmt"
	"reflect"
	"sort"
	"strings"

	"github.com/chenzhangda16/web3-logpipe/internal/logview/render"
)

type NodeKind int

const (
	NodeObject NodeKind = iota
	NodeLeaf
)

type Node struct {
	Name     string
	Path     []string
	Kind     NodeKind
	Type     reflect.Type
	Children []*Node
	Leaf     *Leaf
}

type Leaf struct {
	Path      []string
	PathKey   string
	Title     string
	Width     int
	Align     render.Align
	ValueType reflect.Type

	Accessor []AccessorStep
	Format   func(v reflect.Value) string

	Index int
	X0    int
	X1    int
}

type AccessorKind int

const (
	AccessorField AccessorKind = iota
	AccessorMapKey
)

type AccessorStep struct {
	Kind     AccessorKind
	FieldIdx int
	MapKey   string
}

type MapKeyProvider interface {
	KeysFor(path []string, mt reflect.Type) []string
}

type StaticMapKeyProvider struct {
	Exact map[string][]string
}

func (p StaticMapKeyProvider) KeysFor(path []string, mt reflect.Type) []string {
	if p.Exact == nil {
		return nil
	}
	keys := p.Exact[PathKey(path)]
	out := make([]string, len(keys))
	copy(out, keys)
	return out
}

func BuildSchemaTreeWithKeys(sample any, ovs OverrideSet, mkp MapKeyProvider) (*Node, []*Leaf, error) {
	t := reflect.TypeOf(sample)
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct {
		return nil, nil, fmt.Errorf("BuildSchemaTree expects struct root, got %v", t.Kind())
	}

	root := &Node{
		Name: t.Name(),
		Path: nil,
		Kind: NodeObject,
		Type: t,
	}

	var leaves []*Leaf
	if err := expandStructNode(root, t, nil, nil, &leaves, ovs, mkp); err != nil {
		return nil, nil, err
	}
	for i, lf := range leaves {
		lf.Index = i
	}
	return root, leaves, nil
}

func expandStructNode(
	parent *Node,
	t reflect.Type,
	path []string,
	accessor []AccessorStep,
	leaves *[]*Leaf,
	ovs OverrideSet,
	mkp MapKeyProvider,
) error {
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		if f.PkgPath != "" {
			continue
		}

		name, omit := jsonFieldName(f)
		if omit || name == "" {
			continue
		}

		ft := indirectType(f.Type)
		childPath := appendCopy(path, name)
		childAccessor := appendCopyAccessor(accessor, AccessorStep{
			Kind:     AccessorField,
			FieldIdx: i,
		})

		if ft.Kind() == reflect.Map && ft.Key().Kind() == reflect.String {
			child := &Node{
				Name: name,
				Path: childPath,
				Kind: NodeObject,
				Type: ft,
			}
			parent.Children = append(parent.Children, child)

			if err := expandMapNode(child, ft, childPath, childAccessor, leaves, ovs, mkp); err != nil {
				return err
			}
			continue
		}

		switch ft.Kind() {
		case reflect.Struct:
			child := &Node{
				Name: name,
				Path: childPath,
				Kind: NodeObject,
				Type: ft,
			}
			parent.Children = append(parent.Children, child)

			if err := expandStructNode(child, ft, childPath, childAccessor, leaves, ovs, mkp); err != nil {
				return err
			}

		default:
			leaf, ok := makeLeaf(childPath, ft, childAccessor, ovs)
			if !ok {
				continue
			}
			child := &Node{
				Name: name,
				Path: childPath,
				Kind: NodeLeaf,
				Type: ft,
				Leaf: leaf,
			}
			parent.Children = append(parent.Children, child)
			*leaves = append(*leaves, leaf)
		}
	}
	return nil
}

func expandMapNode(
	parent *Node,
	mt reflect.Type,
	path []string,
	accessor []AccessorStep,
	leaves *[]*Leaf,
	ovs OverrideSet,
	mkp MapKeyProvider,
) error {
	elem := indirectType(mt.Elem())
	keys := nilKeys(mkp, path, mt)
	sort.Strings(keys)

	for _, k := range keys {
		keyPath := appendCopy(path, k)
		keyAccessor := appendCopyAccessor(accessor, AccessorStep{
			Kind:   AccessorMapKey,
			MapKey: k,
		})

		if elem.Kind() == reflect.Map && elem.Key().Kind() == reflect.String {
			child := &Node{
				Name: k,
				Path: keyPath,
				Kind: NodeObject,
				Type: elem,
			}
			parent.Children = append(parent.Children, child)

			if err := expandMapNode(child, elem, keyPath, keyAccessor, leaves, ovs, mkp); err != nil {
				return err
			}
			continue
		}

		switch elem.Kind() {
		case reflect.Struct:
			child := &Node{
				Name: k,
				Path: keyPath,
				Kind: NodeObject,
				Type: elem,
			}
			parent.Children = append(parent.Children, child)

			if err := expandStructNode(child, elem, keyPath, keyAccessor, leaves, ovs, mkp); err != nil {
				return err
			}

		default:
			leaf, ok := makeLeaf(keyPath, elem, keyAccessor, ovs)
			if !ok {
				continue
			}
			child := &Node{
				Name: k,
				Path: keyPath,
				Kind: NodeLeaf,
				Type: elem,
				Leaf: leaf,
			}
			parent.Children = append(parent.Children, child)
			*leaves = append(*leaves, leaf)
		}
	}
	return nil
}

func makeLeaf(path []string, t reflect.Type, accessor []AccessorStep, ovs OverrideSet) (*Leaf, bool) {
	pk := PathKey(path)
	ov := MatchOverride(pk, ovs)

	if ov.Include != nil && !*ov.Include {
		return nil, false
	}

	title := ov.Title
	if title == "" {
		title = defaultLeafTitle(path)
	}

	width := ov.Width
	if width <= 0 {
		width = defaultWidthForTypeAndPath(t, path)
	}

	align := defaultAlignForType(t)
	if ov.Align != nil {
		align = *ov.Align
	}

	format := ov.Format
	if format == nil {
		format = defaultFormatter(t, path)
	}

	return &Leaf{
		Path:      appendCopy(nil, path...),
		PathKey:   pk,
		Title:     title,
		Width:     width,
		Align:     align,
		ValueType: t,
		Accessor:  appendCopyAccessor(nil, accessor...),
		Format:    format,
	}, true
}

func ResolveLeafValue(root reflect.Value, leaf *Leaf) reflect.Value {
	v := root
	for v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return reflect.Value{}
		}
		v = v.Elem()
	}

	for _, step := range leaf.Accessor {
		if !v.IsValid() {
			return reflect.Value{}
		}

		for v.Kind() == reflect.Pointer {
			if v.IsNil() {
				return reflect.Value{}
			}
			v = v.Elem()
		}

		switch step.Kind {
		case AccessorField:
			if v.Kind() != reflect.Struct {
				return reflect.Value{}
			}
			v = v.Field(step.FieldIdx)

		case AccessorMapKey:
			if v.Kind() != reflect.Map {
				return reflect.Value{}
			}
			mv := v.MapIndex(reflect.ValueOf(step.MapKey))
			if !mv.IsValid() {
				return reflect.Value{}
			}
			v = mv
		}
	}
	return v
}

func ProcMapKeyProvider() StaticMapKeyProvider {
	return StaticMapKeyProvider{
		Exact: map[string][]string{
			"core.w":         {"0", "1", "2", "3"},
			"flow.q.win":     {"0", "1", "2", "3"},
			"flow.winmove.w": {"0", "1", "2", "3"},
			"flow.wins":      {"0", "1", "2", "3"},
		},
	}
}

func FetchMapKeyProvider() MapKeyProvider {
	return nil
}

func PathKey(path []string) string {
	return strings.Join(path, ".")
}

func nilKeys(mkp MapKeyProvider, path []string, mt reflect.Type) []string {
	if mkp == nil {
		return nil
	}
	return mkp.KeysFor(path, mt)
}

func appendCopy[T any](base []T, more ...T) []T {
	out := make([]T, len(base), len(base)+len(more))
	copy(out, base)
	out = append(out, more...)
	return out
}

func appendCopyAccessor(base []AccessorStep, more ...AccessorStep) []AccessorStep {
	out := make([]AccessorStep, len(base), len(base)+len(more))
	copy(out, base)
	out = append(out, more...)
	return out
}

func indirectType(t reflect.Type) reflect.Type {
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	return t
}

func jsonFieldName(f reflect.StructField) (name string, omit bool) {
	tag := f.Tag.Get("json")
	if tag == "-" {
		return "", true
	}
	if tag == "" {
		return f.Name, false
	}
	parts := strings.Split(tag, ",")
	if parts[0] == "" {
		return f.Name, false
	}
	return parts[0], false
}

func defaultLeafTitle(path []string) string {
	last := path[len(path)-1]

	switch last {
	case "avg_wait_ns":
		return "aw"
	case "avg_work_ns":
		return "ak"
	case "max_wait_ns":
		return "mw"
	case "max_work_ns":
		return "mk"
	case "busy_pct":
		return "busy"
	case "gomaxprocs":
		return "gomax"
	case "goroutines":
		return "gr"
	case "cpu_pct":
		return "cpu"
	case "rx_bps":
		return "rx"
	case "tx_bps":
		return "tx"
	case "rx_pct":
		return "rx%"
	case "tx_pct":
		return "tx%"
	case "msg_ps":
		return "msgps"
	case "re_off":
		return "reoff"
	default:
		return last
	}
}

func defaultWidthForTypeAndPath(t reflect.Type, path []string) int {
	last := path[len(path)-1]

	switch last {
	case "tag":
		return 12
	case "tick":
		return 6
	case "phase":
		return 6
	case "iface":
		return 5
	case "len", "cap":
		return 5
	case "busy_pct", "cpu_pct", "rx_pct", "tx_pct":
		return 6
	case "avg_ns", "p50_ns", "p90_ns", "p99_ns", "max_ns", "avg_wait_ns", "avg_work_ns", "max_wait_ns", "max_work_ns":
		return 7
	}

	switch t.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return 7
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return 7
	case reflect.Float32, reflect.Float64:
		return 7
	case reflect.String:
		return 8
	default:
		return 8
	}
}

func defaultAlignForType(t reflect.Type) render.Align {
	switch t.Kind() {
	case reflect.String:
		return render.AlignLeft
	default:
		return render.AlignRight
	}
}

func defaultFormatter(t reflect.Type, path []string) func(v reflect.Value) string {
	last := path[len(path)-1]

	switch last {
	case "cpu_pct", "rx_pct", "tx_pct", "busy_pct":
		return func(v reflect.Value) string { return fmt.Sprintf("%.1f", render.AsFloat64(v)) }

	case "rx_bps", "tx_bps":
		return func(v reflect.Value) string { return fmt.Sprintf("%.1f", render.AsFloat64(v)/1024.0/1024.0) }

	case "avg_ns", "p50_ns", "p90_ns", "p99_ns", "max_ns", "avg_wait_ns", "avg_work_ns", "max_wait_ns", "max_work_ns":
		return func(v reflect.Value) string { return render.FmtDur(render.AsInt64(v)) }

	case "work_core_s", "wait_core_s":
		return func(v reflect.Value) string { return fmt.Sprintf("%.3f", render.AsFloat64(v)) }

	case "msg_ps":
		return func(v reflect.Value) string { return fmt.Sprintf("%.0f", render.AsFloat64(v)) }
	}

	switch t.Kind() {
	case reflect.String:
		return func(v reflect.Value) string { return v.String() }
	case reflect.Float32, reflect.Float64:
		return func(v reflect.Value) string { return fmt.Sprintf("%.1f", render.AsFloat64(v)) }
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return func(v reflect.Value) string { return fmt.Sprintf("%d", render.AsInt64(v)) }
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return func(v reflect.Value) string { return fmt.Sprintf("%d", v.Uint()) }
	case reflect.Bool:
		return func(v reflect.Value) string { return fmt.Sprintf("%t", v.Bool()) }
	default:
		return func(v reflect.Value) string { return fmt.Sprint(v.Interface()) }
	}
}
