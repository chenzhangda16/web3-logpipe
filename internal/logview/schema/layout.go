package schema

type LayoutNode struct {
	Name     string
	Path     []string
	Depth    int
	X0       int
	X1       int
	Mid      int
	Children []*LayoutNode
	Leaf     *Leaf
}

func AssignLeafX(leaves []*Leaf, gap int) {
	x := 0
	for _, lf := range leaves {
		lf.X0 = x
		lf.X1 = x + lf.Width - 1
		x = lf.X1 + 1 + gap
	}
}

func BuildLayoutTree(n *Node) *LayoutNode {
	if n == nil {
		return nil
	}

	ln := &LayoutNode{
		Name:  n.Name,
		Path:  appendCopy(nil, n.Path...),
		Leaf:  n.Leaf,
		Depth: len(n.Path),
	}

	for _, ch := range n.Children {
		ln.Children = append(ln.Children, BuildLayoutTree(ch))
	}

	if n.Leaf != nil {
		ln.X0 = n.Leaf.X0
		ln.X1 = n.Leaf.X1
		ln.Mid = (ln.X0 + ln.X1) / 2
		return ln
	}

	if len(ln.Children) > 0 {
		ln.X0 = ln.Children[0].X0
		ln.X1 = ln.Children[len(ln.Children)-1].X1
		ln.Mid = (ln.X0 + ln.X1) / 2
	}

	return ln
}

func HeaderMaxDepth(root *LayoutNode) int {
	if root == nil {
		return 0
	}
	maxD := 0
	var walk func(*LayoutNode)
	walk = func(n *LayoutNode) {
		if n == nil {
			return
		}
		if n.Depth > maxD {
			maxD = n.Depth
		}
		for _, ch := range n.Children {
			walk(ch)
		}
	}
	walk(root)
	return maxD
}
