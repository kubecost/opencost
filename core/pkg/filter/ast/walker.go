package ast

import (
	"fmt"
	"sort"
	"strings"

	"github.com/opencost/opencost/core/pkg/filter/util"
	"golang.org/x/text/cases"
	"golang.org/x/text/language"
)

// used to apply a title to pipeline
var titleCaser cases.Caser = cases.Title(language.Und, cases.NoLower)
var lowerCaser cases.Caser = cases.Lower(language.Und)

// TraversalState represents the state of the current leaf node in a traversal
// of the filter  Any grouping ops will include an Enter on their first
// occurence, and an Exit when leaving the op state.
type TraversalState int

const (
	// TraversalStateNone is used whenever a binary op leaf node is traversed.
	TraversalStateNone TraversalState = iota

	// TraversalStateEnter is used when a group op leaf node is traversed (and, or, not)
	TraversalStateEnter

	// TraversalStateExit is used wwhen a group op leaf node is popped (and, or, not).
	TraversalStateExit
)

// TransformLeaves produces a new tree, leaving non-leaf nodes (e.g. And, Or)
// intact and replacing leaf nodes (e.g. Equals, Contains) with the result of
// calling leafTransformer(node).
func TransformLeaves(node FilterNode, transformer func(FilterNode) FilterNode) FilterNode {
	if node == nil {
		return nil
	}

	// For group ops, we need to execute the callback with an Enter,
	// recursively call traverse, then execute the callback with an Exit.
	switch n := node.(type) {
	case *NotOp:
		return &NotOp{
			Operand: TransformLeaves(n.Operand, transformer),
		}
	case *AndOp:
		var newOperands []FilterNode
		for _, o := range n.Operands {
			newOperands = append(newOperands, TransformLeaves(o, transformer))
		}
		return &AndOp{
			Operands: newOperands,
		}
	case *OrOp:
		var newOperands []FilterNode
		for _, o := range n.Operands {
			newOperands = append(newOperands, TransformLeaves(o, transformer))
		}
		return &OrOp{
			Operands: newOperands,
		}

	// Remaining nodes are assumed to be leaves
	default:
		return transformer(node)
	}
}

// PreOrderTraversal accepts a root `FilterNode` and calls the f callback on
// each leaf node it traverses. When entering "group" leaf nodes (leaf nodes
// which contain other leaf nodes), a TraversalStateEnter/Exit will be includes
// to denote each depth. In short, the callback will be executed twice for each
// "group" op, once before entering, and once bofore exiting.
func PreOrderTraversal(node FilterNode, f func(FilterNode, TraversalState)) {
	if node == nil {
		return
	}

	// For group ops, we need to execute the callback with an Enter,
	// recursively call traverse, then execute the callback with an Exit.
	switch n := node.(type) {
	case *NotOp:
		f(node, TraversalStateEnter)
		PreOrderTraversal(n.Operand, f)
		f(node, TraversalStateExit)

	case *AndOp:
		f(node, TraversalStateEnter)
		for _, o := range n.Operands {
			PreOrderTraversal(o, f)
		}
		f(node, TraversalStateExit)

	case *OrOp:
		f(node, TraversalStateEnter)
		for _, o := range n.Operands {
			PreOrderTraversal(o, f)
		}
		f(node, TraversalStateExit)

	// Otherwise, we just linearly traverse
	default:
		f(node, TraversalStateNone)
	}

}

// ToPreOrderString runs a PreOrderTraversal and generates an indented tree structure string
// format for the provided tree root.
func ToPreOrderString(node FilterNode) string {
	var sb strings.Builder
	indent := 0

	printNode := func(n FilterNode, action TraversalState) {
		if action == TraversalStateEnter {
			sb.WriteString(OpStringFor(n, action, indent))
			indent++
		} else if action == TraversalStateExit {
			indent--
			sb.WriteString(OpStringFor(n, action, indent))
		} else {
			sb.WriteString(OpStringFor(n, action, indent))
		}
	}

	PreOrderTraversal(node, printNode)

	return sb.String()
}

// ToPreOrderShortString runs a PreOrderTraversal and generates a condensed tree structure string
// format for the provided tree root.
func ToPreOrderShortString(node FilterNode) string {
	var sb strings.Builder

	printNode := func(n FilterNode, action TraversalState) {
		if action == TraversalStateEnter {
			sb.WriteString(ShortOpStringFor(n, action))
		} else if action == TraversalStateExit {
			sb.WriteString(ShortOpStringFor(n, action))
		} else {
			sb.WriteString(ShortOpStringFor(n, action))
		}
	}

	PreOrderTraversal(node, printNode)

	return sb.String()
}

// OpStringFor returns a string for the provided leaf node, traversal state, and current
// depth.
func OpStringFor(node FilterNode, traversalState TraversalState, depth int) string {
	prefix := indent(depth)

	if traversalState == TraversalStateExit {
		return prefix + "}\n"
	}

	if traversalState == TraversalStateEnter {
		return prefix + titleCaser.String(string(node.Op())) + " {\n"
	}

	open := prefix + titleCaser.String(string(node.Op())) + " { "

	switch n := node.(type) {
	case *VoidOp:
		open += "Empty }\n"
	case *EqualOp:
		open += fmt.Sprintf("Left: %s, Right: %s }\n", n.Left.String(), n.Right)
	case *ContainsOp:
		open += fmt.Sprintf("Left: %s, Right: %s }\n", n.Left.String(), n.Right)
	case *ContainsPrefixOp:
		open += fmt.Sprintf("Left: %s, Right: %s }\n", n.Left.String(), n.Right)
	case *ContainsSuffixOp:
		open += fmt.Sprintf("Left: %s, Right: %s }\n", n.Left.String(), n.Right)
	default:
		open += "}\n"
	}

	return open
}

// ShortOpStringFor returns a condensed string for the provided leaf node, traversal state, and current
// depth.
func ShortOpStringFor(node FilterNode, traversalState TraversalState) string {
	if traversalState == TraversalStateExit {
		return ")"
	}

	if traversalState == TraversalStateEnter {
		return lowerCaser.String(string(node.Op())) + "("
	}

	open := lowerCaser.String(string(node.Op())) + "("

	switch n := node.(type) {
	case *VoidOp:
		open += ")"
	case *EqualOp:
		open += fmt.Sprintf("%s,%s)", condenseIdent(n.Left), n.Right)
	case *ContainsOp:
		open += fmt.Sprintf("%s,%s)", condenseIdent(n.Left), n.Right)
	case *ContainsPrefixOp:
		open += fmt.Sprintf("%s,%s)", condenseIdent(n.Left), n.Right)
	case *ContainsSuffixOp:
		open += fmt.Sprintf("%s,%s)", condenseIdent(n.Left), n.Right)
	default:
		open += ")"
	}

	return open
}

// condenses an identifier string
func condenseIdent(ident Identifier) string {
	s := condense(ident.Field.Name)
	if ident.Key != "" {
		s += "[" + ident.Key + "]"
	}
	return s
}

func condense(s string) string {
	lc := lowerCaser.String(s)
	if len(lc) > 2 {
		return lc[:2]
	}
	return lc
}

// Clone deep copies and returns the AST parameter.
func Clone(filter FilterNode) FilterNode {
	var result FilterNode = &VoidOp{}
	var currentOps *util.Stack[FilterGroup] = util.NewStack[FilterGroup]()

	PreOrderTraversal(filter, func(fn FilterNode, state TraversalState) {
		if fn == nil {
			return
		}
		switch n := fn.(type) {
		case *AndOp:
			if state == TraversalStateEnter {
				currentOps.Push(&AndOp{})
			} else if state == TraversalStateExit {
				if currentOps.Length() > 1 {
					current := currentOps.Pop()
					currentOps.Top().Add(current)
				} else {
					result = currentOps.Pop()
				}
			}
		case *OrOp:
			if state == TraversalStateEnter {
				currentOps.Push(&OrOp{})
			} else if state == TraversalStateExit {
				if currentOps.Length() > 1 {
					current := currentOps.Pop()
					currentOps.Top().Add(current)
				} else {
					result = currentOps.Pop()
				}
			}
		case *NotOp:
			if state == TraversalStateEnter {
				currentOps.Push(&NotOp{})
			} else if state == TraversalStateExit {
				if currentOps.Length() > 1 {
					current := currentOps.Pop()
					currentOps.Top().Add(current)
				} else {
					result = currentOps.Pop()
				}
			}
		case *ContradictionOp:
			if currentOps.Length() == 0 {
				result = &ContradictionOp{}
			} else {
				currentOps.Top().Add(&ContradictionOp{})
			}
		case *EqualOp:
			var field Field
			if n.Left.Field != nil {
				field = *n.Left.Field
			}
			sm := &EqualOp{
				Left: Identifier{
					Field: &field,
					Key:   n.Left.Key,
				},
				Right: n.Right,
			}

			if currentOps.Length() == 0 {
				result = sm
			} else {
				currentOps.Top().Add(sm)
			}

		case *ContainsOp:
			var field Field
			if n.Left.Field != nil {
				field = *n.Left.Field
			}
			sm := &ContainsOp{
				Left: Identifier{
					Field: &field,
					Key:   n.Left.Key,
				},
				Right: n.Right,
			}

			if currentOps.Length() == 0 {
				result = sm
			} else {
				currentOps.Top().Add(sm)
			}

		case *ContainsPrefixOp:
			var field Field
			if n.Left.Field != nil {
				field = *n.Left.Field
			}
			sm := &ContainsPrefixOp{
				Left: Identifier{
					Field: &field,
					Key:   n.Left.Key,
				},
				Right: n.Right,
			}

			if currentOps.Length() == 0 {
				result = sm
			} else {
				currentOps.Top().Add(sm)
			}

		case *ContainsSuffixOp:
			var field Field
			if n.Left.Field != nil {
				field = *n.Left.Field
			}
			sm := &ContainsSuffixOp{
				Left: Identifier{
					Field: &field,
					Key:   n.Left.Key,
				},
				Right: n.Right,
			}

			if currentOps.Length() == 0 {
				result = sm
			} else {
				currentOps.Top().Add(sm)
			}
		}
	})

	return result
}

// returns an 2-space indention for each depth
func indent(depth int) string {
	if depth <= 0 {
		return ""
	}
	return strings.Repeat("  ", depth)
}

func Fields(filter FilterNode) []Field {
	fields := map[Field]bool{}

	PreOrderTraversal(filter, func(fn FilterNode, state TraversalState) {
		if fn == nil {
			return
		}
		switch n := fn.(type) {
		case *EqualOp:
			if n.Left.Field != nil {
				fields[*n.Left.Field] = true
			}
		case *ContainsOp:
			if n.Left.Field != nil {
				fields[*n.Left.Field] = true
			}
		case *ContainsPrefixOp:
			if n.Left.Field != nil {
				fields[*n.Left.Field] = true
			}
		case *ContainsSuffixOp:
			if n.Left.Field != nil {
				fields[*n.Left.Field] = true
			}
		}
	})

	response := make([]Field, 0, len(fields))
	for field := range fields {
		response = append(response, field)
	}

	sort.Slice(response, func(i, j int) bool {
		return response[i].Name < response[j].Name
	})

	return response
}
